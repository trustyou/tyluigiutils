"""
Gather data using Sqoop table dumps run on RDBMS databases.
Adapted from: https://github.com/edx/edx-analytics-pipeline/blob/master/edx/analytics/tasks/sqoop.py
"""
import datetime
import json
import logging
import os
import luigi
import luigi.contrib.hadoop
import luigi.configuration

from luigi.contrib import hdfs
from .url import get_target_from_url
from .url import url_path_join
from .url import get_hdfs_target_from_rel_path

from .path import generate_temporary_path_name


log = logging.getLogger(__name__)


def load_sqoop_cmd():
    """Get path to sqoop command from Luigi configuration."""
    return luigi.configuration.get_config().get('sqoop', 'command', 'sqoop')


class SqoopImportTask(luigi.contrib.hadoop.BaseHadoopJobTask):
    """
    An abstract task that uses Sqoop to read data out of a database and
    writes it to a file in several format.
    In order to protect the database access credentials they are
    loaded from an external file which can be secured appropriately.
    By default it can be read from configuration (bad practice)
    but the `_get_credentials` method can be overridden to use a more
    secure approach

    Known Issues:
    - Parquet file output won't work on free query. So use it only when
    dumping a whole table.  This will be solved in Sqoop 1.4.7

    """
    destination = luigi.Parameter(
        description='The directory to write the output files to.',
    )

    username = luigi.Parameter(config_path={'section': 'db', 'name': 'username'})
    password = luigi.Parameter(
        config_path={'section': 'db', 'name': 'password'},
        default='',
    )
    host = luigi.Parameter(config_path={'section': 'db', 'name': 'host'})
    port = luigi.Parameter(config_path={'section': 'db', 'name': 'port'})

    database = luigi.Parameter(config_path={'section': 'db', 'name': 'database'})

    num_mappers = luigi.Parameter(
        default=1,
        description='The number of map tasks to ask Sqoop to use.',
    )
    verbose = luigi.BoolParameter(
        default=False,
        description='Print more information while working.',
    )
    table_name = luigi.Parameter(
        description='The name of the table to import.',
        default=None
    )

    query = luigi.Parameter(
        description='The query to perform must contain where $CONDITION at the end',
        default=None
    )

    split_by = luigi.Parameter(
        description='which field of the query/table is going to be used to split among the mappers',
        default=None
    )

    where = luigi.Parameter(
        default=None,
        description='A "where" clause to be passed to Sqoop.  Note that '
        'no spaces should be embedded and special characters should '
        'be escaped.  For example: --where "id\<50". ',
    )
    columns = luigi.Parameter(
        default=[],
        description='A list of column names to be included.  Default is to include all columns.'
    )
    null_string = luigi.Parameter(
        default=None,
        description='String to use to represent NULL values in output data.',
    )
    fields_terminated_by = luigi.Parameter(
        default=None,
        description='Defines the file separator to use on output.',
    )
    delimiter_replacement = luigi.Parameter(
        default=None,
        description='Defines a character to use as replacement for delimiters '
        'that appear within data values, for use with Hive.  Not specified by default.'
    )

    as_avro = luigi.BoolParameter(
        description='Whether to use Avro instead of TSV/CSV. Includes Snappy compression by default.',
        default=False
    )

    as_parquet = luigi.BoolParameter(
        description='Whether to use Parquet instead of TSV/CSV. Includes Snappy compression by default.',
        default=False
    )

    columns_map = luigi.Parameter(
        description='A dictionary with keys being the column names and the values the target Java type',
        default=None,
    )

    atomic_output = luigi.BoolParameter(
        description='Writes the output into a temporary target first and then moves to the intended output directory.',
        default=True
    )

    def __init__(self, *args, **kwargs):
        super(SqoopImportTask, self).__init__(*args, **kwargs)
        self.temporary_destination = None  # type: str
        if self.atomic_output is True:
            self.temporary_destination_full_path = generate_temporary_path_name(self.output().path)
            self.temporary_destination = self.temporary_destination_full_path.split('/')[-1]

    def get_working_destination(self):
        if self.atomic_output is True:
            return self.temporary_destination
        return self.destination

    def working_output_path(self):
        return self.temporary_destination_full_path

    def output(self):
        return get_hdfs_target_from_rel_path(self.destination)

    def metadata_output(self):
        """Return target to which metadata about the task execution can be written."""
        return get_target_from_url(url_path_join(self.get_working_destination(), '.metadata'))

    def job_runner(self):
        """Use simple runner that gets args from the job and passes through."""
        return SqoopImportRunner()

    def get_arglist(self, password_file):
        """Returns list of arguments for running Sqoop."""
        arglist = [load_sqoop_cmd(), 'import']
        # Generic args should be passed to sqoop first, followed by import-specific args.
        arglist.extend(self.generic_args(password_file))
        arglist.extend(self.import_args())
        return arglist

    def generic_args(self, password_target):
        """Returns list of arguments used by all Sqoop commands, using credentials read from file."""
        cred = self._get_credentials()
        url = self.connection_url(cred)
        generic_args = ['--connect', url, '--username', cred['username']]

        if self.verbose:
            generic_args.append('--verbose')

        #  write password to temp file object, and pass name of file to Sqoop:
        with password_target.open('w') as password_file:
            password_file.write(cred['password'])
            password_file.flush()
        generic_args.extend(['--password-file', password_target.path])

        return generic_args

    def _check_args(self):
        """ verifies that all the arguments make sense  """

        if self.query and not self.split_by and self.num_mappers > 1:
            raise ValueError(
                "split_by is None: You need to specify a column to split by when using a query and more than one reducer"
            )

        if self.query is not None and self.table_name is not None:
            raise ValueError("Incompatible parameter, use either 'table_name' or 'query'")

        if self.query is None and self.table_name is None:
            raise ValueError("One of 'table_name' or 'query' parameter should be set")

        if self.as_avro and self.as_parquet:
            raise ValueError("Select only one output format, either Parquet or Avro")

    def import_args(self):
        """Returns list of arguments specific to Sqoop import."""

        self._check_args()
        arglist = []

        if self.table_name:
            arglist.extend(['--table', self.table_name])
        else:
            arglist.extend(['--query', self.query])

        if self.split_by:
            arglist.extend(['--split-by', self.split_by])
        elif self.query and not self.num_mappers or self.num_mappers == 1:
            # if query is specified but not split column,do it serially
            logging.warn("No split_by column specified, importing serially")
            arglist.extend(['--num-mappers', "1"])

        target_dir = self.working_output_path()
        log.info("Writing the dump to temporary directory {}.".format(target_dir))
        arglist.extend(['--target-dir', target_dir])

        if len(self.columns) > 0:
            arglist.extend(['--columns', ','.join(self.columns)])
        if self.num_mappers is not None:
            arglist.extend(['--num-mappers', str(self.num_mappers)])
        if self.where is not None:
            arglist.extend(['--where', str(self.where)])
        if self.null_string is not None:
            arglist.extend(['--null-string', self.null_string, '--null-non-string', self.null_string])
        if self.fields_terminated_by is not None:
            arglist.extend(['--fields-terminated-by', self.fields_terminated_by])
        if self.delimiter_replacement is not None:
            arglist.extend(['--hive-delims-replacement', self.delimiter_replacement])

        if self.columns_map:
            m = ",".join(["{}={}".format(c, m) for c, m in self.columns_map.iteritems()])
            arglist.extend(['--map-column-java', m])

        if self.as_avro:
            arglist.extend(['--as-avrodatafile'])
            arglist.extend(['--compression-codec', 'org.apache.hadoop.io.compress.SnappyCodec'])

        if self.as_parquet:
            arglist.extend(['--as-parquetfile'])
            arglist.extend(['--compression-codec', 'org.apache.hadoop.io.compress.SnappyCodec'])

        return arglist

    def connection_url(self, _cred):
        """Construct connection URL from provided credentials."""
        raise NotImplementedError  # pragma: no cover

    def _get_credentials(self):
        """
        A dict containing credentials. Default implementation
        """
        cred = {
            'username': self.username,
            'password': self.password,
            'host': self.host,
            'port': self.port,
        }
        return cred


class SqoopImportFromPgSQL(SqoopImportTask):
    """
    An abstract task that uses Sqoop to read data out of a database and writes it to a file in CSV or Avro format.

    Known Issues:
    - Sqoop does not support very well the `uuid` format of Postgres so a cast to string needs to be done when dumping.
    Be aware of this when spliting (split_by) as this will be slow for large databases. For splitting is better to use
    a numeric index or use not splitting at all.

    """
    schema = luigi.Parameter(
        description='Which schema from pgSQL to use, valid whe using table',
        default=None,
    )

    def connection_url(self, cred):
        """Construct connection URL from provided credentials."""
        return 'jdbc:postgresql://{host}/{database}'.format(host=cred['host'], database=self.database)

    def import_args(self):
        """Returns list of arguments specific to Sqoop import from a Postgress database."""
        arglist = super(SqoopImportFromPgSQL, self).import_args()
        # If we specify a table we need a schema
        if self.table_name and self.schema is not None:
            raise ValueError("When using table_name the schema should be specified")

        if self.table_name:
            arglist.extend(["--"])
            arglist.extend(["--schema", self.schema])

        return arglist

    def _read_password(self):
        pgpass_path = os.path.expanduser('~/.pgpass')
        with open(pgpass_path) as f:
            for line in f:
                host, port, _, user, passwd = line.split(':')
                if host == self.host and user == self.username:
                    return passwd.strip()

        error_msg = "Could not find password for user {} is ~/.pgsql".format(self.username)
        raise ValueError(error_msg)

    def _get_credentials(self):
        """
        Specific for reading the password from .psql file.
        """
        cred = {
            'username': self.username,
            'password': self._read_password(),
            'host': self.host,
            'port': self.port,
        }
        return cred


class SqoopPasswordTarget(luigi.contrib.hdfs.HdfsTarget):
    """Defines a temp file in HDFS to hold password."""
    def __init__(self):
        super(SqoopPasswordTarget, self).__init__(is_tmp=True)


class SqoopImportRunner(luigi.contrib.hadoop.JobRunner):
    """Runs a SqoopImportTask by shelling out to sqoop."""

    def run_job(self, job):
        """Runs a SqoopImportTask by shelling out to sqoop."""

        metadata = {
            'start_time': datetime.datetime.utcnow().isoformat()
        }
        try:
            # Create a temp file in HDFS to store the password,
            # so it isn't echoed by the hadoop job code.
            # It should be deleted when it goes out of scope
            # (using __del__()), but safer to just make sure.
            password_target = SqoopPasswordTarget()
            arglist = job.get_arglist(password_target)
            luigi.contrib.hadoop.run_and_track_hadoop_job(arglist)
        except Exception as e:
            raise Exception(e)
        finally:
            try:
                password_target.remove()
            except:
                pass  # Do nothing if files does not exists
            metadata['end_time'] = datetime.datetime.utcnow().isoformat()
            try:
                with job.metadata_output().open('w') as metadata_file:
                    json.dump(metadata, metadata_file)
            except Exception:
                log.exception("Unable to dump metadata information.")
                pass

        self.finish(job)

    def finish(self, job):
        hdfs_client = hdfs.get_autoconfig_client()
        outpath = unicode(job.output().path)
        temppath = unicode(job.working_output_path())

        if hdfs_client.exists(outpath) is False and hdfs_client.exists(temppath) is True:
            hdfs_client.rename_dont_move(temppath, outpath)
        else:
            if hdfs_client.exists(job.output().path):
                message = 'Destination directory already exists {}.'.format(outpath)
            else:
                message = 'Temporary destination directory does not exists {}.'.format(temppath)
            raise Exception(message)
