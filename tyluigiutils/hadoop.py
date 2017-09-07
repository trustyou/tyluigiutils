from __future__ import print_function
import os
import logging
import sys
import tempfile
import shutil

import jsonschema
from jsonschema.exceptions import ValidationError

import luigi
import luigi.contrib.hadoop
import simplejson as json


from luigi.contrib.hadoop import JobTask, HadoopJobRunner
from luigi import configuration, Parameter

logger = logging.getLogger('luigi-interface')


class VenvJobTask(JobTask):
    """
    Hadoop job which runs in a virtual environment.

    It uses the configuration 'venv_path' in the 'hadoop' section of your luigi configuration file.
    If the venv_path is a zip file it will use it directly  as archive.
    If it is a directory will compress it and ship it with the hadoop job.
    The venv can be also located  directly on HDFS, use the hdfs:// url prefix to indicate it.
    """
    venv_tmp = None
    venv_path = Parameter(config_path={'section': 'hadoop', 'name': 'venv_path'})
    libjars = None

    def _create_venv_archive(self, path):
        logging.info("Creating virtual environment archive from {}".format(path))
        self.venv_temp = tempfile.mkdtemp()
        zip_name = path.split('/')[-1] + '.zip'
        archive_name = os.path.join(self.venv_temp, zip_name)
        return shutil.make_archive(archive_name, 'zip', path)

    def job_runner(self):

        config = configuration.get_config()
        venv_path = self.venv_path
        if not venv_path.lower().endswith(".zip"):
            if venv_path.startswith('hdfs://'):
                raise ValueError(
                    "Cannot automatically compress a venv located on HDFS"
                )
            venv_path = self._create_venv_archive(venv_path)

        python_excutable = config.get('hadoop', 'python-executable', 'python')
        self.old_python_executable = python_excutable
        symbolic = venv_path.split('/')[-1].split('.')[0]
        venv_archive = "{}#{}".format(venv_path, symbolic)
        python_executable = "{}/bin/{}".format(symbolic, python_excutable)
        config.set('hadoop', 'python-executable', python_executable)

        return TYDefaultHadoopJobRunner(
            archives=[venv_archive],
            libjars=self.libjars,
        )

    def finish(self):
        if self.venv_tmp and os.path.exists(self.venv_tmp):
            shutil.rmtree(self.venv_tmp)
        config = configuration.get_config()
        config.set('hadoop', 'python-executable', self.old_python_executable)
        super(VenvJobTask, self).finish()


class TYDefaultHadoopJobRunner(HadoopJobRunner):
    def __init__(self, **kwargs):

        config = configuration.get_config()
        streaming_jar = config.get('hadoop', 'streaming-jar')
        super(TYDefaultHadoopJobRunner, self).__init__(
            streaming_jar=streaming_jar,
            **kwargs
        )


class JsonInputJobTask(luigi.contrib.hadoop.JobTask):
    """
    Helper class for a Hadoop job that reads JSON input, optionally
    validated by a schema.
    """

    def input_schema(self):
        """
        Override and return a Python object representing a JSON schema.
        If any value in the mapper input does not validate against this
        schema, an error is logged, and the job fails.
        """
        return None

    def reader(self, input_stream):
        for line in input_stream:
            key, value_str = line.decode("utf-8").rstrip("\n").split("\t", 1)
            try:
                value = json.loads(value_str)
            except ValueError:
                logging.critical("Invalid JSON: %s", value_str)
                raise
            if self.input_schema():
                try:
                    jsonschema.validate(value, self.input_schema(), format_checker=jsonschema.FormatChecker())
                except ValidationError as ex:
                    logging.critical("JSON schema violation '%s' at %s", ex.message, "/".join(str(el) for el in ex.path))
                    raise
            yield key, value

    def mapper(self, key, value):
        """
        Identity mapper
        """
        yield key, value


class JsonOutputJobTask(luigi.contrib.hadoop.JobTask):
    """
    Helper class for a Hadoop job that writes JSON input, optionally
    validated by a schema.
    """

    def output_schema(self):
        """
        Override and return a Python object representing a JSON schema.
        If any value output by the reducer does not validate against this
        schema, an error is logged, and the job fails.
        """
        return None

    def writer(self, outputs, stdout, stderr=sys.stderr):
        for key, value in outputs:
            if self.output_schema():
                try:
                    jsonschema.validate(value, self.output_schema(), format_checker=jsonschema.FormatChecker())
                except ValidationError as ex:
                    logging.critical("JSON schema violation '%s' at %s", ex.message, "/".join(str(el) for el in ex.path))
                    raise
            print("{0}\t{1}".format(key, json.dumps(value)).encode("utf-8"), file=stdout)


class JsonJobTask(JsonInputJobTask, JsonOutputJobTask):
    """
    Hadoop job which reads and writes JSON lines. Keys are plain text, but
    values are JSON-encoded. Fails if JSON decoding or encoding fails for
    any input or output line.

    Optionally, input and/or output can be validated against a JSON schema.
    To achieve this, override the input_schema and/or output_schema methods.
    """
    pass
