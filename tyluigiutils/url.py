"""
Adapted from https://github.com/edx/edx-analytics-pipeline/blob/master/edx/analytics/tasks/url.py
Support URLs.  Specifically, we want to be able to refer to data stored in a
variety of locations and formats using a standard URL syntax.
Examples::
s3://some-bucket/path/to/file
/path/to/local/file.gz
hdfs://some/directory/
file://some/local/directory

"""
from __future__ import absolute_import

import os
import urlparse

import luigi
import luigi.configuration
import luigi.format
import luigi.contrib.hdfs
import luigi.contrib.s3
from luigi.contrib.s3 import S3Target

luigi_config = luigi.configuration.LuigiConfigParser.instance()


class ExternalURL(luigi.ExternalTask):
    """Simple Task that returns a target based on its URL"""
    url = luigi.Parameter()

    def output(self):
        return get_target_from_url(self.url)


class UncheckedExternalURL(ExternalURL):
    """A ExternalURL task that does not verify if the source file exists, which can be expensive for S3 URLs."""

    def complete(self):
        return True


class IgnoredTarget(luigi.contrib.hdfs.HdfsTarget):
    """Dummy target for use in Hadoop jobs that produce no explicit output file."""
    def __init__(self):
        super(IgnoredTarget, self).__init__(is_tmp=True)

    def exists(self):
        return False

    def open(self, mode='r'):
        return open('/dev/null', mode)


DEFAULT_TARGET_CLASS = luigi.LocalTarget
URL_SCHEME_TO_TARGET_CLASS = {
    'hdfs': luigi.contrib.hdfs.HdfsTarget,
    's3': S3Target,
    'file': luigi.LocalTarget
}


def get_target_class_from_url(url):
    """Returns a luigi target class based on the url scheme"""
    parsed_url = urlparse.urlparse(url)
    target_class = URL_SCHEME_TO_TARGET_CLASS.get(parsed_url.scheme, DEFAULT_TARGET_CLASS)
    kwargs = {}

    if issubclass(target_class, luigi.LocalTarget) or parsed_url.scheme == 'hdfs':
        # LocalTarget and HdfsTarget both expect paths without any scheme, netloc etc, just bare paths. So strip
        # everything else off the url and pass that in to the target.
        url = parsed_url.path
    # if issubclass(target_class, luigi.contrib.s3.S3Target):
    #   kwargs['client'] = ScalableS3Client()

    url = url.rstrip('/')
    args = (url,)

    return target_class, args, kwargs


def get_target_from_url(url):
    """Returns a luigi target based on the url scheme"""
    cls, args, kwargs = get_target_class_from_url(url)
    return cls(*args, **kwargs)


def url_path_join(url, *extra_path):
    """
    Extend the path component of the given URL.  Relative paths extend the
    existing path, absolute paths replace it.  Special path elements like '.'
    and '..' are not treated any differently than any other path element.

    Examples:
        url=http://foo.com/bar, extra_path=baz -> http://foo.com/bar/baz
        url=http://foo.com/bar, extra_path=/baz -> http://foo.com/baz
        url=http://foo.com/bar, extra_path=../baz -> http://foo.com/bar/../baz
    Args:
        url (str): The URL to modify.
        extra_path (str): The path to join with the current URL path.
    Returns:
        The URL with the path component joined with `extra_path` argument.
    """
    (scheme, netloc, path, params, query, fragment) = urlparse.urlparse(url)
    joined_path = os.path.join(path, *extra_path)
    return urlparse.urlunparse((scheme, netloc, joined_path, params, query, fragment))


def get_hdfs_target_from_rel_path(path, use_hdfs_dir=False):
    """ Transform paths into hdfs url using hdfs_dir

    param: use_hdfs_dir : if True, it will use hdfs_dir configuration
    to build the path.

    """
    user = os.getenv('USER')
    hdfs_dir = luigi_config.get('core', 'hdfs-dir')
    if use_hdfs_dir:
        url = "hdfs:///user/{user}/{hdfs_dir}/{path}"
    else:
        url = "hdfs:///user/{user}/{path}"

    url = url.format(
        user=user,
        hdfs_dir=hdfs_dir,
        path=path
    )

    return get_target_from_url(url)


def get_hdfs_dir_url():
    user = os.getenv('USER')
    hdfs_dir = luigi_config.get('core', 'hdfs-dir')
    url = "hdfs:///user/{user}/{hdfs_dir}/".format(
        user=user,
        hdfs_dir=hdfs_dir
    )
    return url


def get_hdfs_target_with_date(filename):
    """
    Returns the default  Target formatted
    hdfs:///user/{username}/{hdfs_dir}/{date}/{filename}
    """
    url = get_hdfs_url_with_date(filename)
    return get_target_from_url(url)


def get_hdfs_url_with_date(filename):
    date = luigi_config.get('task_params', 'date')
    return get_hdfs_url_with_specific_date(filename, date)


def get_hdfs_target_with_specific_date(filename, date):
    """
    Returns the default  Target formatted
    hdfs:///user/{username}/{hdfs_dir}/{date}/{filename}
    """
    url = get_hdfs_url_with_specific_date(filename, date)
    return get_target_from_url(url)


def get_hdfs_url_with_specific_date(filename, date):
    user = os.getenv('USER')
    hdfs_dir = luigi_config.get('core', 'hdfs-dir')
    date_format = luigi_config.get('core', 'date_format')

    if not isinstance(date, basestring):
        # assume datetime
        date = date.strftime(date_format)

    url = "hdfs:///user/{user}/{hdfs_dir}/{date}/{filename}".format(
        user=user,
        hdfs_dir=hdfs_dir,
        date=date,
        filename=filename
    )
    return url


def get_s3_url_with_date(filename):
    bucket = luigi_config.get('aws', 's3-bucket')
    date = luigi_config.get('task_params', 'date')
    date_format = luigi_config.get('core', 'date_format')

    if not isinstance(date, basestring):
        # assume datetime
        date = date.strftime(date_format)

    url = "s3://{bucket}/{date}/{filename}".format(
        bucket=bucket,
        date=date,
        filename=filename
    )
    return url


def get_s3_bucket_url(bucket):
    return "s3://{bucket}".format(
        bucket=bucket
    )


def get_absulute_hdfs_dir():
    user = os.getenv('USER')
    hdfs_dir = luigi_config.get('core', 'hdfs-dir')
    path = "/user/{user}/{hdfs_dir}".format(
        user=user,
        hdfs_dir=hdfs_dir
    )
    return path


def get_absulute_hdfs_dir_url():
    return "hdfs://" + get_absulute_hdfs_dir()


def get_absulute_hdfs_dir_url_with_date(d):
    abs_path = get_absulute_hdfs_dir_url()
    path = "{abs_path}/{date}".format(
        abs_path=abs_path,
        date=d
    )
    return path


def normalize_hdfs_url(hdfs_path):
    """Normalizes an hdfs url to return an hdfs path"""
    if "hdfs://" in hdfs_path:
        return hdfs_path[7:]
    return hdfs_path
