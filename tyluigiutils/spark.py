import random
import typing  # noqa: F401
from os import rename
from functools import wraps
from typing import Any, Callable  # noqa: F401

import luigi
from luigi.contrib.spark import PySparkTask
from luigi.contrib import hdfs

from .path import generate_temporary_path_name


def write_decorator(write_function):
    # type: (Callable[..., None]) -> Callable[..., None]
    """
    Decorator for Spark output writer.

    This decorator writes first to a temporary files. if the writing is successful
    then it moves the temporary file to the final name. This is necessary as Spark
    might fail at the saveAs* stage however the final file would be created preventing Luigi
    to scheduling the task again after a failur.

    :param write_function: A writer function that saves the output.
    :return: The wrapper function.

    """
    @wraps(write_function)
    def write_wrapper(self, *args, **kwargs):
        # type: (Any, *Any, **Any) -> None
        """
        Writer wrapper.

        :param self: Instance of self of the caller.
        :param args: Arguments.
        :param kwargs: Keyed arguments.

        :return: None
        """
        write_function(self, *args, **kwargs)
        if self._is_atomic_output():
            spark_context = args[1]  # type: ignore
            # Using Any type annotations instead of the specific types because pyspark is only sent to the cluster
            # with luigi PySparkTask tasks. So for non-PySparkTasks, pyskark
            # package has to be sent by adding it to py-package, which would be redundant and not certain if that will
            # result into conflicts. So we don't send pyspark over the cluster just for this type annotation, and
            # therefore ignoring it.
            # We should move this SparkSpecific part to something like base_pyspark.py and then put all the spark
            # related things there.
            if spark_context.master.startswith('local'):
                old_path = normalize_local_filename(self._get_temp_output_path())
                new_path = normalize_local_filename(self._get_output_path())
                rename(old_path, new_path)
            else:
                hdfs_client = hdfs.get_autoconfig_client()  # type: ignore
                hdfs_client.rename_dont_move(self._get_temp_output_path(), self._get_output_path())
    return write_wrapper


def normalize_local_filename(filename):
    # type: (str) -> str
    """
    Normalizes a local file full path to be used by python os/file io.

    :param filename: File full path.
    :return: Normalized filename.

    """
    prefix = "file://"  # type: str
    if filename.startswith(prefix):
        filename = filename.replace(prefix, "")
    return filename


class BaseTYPySparkTask(PySparkTask):

    atomic_output = luigi.BoolParameter(
        description='Writes the output into a temporary target first and then moves to the intended output directory.',
        default=True
    )

    def __init__(self, *args, **kwargs):  # noqa: D102
        super(BaseTYPySparkTask, self).__init__(*args, **kwargs)
        # initiate a random suffix for atomic write. Initiate it here because we need it in other places in the class.
        # Not initiating the full temporary path here because then it needs to call output().path and every time someone
        # tests a BaseReviewPrecompPySparkTask needs to mock output() or _get_output_path()
        self.random_suffix = random.randrange(0, 10000000000)  # type: int

    def _get_input_path(self, name=None):
        """ Utility method to obtain the path from the input() method """
        _input = self.input()

        if name and not isinstance(_input, dict):
            raise ValueError("Cannot get named output from a non dict-like.")

        if name and name not in _input:
            raise ValueError(
                "Input '{}' not found. Verify the requires method".format(name)
            )
        if name:
            return _input[name].path
        else:
            return _input.path

    def _get_output_path(self):
        return self.output().path

    def _get_temp_output_path(self):
        return generate_temporary_path_name(self._get_output_path(), self.random_suffix)

    def _is_atomic_output(self):
        return self.atomic_output

    # pyspark is only sent to the cluster with luigi PySparkTask tasks. So for non-PySparkTasks -- in this file
    # there are many non-PySparkTasks and if we did an `import pyspark`, they would fail -- pyskark
    # package has to be sent by adding it to py-package, which would be redundant and not certain if that will
    # result into conflicts. So we don't send pyspark over the cluster just for this type annotation, and
    # therefore ignoring it
    @write_decorator
    def write_jsonl_output(self, rdd, sc):
        # type: (Any, Any) -> None
        """
        Writes a json lines output from an RDD.

        :param rdd: An RDD.
        :param sc: A SparkContext.

        :return: None
        """

        if self._is_atomic_output():
            out_path = self._get_temp_output_path()  # type: str
        else:
            out_path = self._get_output_path()
        rdd.saveAsTextFile(out_path)

    @write_decorator
    def write_parquet_output(self, df, sc, compression="snappy", mode="overwrite"):
        # type: (Any, Any, str, str) -> None
        """
        Writes parquet output from a dataframe.

        :param df: A dataframe.
        :param sc: A SparkContext.
        :param compression: Compression to be used.
        :param mode: Write mode.
        :return: None
        """
        if self._is_atomic_output():
            out_path = self._get_temp_output_path()  # type: str
        else:
            out_path = self._get_output_path()
        df.write.parquet(
            out_path,
            mode=mode,
            compression=compression
        )

    @write_decorator
    def write_csv_output(self, df, sc, sep='\t', quote=None, escape=None, escapeQuotes=None):
        # type: (Any, Any, str, str, str, str) -> None
        """
        Writes csv output from a dataframe.

        :param df: A dataframe.
        :param sc: A SparkContext.
        :param sep: Separator.
        :param quote: sets the single character used for escaping quoted values where the separator can be part of the \
        value. If None is set, it uses the default value, ". If you would like to turn off quotations, you need to set \
        an empty string.
        :param escape:  sets the single character used for escaping quotes inside an already quoted value. \
        If None is set, it uses the default value.
        :param escapeQuotes: A flag indicating whether values containing quotes should always be enclosed in quotes. \
        If None is set, it uses the default value true, escaping all values containing a quote character.

        :return: None
        """
        if self._is_atomic_output():
            out_path = self._get_temp_output_path()  # type: str
        else:
            out_path = self._get_output_path()

        df.write.csv(out_path, sep=sep, quote=quote, escape=escape, escapeQuotes=escapeQuotes)

    def main(self, sc, *args):  # noqa: D102
        pass
