import logging
import sys

import jsonschema
from jsonschema.exceptions import ValidationError
import luigi
import luigi.contrib.hadoop
import simplejson as json


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
            key, value_str = line.decode("utf-8").rstrip(u"\n").split(u"\t", 1)
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

    # TODO Is the output schema verified if the job has no reducer in
    # Luigi 1.0.16?

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
            print >>stdout, u"{0}\t{1}".format(key, json.dumps(value)).encode("utf-8")

class JsonJobTask(JsonInputJobTask, JsonOutputJobTask):
    """
    Hadoop job which reads and writes JSON lines. Keys are plain text, but
    values are JSON-encoded. Fails if JSON decoding or encoding fails for
    any input or output line.

    Optionally, input and/or output can be validated against a JSON schema.
    To achieve this, override the input_schema and/or output_schema methods.
    """
    pass
