from luigi.contrib.hadoop import JobTask, HadoopJobRunner
from luigi import configuration, Parameter

import shutil
import tempfile
import os
import logging

logger = logging.getLogger('luigi-interface')


class VenvJobTask(JobTask):
	"""
	Hadoop job which runs in a virtual environment.

	It uses the configuration 'venv_path' in the 'hadoop'.
	If the venv_path is a zip file it will use it directly
	as archive. if it is a directory will compress it and ship
	it the job. The venv can be also located  directly on HDFS,
	use the hdfs:// url prefix to indicate it.
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
