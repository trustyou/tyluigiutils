import luigi.contrib.hdfs
import luigi.task


class ExternalFileTask(luigi.task.ExternalTask):
    """
    Use this task to model a dependency on a file that is created outside of
    Luigi. This task has no run method, so if the expected file is not
    present, this task will fail, which is the only thing we can do in this
    case.
    """

    output_path = luigi.Parameter()
    hdfs = luigi.Parameter(default=True)

    def output(self):
        if self.hdfs:
            return luigi.contrib.hdfs.HdfsTarget(self.output_path)
        else:
            return luigi.LocalTarget(self.output_path)
