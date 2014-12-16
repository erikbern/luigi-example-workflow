import luigi, luigi.hadoop, luigi.hdfs
from other_luigi_module import EndSong
 
class SubsampleFeatures(luigi.hadoop.JobTask):
  date_interval = luigi.DateIntervalParameter()
  def requires(self): return [EndSong(date) for date in self.date_interval]
  def mapper(self, line): pass # TODO: implement
  def reducer(self): pass # TODO: implement
  def output(self): return luigi.hdfs.HdfsTarget('/tmp/subsampled-%s' % self.date_interval)
  
class TrainClassifier(luigi.Task):
  date_interval = luigi.DateIntervalParameter()
  n_trees = luigi.IntParameter(default=10)
  def requires(self): return SubsampleFeatures(self.date_interval)
  def run(self): pass # TODO: implement
  def output(self): return luigi.LocalTarget('model-%s.pickle' % self.date_interval)
 
class UploadModel(luigi.Task):
  date_interval = luigi.DateIntervalParameter()
  n_trees = luigi.IntParameter(default=10)
  def requires(self): return TrainClassifier(self.date_interval, self.n_trees)
  def run(self): pass # TODO: implement
 
if __name__ == '__main__':
  luigi.run()
