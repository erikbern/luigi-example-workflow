import pickle
import random
from spotify.reporting_recorts.common import parseString
import luigi, luigi.hadoop, luigi.hdfs
from spotify.luigi.external_shrek import EndSongCleaned
from sklearn.ensemble import GradientBoostingClassifier
 
class SubsampleFeatures(luigi.hadoop.JobTask):
  date_interval = luigi.DateIntervalParameter()

  def requires(self):
    return [EndSongCleaned(date) for date in self.date_interval]

  def mapper(self, line):
    if random.random() > 1e-4: return
    rec = parseString(line)
    features = [rec.time, rec.ms_played, int(rec.shuffle), int(rec.local_track), rec.bitrate]
    features += [int(rec.country == country) for country in ['US', 'SE', 'GB', 'NO', 'FI', 'FR', 'ES']]
    yield features, rec.skipped

  def reducer(self):
    for features, label in values:
      yield features + [label]
    
  def output(self):
    return luigi.hdfs.HdfsTarget('/tmp/subsampled-%s' % self.date_interval)
  
class TrainClassifier(luigi.Task):
  date_interval = luigi.DateIntervalParameter()
  n_trees = luigi.IntParameter(default=10)

  def requires(self):
    return SubsampleFeatures(self.date_interval)

  def run(self):
    X, y = [], []
    for line in self.input().open('r'):
      items = line.strip().split()
      X.append([float(x) for x in items[:-1]])
      y.append(int(items[-1] == 'True'))

      c = GradientBoostingClassifier(n_estimators=self.n_trees)
      c.fit(X, y)
      
      f = self.output().open('w')
      pickle.dump(c, f)
      f.close()

  def output(self):
    return luigi.LocalTarget('model-%s.pickle' % self.date_interval)
 
if __name__ == '__main__':
  luigi.run()
