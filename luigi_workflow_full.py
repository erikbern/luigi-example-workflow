import pickle
import random, sys
from spotify.reporting_records.common import parseString
import luigi, spotify.luigi
from spotify.luigi.external_shrek import EndSongCleaned
 
class SubsampleFeatures(spotify.luigi.HadoopJobTask):
  date_interval = luigi.DateIntervalParameter()

  def requires(self):
    return [EndSongCleaned(date) for date in self.date_interval]

  def mapper(self, line):
    if random.random() > 1e-4: return
    rec = parseString(line)
    features = [rec.time, rec.ms_played, int(rec.shuffle), int(rec.local_track), rec.bitrate]
    yield (features, rec.skipped)

  def reducer(self, features, labels):
    for label in labels:
        yield features + [label]

  def output(self):
    return spotify.luigi.HdfsTarget('/tmp/subsampled-%s' % self.date_interval)


def read_input(input):
  X, y = [], []
  for line in input.open('r'):
    items = line.strip().split()
    X.append([float(x) for x in items[:-1]])
    y.append(int(items[-1] == 'True'))
  return X, y

  
class TrainClassifier(luigi.Task):
  date_interval = luigi.DateIntervalParameter()
  n_trees = luigi.IntParameter(default=10)

  def requires(self):
    return SubsampleFeatures(self.date_interval)

  def run(self):
    from sklearn.ensemble import GradientBoostingClassifier

    X, y = read_input(self.input())
    c = GradientBoostingClassifier(n_estimators=self.n_trees)
    c.fit(X, y)

    f = self.output().open('w')
    pickle.dump(c, f)
    f.close()

  def output(self):
    return luigi.LocalTarget('model-%s-%d.pickle' % (self.date_interval, self.n_trees))


class InspectModel(luigi.Task):
  date_interval = luigi.DateIntervalParameter()
  n_trees = luigi.IntParameter(default=10)

  def requires(self):
    return TrainClassifier(self.date_interval, self.n_trees)

  def run(self):
    model = pickle.load(self.input().open('r'))
    features = ['time', 'ms_played', 'shuffle', 'local_track', 'bitrate']
    for f, weight in zip(features, model.feature_importances_):
      print '%20s %7.4f%%' % (f, weight * 100)
  
 
if __name__ == '__main__':
  luigi.run()
