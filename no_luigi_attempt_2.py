import os
from other_module import subsample_features_job, train_classifier, upload_model
 
if __name__ == '__main__':
  if not os.path.exists('model.pickle'):
    if not hdfs.exists('/tmp/subsampled'):
      dates = [datetime.date(2013, 11, 1) + datetime.timedelta(i) for i in xrange(7)]
      inputs = [date.strftime('/log/endsongcleaned/%Y-%m-%d') for date in dates]
      try:
        subsample_features(inputs, '/tmp/subsampled')
      except:
        hdfs.remove('/tmp/subsampled')
        raise
      
    train_classifier('/tmp/subsampled', 'model.pickle')
  upload_model('model.pickle', 'server.spotify.net')
