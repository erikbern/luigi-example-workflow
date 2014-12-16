from other_module import subsample_features_job, train_classifier, inspect_model
 
if __name__ == '__main__':
  dates = [datetime.date(2013, 11, 1) + datetime.timedelta(i) for i in xrange(7)]
  inputs = [date.strftime('/log/endsongsource/%Y-%m-%d') for date in dates]
  
  subsample_features_job(inputs, '/tmp/subsampled')
  train_classifier('subsampled.txt', 'model.pickle')
  inspect_model('model.pickle')
