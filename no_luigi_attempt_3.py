import sys
import argparse
from other_module import subsample_features_job, train_classifier, upload_model
 
if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--date-first', required=True)
  parser.add_argument('--date-last', required=True)
  parser.add_argument('--n-trees', required=True)
  parser.add_argument('--server', default='server.spotify.net')
  args = parser.parse_args()
  
  def parse_date(d): return datetime.date(*(int(x) for x in d.split('-')))
  
  date_start = parse_date(args.date_start)
  date_stop = parse_date(args.date_stop)
  
  model_fn = 'model-%s-%s.pickle' % (date_start, date_stop)
  subsampled_fn = '/tmp/subsampled-%s-%s' % (date_start, date_stop)
  
  if not os.path.exists(model_fn):
    if not hdfs.exists(subsampled_fn):
      inputs = []
      while date_start < date_stop:
        inputs.append(date_start.strftime('/log/endsongsource/%Y-%m-%d'))
 
      try:
        subsample_features_job(inputs, subsampled_fn)
      except:
        hdfs.remove(subsampled_fn)
        raise
      
    train_classifier(subsampled_fn, model_fn, n_trees=args.n_trees)
  upload_model(model_fn, args.server)
