from luigi_workflow_full import TrainClassifier, SubsampleFeatures, read_input
import pickle
from sklearn.metrics import roc_curve, auc
import luigi

class CrossValidation(luigi.Task):
    date_interval_a = luigi.DateIntervalParameter()
    date_interval_b = luigi.DateIntervalParameter()
    n_trees = luigi.IntParameter(default=10)

    def requires(self):
        return (SubsampleFeatures(self.date_interval_a),
                SubsampleFeatures(self.date_interval_b),
                TrainClassifier(self.date_interval_a, self.n_trees))

    def run(self):
        sf_a, sf_b, tc_a = self.input()

        classifier = pickle.load(tc_a.open('r'))

        for date_interval, tag, input in [(self.date_interval_a, 'train', sf_a),
                                          (self.date_interval_b, 'test', sf_b)]:
            X, y = read_input(input)
            
            y_pred = classifier.predict(X)
            fpr, tpr, _ = roc_curve(y, y_pred)
            print '%s (%5s) AUC: %.4f' % (date_interval, tag, auc(fpr, tpr))

if __name__ == '__main__':
    luigi.run()
