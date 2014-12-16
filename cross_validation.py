from luigi_workflow_full import TrainClassifier, SubsampleFeatures, read_input
import pickle
from sklearn.metrics import roc_curve, auc

class CrossValidation(luigi.Task):
    date_interval_a = luigi.DateIntervalParameter()
    date_interval_b = luigi.DateIntervalParameter()

    def requires(self):
        return (SubsampleFeatures(self.date_interval_a),
                SubsampleFeatures(self.date_interval_b),
                TrainClassifier(self.date_inteval_a))

    def run(self):
        sf_a, sf_b, tc_a = self.input()

        classifier = pickle.load(tc_a.open('r'))

        for tag, input in [('A', sf_a), ('B', sf_b)]:
            X, y = read_input(input.open('r'))
            
            y_pred = classifier.predict(X)
            print '%s AUC: %s' % (tag, auc(*roc_curve(y, y_pred)))

if __name__ == '__main__':
    luigi.run()
