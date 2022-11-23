import os

import dill
from nltk.lm import MLE

import hakuin



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'data', 'models')



class Model:
    def __init__(self, model_path=None):
        self.model = None
        if model_path:
            self.load_model(model_path)


    def load_model(self, model_path):
        with open(model_path, 'rb') as f:
            self.model = dill.load(f)


    def score(self, s, context):
        return self.score_dict(context).get(s, 0.0)


    def score_dict(self, context):
        context = self.model.vocab.lookup(context) if context else None
        counts = self.model.context_counts(context)
        return {c: counts.freq(c) for c in counts}


    def count(self, s, context):
        return self.count_dict(context).get(s, 0.0)


    def count_dict(self, context):
        context = self.model.vocab.lookup(context) if context else None
        return self.model.context_counts(context)


    def _fit(self, train, vocab):
        # nltk fit method does not gradually update the vocabulary 
        self.model.vocab.update(vocab)
        self.model.counts.update(self.model.vocab.lookup(t) for t in train)


    def fit(self, data):
        train, vocab = hakuin.utils.padded_everygram_pipeline(data, self.max_ngram)
        self._fit(train, vocab)


    def fit_correct(self, s, correct):
        train, vocab = hakuin.utils.padded_correct_everygram_pipeline(s, correct, self.max_ngram)
        self._fit(train, vocab)


    @property
    def max_ngram(self):
        assert self.model
        return self.model.order


def get_model_clean(ngram):
    m = Model()
    m.model = MLE(ngram)
    return m


def get_model_tables():
    return Model(model_path=os.path.join(DIR_MODELS, 'model_tables.pkl'))


def get_model_columns():
    return Model(model_path=os.path.join(DIR_MODELS, 'model_columns.pkl'))


def get_model_generic():
    return Model(model_path=os.path.join(DIR_MODELS, 'model_generic.pkl'))


# import code; code.interact(local=dict(globals(), **locals()))