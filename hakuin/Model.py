import copy
import os

import dill
from nltk.lm import MLE

import hakuin
from hakuin.utils import SOS



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'data', 'models')



class Model:
    @staticmethod
    def make_clean(ngram):
        m = Model()
        m.model = MLE(ngram)
        return m


    def __init__(self):
        self.model = None


    def load(self, model_path):
        with open(model_path, 'rb') as f:
            self.model = dill.load(f)


    def score(self, s, context):
        return self.score_dict(context).get(s, 0.0)


    def score_dict(self, context):
        context = self.model.vocab.lookup(context) if context else None
        counts = self.model.context_counts(context)
        return {c: counts.freq(c) for c in counts}


    def score_any_dict(self, context):
        context = copy.deepcopy(context)

        while context:
            scores = self.score_dict(context)
            if scores:
                return scores
            context.pop(0)

        return self.score_dict([])


    def count(self, s, context):
        return self.count_dict(context).get(s, 0.0)


    def count_dict(self, context):
        context = self.model.vocab.lookup(context) if context else None
        return self.model.context_counts(context)


    def _fit(self, train, vocab):
        self.model.vocab.update(vocab)
        self.model.counts.update(self.model.vocab.lookup(t) for t in train)


    def fit(self, data):
        train, vocab = hakuin.utils.padded_everygram_pipeline(data, self.max_ngram)
        self._fit(train, vocab)


    def fit_correct(self, ctx, correct):
        if type(ctx) == str:
            # TODO remove all ngrams like [SOS, SOS, SOS, 'A'] and replace them with [SOS, 'A'] and ['A']
            # then retrain the models (fix the whole codebase)
            ctx = [SOS] * self.max_ngram + list(ctx)

        ctx = ctx + [correct]
        ctx = ctx[-self.max_ngram:]

        train = (ctx[i:] for i in range(self.max_ngram))
        train = (train, )
        self._fit(train, ctx)


    @property
    def max_ngram(self):
        assert self.model
        return self.model.order



_m_tables = None
_m_columns = None


def get_model_tables():
    global _m_tables
    if _m_tables is None:
        _m_tables = Model()
        _m_tables.load(os.path.join(DIR_MODELS, 'model_tables.pkl'))
    return _m_tables


def get_model_columns():
    global _m_columns
    if _m_columns is None:
        _m_columns = Model()
        _m_columns.load(os.path.join(DIR_MODELS, 'model_columns.pkl'))
    return _m_columns
