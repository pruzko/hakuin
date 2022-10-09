import os
import string

import dill


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'data', 'models')
DIR_CORPUSES = os.path.join(DIR_ROOT, 'data', 'corpuses')


class Hakuin:
    MAX_NGRAM = 4
    ALPHABET = list(string.ascii_lowercase + string.digits + '_#$@') + ['</s>']

    def __init__(self):
        with open(os.path.join(DIR_MODELS, 'ngram_tables.pkl'), 'rb') as f:
            self.model_tables = dill.load(f)
        with open(os.path.join(DIR_MODELS, 'ngram_columns.pkl'), 'rb') as f:
            self.model_columns = dill.load(f)


    def predict_table(self, history, ngram=4):
        return self.predict(history=history, mode='t', ngram=ngram)


    def predict_column(self, history, ngram=4):
        return self.predict(history=history, mode='c', ngram=ngram)


    def predict(self, history, mode, ngram=4):
        assert ngram > 0 and ngram <= self.MAX_NGRAM, f'ngram must be in <1, {self.MAX_NGRAM}>'
        assert mode in ['t', 'c'], f'mode must be either "t" for table or "c" for column'
        history = ['<s>'] * (ngram - 1) + list(history)
        history = history[-ngram + 1:]
        model = self.model_tables if mode == 't' else self.model_columns
        return {c: model.score(c, history) for c in self.ALPHABET}
