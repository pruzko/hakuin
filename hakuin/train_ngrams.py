import os
import dill
from nltk.lm import MLE
from nltk.lm.preprocessing import padded_everygram_pipeline

from hakuin import Hakuin


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'data', 'models')
DIR_CORPUSES = os.path.join(DIR_ROOT, 'data', 'corpuses')


def fetch_data(fname):
    with open(fname, 'r') as f:
        data = [l.strip() for l in f]
        data = [d.split(',') for d in data]
        data = [x for d in data for x in [d[0]] * int(d[1]) if '[' not in x]
    return data


def main():
    print('Tables...')
    data = fetch_data(os.path.join(DIR_CORPUSES, 'tables.csv'))
    train, vocab = padded_everygram_pipeline(Hakuin.MAX_NGRAM, data)
    model = MLE(Hakuin.MAX_NGRAM)
    model.fit(train, vocab)

    with open(os.path.join(DIR_MODELS, 'ngram_tables.pkl'), 'wb') as f:
        dill.dump(model, f)
    print('Done.')

    print('Columns...')
    data = fetch_data(os.path.join(DIR_CORPUSES, 'columns.csv'))
    train, vocab = padded_everygram_pipeline(Hakuin.MAX_NGRAM, data)
    model = MLE(Hakuin.MAX_NGRAM)
    model.fit(train, vocab)

    with open(os.path.join(DIR_MODELS, 'ngram_columns.pkl'), 'wb') as f:
        dill.dump(model, f)
    print('Done.')


if __name__ == '__main__':
    main()
