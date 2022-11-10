import os

import dill
from nltk.lm import MLE

import hakuin.utils



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..', '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'data', 'models')
DIR_CORPUSES = os.path.join(DIR_ROOT, 'data', 'corpuses')
DIR_BOOKCORPUS = os.path.join(DIR_ROOT, 'data', 'bookcorpus')

MAX_NGRAM = 5


def fetch_data(fname):
    with open(fname, 'r') as f:
        data = [l.strip() for l in f]
        data = [d.split(',') for d in data]
        data = [x for d in data for x in [d[0]] * int(d[1])]
    return data


def main():
    # print('Tables...')
    # data = fetch_data(os.path.join(DIR_CORPUSES, 'tables.csv'))
    # train, vocab = hakuin.utils.padded_everygram_pipeline(data, MAX_NGRAM)
    # model = MLE(MAX_NGRAM)
    # model.fit(train, vocab)

    # with open(os.path.join(DIR_MODELS, 'model_tables.pkl'), 'wb') as f:
    #     dill.dump(model, f)
    # print('Done.')

    # print('Columns...')
    # data = fetch_data(os.path.join(DIR_CORPUSES, 'columns.csv'))
    # train, vocab = hakuin.utils.padded_everygram_pipeline(data, MAX_NGRAM)
    # model = MLE(MAX_NGRAM)
    # model.fit(train, vocab)

    # with open(os.path.join(DIR_MODELS, 'model_columns.pkl'), 'wb') as f:
    #     dill.dump(model, f)
    # print('Done.')

    print('Generic...')
    data = fetch_data(os.path.join(DIR_BOOKCORPUS, 'wordlist.csv'))
    train, vocab = hakuin.utils.padded_everygram_pipeline(data, MAX_NGRAM)
    model = MLE(MAX_NGRAM)
    model.fit(train, vocab)

    with open(os.path.join(DIR_MODELS, 'model_generic.pkl'), 'wb') as f:
        dill.dump(model, f)
    print('Done.')

    # import code; code.interact(local=dict(globals(), **locals()))

if __name__ == '__main__':
    main()
