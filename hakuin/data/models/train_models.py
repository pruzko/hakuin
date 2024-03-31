import asyncio
import os
import pickle

from nltk.lm import MLE

import hakuin



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DATA = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_CORPORA = os.path.join(DIR_DATA, 'corpora')
DIR_MODELS = os.path.join(DIR_DATA, 'models')




def fetch_data(fname):
    with open(fname, 'r') as f:
        data = [l.strip() for l in f]
        data = [d.split(',') for d in data]
        data = [x for d in data for x in [d[0]] * int(d[1])]
    return data


async def main():
    print('Schemas...')
    data = fetch_data(os.path.join(DIR_CORPORA, 'schemas.csv'))
    m = hakuin.Model(5)
    await m.fit_data(data)

    with open(os.path.join(DIR_MODELS, 'model_schemas.pkl'), 'wb') as f:
        pickle.dump(m.model, f)
    print('Done.')

    print('Tables...')
    data = fetch_data(os.path.join(DIR_CORPORA, 'tables.csv'))
    m = hakuin.Model(5)
    await m.fit_data(data)

    with open(os.path.join(DIR_MODELS, 'model_tables.pkl'), 'wb') as f:
        pickle.dump(m.model, f)
    print('Done.')

    print('Columns...')
    data = fetch_data(os.path.join(DIR_CORPORA, 'columns.csv'))
    m = hakuin.Model(5)
    await m.fit_data(data)

    with open(os.path.join(DIR_MODELS, 'model_columns.pkl'), 'wb') as f:
        pickle.dump(m.model, f)
    print('Done.')


if __name__ == '__main__':
    asyncio.run(main())
