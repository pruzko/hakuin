import asyncio
import os
import pickle
import sys
import tqdm

from nltk.lm import MLE

from hakuin import Model
from hakuin.utils import DIR_MODELS


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_CORPORA = os.path.join(DIR_ROOT, 'corpora')


def fetch_data(fname):
    with open(fname, 'r') as f:
        data = [l.strip() for l in f]
        data = [d.split(',') for d in data]
        data = [x for d in data for x in [d[0]] * int(d[1])]
    return data


async def main():
    for m_type in tqdm.tqdm(['schemas', 'tables', 'columns']):
        tqdm.tqdm.write(f'Training {m_type}. This may take a while...', file=sys.stderr)
        data = fetch_data(os.path.join(DIR_CORPORA, f'{m_type}.csv'))
        m = Model(5)
        await m.fit_data(data)

        tqdm.tqdm.write(f'Saving {m_type}...', file=sys.stderr)
        with open(os.path.join(DIR_MODELS, f'model_{m_type}.pkl'), 'wb') as f:
            pickle.dump(m, f)

        tqdm.tqdm.write(f'Done.', file=sys.stderr)


if __name__ == '__main__':
    asyncio.run(main())
