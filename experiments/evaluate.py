import json
import math
import os
import sys

import dill
from nltk.util import ngrams
from nltk.lm.preprocessing import pad_both_ends

from huffman import huffman

DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_HAKUIN = os.path.abspath(os.path.join(DIR_ROOT, 'hakuin'))

sys.path.append(DIR_HAKUIN)
from hakuin import Hakuin



def load_dbanswers_data():
    data = []
    for file in os.listdir('dbanswers'):
        with open(os.path.join('dbanswers', file), 'r') as f:
            data.append(json.load(f))
    return data


def eval_bin_search(data):
    return math.log(len(Hakuin.ALPHABET), 2) * len(data)


def eval_hakuin(hakuin, data, mode):
    res = {}
    for i in range(1, Hakuin.MAX_NGRAM + 1):
        padding = 2 if i == 1 else i
        new_data = [x for d in data for x in ngrams(pad_both_ends(d, n=padding), n=i)]
        new_data = [d for d in new_data if d.count('<s>') != len(d) and d.count('</s>') < 2]

        res[str(i)] = {
            'huffman': 0,
            'plain': 0,
        }

        for seq in new_data:
            char_freq = hakuin.predict(history=seq[:i-1], mode=mode, ngram=i)

            h = huffman(char_freq)
            res[str(i)]['huffman'] += h[seq[-1]]

            alphabet = sorted([item for item in char_freq.items()], key=lambda x: x[1], reverse=True)
            alphabet = [x[0] for x in alphabet]
            res[str(i)]['plain'] += alphabet.index(seq[-1])

    return res


def evaluate(data, hakuin, mode):
    merged = '!'.join(data)
    total = len(merged) + 1
    bin_search = eval_bin_search(merged) / total
    hakuin_res = eval_hakuin(hakuin, data, mode)

    if mode == 't':
        print('=== TABLES ===')
    else:
        print('=== COLUMNS ===')
    print('total:', total)
    print('bin_search:', bin_search)
    for ngram in hakuin_res:
        for t, c in hakuin_res[ngram].items():
            print(f'hakuin_{ngram}_{t}:', c / total)


def main():
    hakuin = Hakuin()
    data = load_dbanswers_data()
    tab_data = [t['table'] for tables in data for t in tables]
    evaluate(tab_data, hakuin, mode='t')
    col_data = [c for tables in data for t in tables for c in t['columns']]
    evaluate(col_data, hakuin, mode='c')


if __name__ == '__main__':
    main()
