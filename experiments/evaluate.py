import code
import copy
import json
import math
import os
import string
import sys

import dill

import hakuin
from hakuin.utils import split_to_ctx
from huffman import huffman


CHARSET_ASCII = [chr(x) for x in range(128)] + ['</s>']
CHARSET_LOWER = list(string.ascii_lowercase) + ['</s>']
CHARSET_UPPER = list(string.ascii_uppercase) + ['</s>']
CHARSET_OTHER = [x for x in CHARSET_ASCII if x not in CHARSET_LOWER + CHARSET_UPPER] + ['</s>']



def load_dbanswers_data():
    data = []
    for file in os.listdir('dbanswers'):
        with open(os.path.join('dbanswers', file), 'r') as f:
            data.append(json.load(f))
    return data


def load_generic_db():
    with open(os.path.join('generic_db', 'db.json'), 'r') as f:
        data = json.load(f)

    res = {}
    for table, rows in data.items():
        if not rows:
            continue

        for column in rows[0]:
            res[f'{table}.{column}'] = [r[column] for r in rows]

    return res


def bin_search(s, charset):
    total = 0
    for c in s:
        if c in charset:
            total += math.log(len(charset), 2)
        else:
            total += math.log(len(CHARSET_ASCII) - len(charset), 2)
    # EOS
    if '</s>' in charset:
        total += math.log(len(charset), 2)
    else:
        total += math.log(len(CHARSET_ASCII) - len(charset), 2)
    return total


def get_plain_guesses(scores, correct):
    alphabet = sorted(list(scores.items()), key=lambda x: x[1], reverse=True)
    alphabet = [x[0] for x in alphabet]
    return alphabet.index(correct) + 1


def get_gradual_miss_resolve(scores, correct):
    total = 0

    charsets = (CHARSET_LOWER, CHARSET_UPPER, CHARSET_OTHER)
    for charset in charsets:
        total += 1
        if correct in charset:
            already_tried = set(scores).intersection(set(charset))
            total += math.log(len(charset) - len(already_tried), 2)
            break

    return total


def hakuin_search(model, s, ngram, mode, selection, downgrading, threshold_scores=None, threshold_counts=None, gradual_miss_resolve=False):
    assert selection in ['plain', 'huffman']
    assert mode in ['schema', 'generic']

    contexts = [list(ctx) for ctx in split_to_ctx(s, ngram)]

    total = 0.0
    for i, ctx in enumerate(contexts):
        correct = ctx.pop(-1)

        scores = {}
        if downgrading:
            for i in range(len(ctx) + 1):
                scores = model.score_dict(ctx[i:])
                if correct in scores:
                    break
        else:
            scores = model.score_dict(ctx)

        if threshold_scores is not None:
            scores = {c: score for c, score in scores.items() if score >= threshold_scores}
        if threshold_counts is not None:
            scores = {c: score for c, score in scores.items() if model.count(c, ctx) >= threshold_counts}

        huff = huffman(scores)

        # hit
        if correct in scores:
            if selection == 'plain':
                total += get_plain_guesses(scores, correct)
            else:
                total += huff[correct]
            continue

        # miss penalty
        if selection == 'plain':
            total += len(scores)
        else:
            total += max(huff.values()) if huff else 0

        # exhaustive search
        if mode == 'schema':
            total += math.log(len(hakuin.CHARSET_SCHEMA) - len(scores), 2)
        else:
            if gradual_miss_resolve:
                total += get_gradual_miss_resolve(scores, correct)
            else:
                total += math.log(len(CHARSET_ASCII) - len(scores), 2)

        # if correct in string.ascii_lowercase:
        #     print('LOWER')
        # elif correct in string.ascii_uppercase:
        #     print('UPPER')
        # elif correct.isdigit():
        #     print('DIGIT')
        # else:
        #     print(f'OTHER: "{correct}"')

    return total


def hakuin_analyze_per_idx(model, s, ngram, res, charset=None, selection='plain'):
    assert selection in ['plain', 'huffman']
    contexts = split_to_ctx(s, ngram)

    for i, ctx in enumerate(contexts):
        correct = ctx.pop(-1)
        scores = model.score_dict(ctx)
        huff = huffman(scores)

        total = get_plain_guesses(scores, correct) if selection == 'plain' else huff[correct]
        res[str(i)]['total'] += total
        res[str(i)]['n'] += 1


def count_data(data):
    return len(''.join(data)) + len(data)      # adding EOS


def count_bin_search(data, charset):
    return sum([bin_search(d, charset) for d in data])


def count_hakuin(data, model, mode, downgrading, threshold_scores=None, threshold_counts=None, gradual_miss_resolve=False):
    res = {}

    # for i in range(1, model.max_ngram + 1):
    for i in [5]:
        for d in data:
            res[f'{i}_plain'] = res.get(f'{i}_plain', 0)
            res[f'{i}_huffman'] = res.get(f'{i}_huffman', 0)
            # res[f'{i}_plain'] += hakuin_search(model, d, ngram=i, mode=mode, selection='plain', downgrading=downgrading, threshold_scores=threshold_scores, threshold_counts=threshold_counts)
            res[f'{i}_huffman'] += hakuin_search(
                model,
                d,
                ngram=i,
                mode=mode,
                selection='huffman',
                downgrading=downgrading,
                threshold_scores=threshold_scores,
                threshold_counts=threshold_counts,
                gradual_miss_resolve=gradual_miss_resolve
            )

    return res
