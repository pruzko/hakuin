import code
import collections
import copy
import json
import math
import os
import string
import sys

import dill

import hakuin
from hakuin.utils import split_to_ctx, split_to_batches
from huffman import huffman


CHARSET_ASCII = [chr(x) for x in range(128)] + ['</s>']
CHARSET_LOWER = list(string.ascii_lowercase) + ['</s>']
CHARSET_UPPER = list(string.ascii_uppercase) + ['</s>']
CHARSET_OTHER = [x for x in CHARSET_ASCII if x not in CHARSET_LOWER + CHARSET_UPPER] + ['</s>']



def load_dbanswers_data():
    data = []
    for file in os.listdir('dbanswers'):
        if not file.endswith('.json'):
            continue
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
        total += math.log(len(charset), 2)
        if c not in charset:
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


def hakuin_search(
        s,
        model,
        ngram,
        mode,
        selection,
        downgrading,
        threshold_scores,
        threshold_counts,
        gradual_miss_resolve,
        multi_word
    ):
    assert selection in ['plain', 'huffman']
    assert mode in ['schema', 'generic']

    contexts = [list(ctx) for ctx in split_to_ctx(s, ngram)]

    total = 0.0
    for ctx in contexts:
        correct = ctx.pop(-1)

        scores = {}
        if downgrading:
            for i in range(len(ctx) + 1):
                scores = model.score_dict(ctx[i:])
                if scores:
                    break
        else:
            scores = model.score_dict(ctx)

        if threshold_scores is not None:
            scores = {c: score for c, score in scores.items() if score >= threshold_scores}
        if threshold_counts is not None:
            scores = {c: score for c, score in scores.items() if model.count(c, ctx) >= threshold_counts}

        if multi_word and '</s>' in scores:
            scores[' '] = scores['</s>']

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

    return total


def count_data(data):
    return len(''.join(data)) + len(data)      # adding EOS


def count_bin_search(data, charset):
    return sum([bin_search(d, charset) for d in data])


def count_hakuin(data, **kwargs):
    return sum([hakuin_search(s=s, **kwargs) for s in data])


def count_hakuin_adaptive(data, batch_size, kwargs_generic, kwargs_adaptive):
    model_adaptive = None
    total = 0.0

    for batch in split_to_batches(data, batch_size):
        if not model_adaptive:
            model_adaptive = hakuin.get_model_clean(kwargs_adaptive['ngram'])
            if kwargs_generic is None:
                total += count_bin_search(batch, CHARSET_ASCII)
            else:
                total += count_hakuin(batch, **kwargs_generic)
        else:
            total += count_hakuin(batch, model=model_adaptive, **kwargs_adaptive)

        model_adaptive.fit(batch)

    return total
