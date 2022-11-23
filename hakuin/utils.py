import string
from functools import partial
from itertools import chain, islice

from nltk.util import ngrams as nltk_ngrams
from nltk.lm.preprocessing import pad_both_ends



CHARSET_ASCII = [chr(x) for x in range(128)] + ['</s>']
CHARSET_SCHEMA = list(string.ascii_lowercase + string.digits + '_#@') + ['</s>']
CHARSET_LOWER = list(string.ascii_lowercase) + ['</s>']



def everygrams(s, max_ngram):
    egrams = [ngrams(s, i) for i in range(1, max_ngram + 1)]
    for egram in egrams:
        for ngram in egram:
            yield ngram


def ngrams(s, n):
    return nltk_ngrams(tuple(s) + ('</s>',), n=n, pad_left=True, left_pad_symbol='<s>')


def tokenize(s):
    return ['<s>'] + list(s) + ['</s>']


def padded_everygram_pipeline(data, max_ngram):
    train = (everygrams(s, max_ngram) for s in data)
    vocab = chain.from_iterable(map(tokenize, data))
    return train, vocab


def padded_correct_everygram_pipeline(s, correct, max_ngram):
    ctx = list(s) + [correct]
    if len(ctx) < max_ngram:
        ctx = ['<s>'] * max_ngram + ctx
    ctx = ctx[-max_ngram:]
    train = (ctx[i:] for i in range(max_ngram))
    return (train, ), ctx


def split_to_ctx(s, ngram):
    return list(ngrams(s, ngram))


def split_to_batches(l, size):
    it = iter(l)
    return iter(lambda: tuple(islice(it, size)), ())


def split_at(s, i):
    return s[:i], s[i:]