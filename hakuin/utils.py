from functools import partial
from itertools import chain, islice

from nltk.util import ngrams as nltk_ngrams
from nltk.lm.preprocessing import pad_both_ends



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


def split_to_ctx(s, ngram):
    return list(ngrams(s, ngram))


def split_to_batches(l, size):
    it = iter(l)
    return iter(lambda: tuple(islice(it, size)), ())