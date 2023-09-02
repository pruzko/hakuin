import os
import string
from functools import partial
from itertools import chain, islice

from nltk.util import ngrams as nltk_ngrams
from nltk.lm.preprocessing import pad_both_ends



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'data', 'models')

CHARSET_ASCII = [chr(x) for x in range(128)] + ['</s>']
CHARSET_SCHEMA = list(string.ascii_lowercase + string.digits + '_#@') + ['</s>']

EOS = '</s>'
SOS = '<s>'


def everygrams(s, max_ngram):
    egrams = [ngrams(s, i) for i in range(1, max_ngram + 1)]
    for egram in egrams:
        for ngram in egram:
            yield ngram


def ngrams(s, n):
    return nltk_ngrams(tuple(s) + ('</s>',), n=n, pad_left=True, left_pad_symbol='<s>')


def tokenize(s, add_sos=True, add_eos=True):
    tokens = [SOS] if add_sos else []
    tokens += list(s)
    tokens += [EOS] if add_eos else []
    return tokens


def padded_everygram_pipeline(data, max_ngram):
    train = (everygrams(s, max_ngram) for s in data)
    vocab = chain.from_iterable(map(tokenize, data))
    return train, vocab


def split_to_ctx(s, ngram):
    return list(ngrams(s, ngram))


def split_to_batches(l, size):
    it = iter(l)
    return iter(lambda: tuple(islice(it, size)), ())


def split_at(s, i):
    return s[:i], s[i:]
