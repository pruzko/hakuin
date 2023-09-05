from itertools import chain

from nltk.util import ngrams as nltk_ngrams
from nltk.lm.preprocessing import pad_both_ends

from hakuin.utils import SOS, EOS, tokenize



def ngrams(s, order):
    '''Creates ngrams from string.

    Params:
        s (str): string to create ngrams from
        order (int): order of ngrams

    Returns:
        zip: ngrams
    '''
    return nltk_ngrams(tuple(s) + (EOS,), n=order, pad_left=True, left_pad_symbol=SOS)


def everygrams(s, order):
    '''Creates everygram from string.

    Params:
        s (str): string to create ngrams from
        order (int): order of everygrams

    Returns:
        generator: ngrams
    '''
    egrams = [ngrams(s, i) for i in range(1, order + 1)]
    for egram in egrams:
        for ngram in egram:
            yield ngram


def padded_everygram_pipeline(data, order):
    '''Creates character-based train set and vocabulary.

    Params:
        data (list): train set strings
        order (int): order of everygrams

    Returns:
        (generator, itertools.chain): train set and vocabulary
    '''
    train = (everygrams(s, order) for s in data)
    vocab = chain.from_iterable(map(tokenize, data))
    return train, vocab
