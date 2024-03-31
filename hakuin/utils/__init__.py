import os
import string



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'data', 'models')
DIR_QUERIES = os.path.join(DIR_ROOT, 'data', 'queries')

ASCII_MAX = 0x7f
UNICODE_MAX = 0x10ffff
BYTE_MAX = 0xff

CHARSET_DIGITS = list(string.digits) + ['-', '.', '</s>']

EOS = '</s>'
SOS = '<s>'


def split_at(s, i):
    '''Splits sequence.

    Params:
        s (list|str): sequence
        i (int): index to split at

    Returns:
        (list|str, list|str): split sequences
    '''
    return s[:i], s[i:]


def tokenize(s, add_sos=True, add_eos=True, pad_left=1):
    '''Converts string to list of tokens.

    Params:
        s (str): string to tokenize
        add_sos (bool): True if SOS should be included
        add_eos (bool): True if EOS should be included
        pad_left (int): specifies how many SOS should be included if add_sos is on

    Returns:
        list: tokens
    '''
    tokens = [SOS] * pad_left if add_sos else []
    tokens += list(s)
    tokens += [EOS] if add_eos else []
    return tokens
