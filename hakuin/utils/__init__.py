import os
import string
from enum import Enum



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'models')
if not os.path.isdir(DIR_MODELS):
    DIR_MODELS = os.path.abspath(os.path.join(DIR_ROOT, '..', 'models'))

ASCII_MAX = 0x7f
UNICODE_MAX = 0x10ffff
BYTE_MAX = 0xff

SOS = '<s>'
EOS = '</s>'

CHARSET_DIGITS = list(string.digits) + [EOS]


def tokenize(s, add_sos=True, add_eos=True):
    '''Converts string to list of tokens.

    Params:
        s (str|bytes): string to tokenize
        add_sos (bool): True if SOS should be included
        add_eos (bool): True if EOS should be included

    Returns:
        list: tokens
    '''
    tokens = [SOS] if add_sos else []
    tokens += [bytes([b]) for b in s] if type(s) is bytes else list(s)
    tokens += [EOS] if add_eos else []
    return tokens
