import os
import re
import string
import sys

from tqdm import tqdm

from .info_messages import _INFO_MESSAGES



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_MODELS = os.path.join(DIR_ROOT, 'models')
if not os.path.isdir(DIR_MODELS):
    DIR_MODELS = os.path.abspath(os.path.join(DIR_ROOT, '..', 'models'))


RE_CAMEL_CASE = re.compile("(?<!^)(?=[A-Z])")


ASCII_MAX = 0x7f
UNICODE_MAX = 0x10ffff
BYTE_MAX = 0xff

SOS = '<s>'
EOS = '</s>'

CHARSET_DIGITS = list(string.digits) + [EOS]





def info(msg, *args, progress=None):
    '''Prints debug information.

    Params:
        msg (str): message name
        *args: format string arguments
        progress (tqdm.tqdm | None): progress object
    '''
    progress = progress or tqdm
    progress.write(_INFO_MESSAGES[msg].format(*args), file=sys.stderr)    


def pascal_to_snake_case(s):
    '''Converts PascalCase to snake_case.
    
    Params:
        s (str): string in PascalCase

    Returns:
        str: string in snake_case
    '''
    return RE_CAMEL_CASE.sub('_', s).lower()


def snake_to_pascal_case(s):
    '''Converts snake_case to PascalCase.
    
    Params:
        s (str): string in snake_case
        
    Returns:
        str: string in PascalCase
    '''
    return ''.join(w.capitalize() for w in s.split('_'))


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
