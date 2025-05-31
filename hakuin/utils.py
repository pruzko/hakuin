import os
import re
import string
import sys
from enum import Enum

from tqdm import tqdm



DIR_ROOT = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
DIR_MODELS = os.path.join(DIR_ROOT, 'models')
if not os.path.isdir(DIR_MODELS):
    DIR_MODELS = os.path.abspath(os.path.join(DIR_ROOT, '..', 'models'))


RE_CAMEL_CASE = re.compile("(?<!^)(?=[A-Z])")


ASCII_MAX = 0x7f
UNICODE_MAX = 0x10ffff
BYTE_MAX = 0xff


class Symbol(Enum):
    '''Special character symbols.'''
    SOS = '<s>'
    EOS = '</s>'


CHARSET_DIGITS = list(string.digits) + [Symbol.EOS]


_INFO_MESSAGES = {
    'extracting_column': 'Extracting column "{}.{}"',
    'extracting_column_names': 'Extracting column names of "{}"',
    'extracting_table_names': 'Extracting tables names.',
    'extracting_schema_names': 'Extracting schema names.',
    'row_extracted': '({}/{}): {}',
}


def info(msg, *args, progress=None):
    '''Prints debug information.

    Params:
        msg (str): message name
        *args: format string arguments
        progress (tqdm.tqdm|None): progress object
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


def to_chars(s):
    '''Converts a string into a list of characters.

    Params:
        s (str|bytes): string

    Returns:
        list: characters
    '''
    return [bytes([b]) for b in s] if type(s) is bytes else list(s)
