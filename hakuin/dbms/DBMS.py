import os
import re

import jinja2
from abc import ABCMeta, abstractmethod

from hakuin.utils import DIR_QUERIES, BYTE_MAX



class DBMS(metaclass=ABCMeta):
    '''Database Management System (DBMS) interface.

    Attributes:
        DATA_TYPES (list): all available data types
    '''
    _RE_ESCAPE = re.compile(r'^[a-zA-Z0-9_#@]+$')
    _RE_NORMALIZE = re.compile(r'[ \n]+')
    DATA_TYPES = []


    def __init__(self):
        self.jj = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(DIR_QUERIES, 'DBMS')))
        self.jj.filters['sql_escape'] = self.sql_escape
        self.jj.filters['sql_hex_str'] = self.sql_hex_str
        self.jj.filters['sql_hex_byte'] = self.sql_hex_byte
        self.jj.filters['sql_len'] = self.sql_len
        self.jj.filters['sql_char_at'] = self.sql_char_at
        self.jj.filters['sql_in_str'] = self.sql_in_str
        self.jj.filters['sql_in_str_set'] = self.sql_in_str_set
        self.jj.filters['sql_is_ascii'] = self.sql_is_ascii
        self.jj.filters['sql_unicode'] = self.sql_unicode


    @staticmethod
    def normalize(s):
        return DBMS._RE_NORMALIZE.sub(' ', s).strip()


    # Template Filters
    @staticmethod
    def sql_escape(s):
        if DBMS._RE_ESCAPE.match(s):
            return s
        assert ']' not in s, f'Cannot escape "{s}"'
        return f'[{s}]'

    @staticmethod
    def sql_hex_str(s):
        return f'x\'{s.encode("utf-8").hex()}\''

    @staticmethod
    def sql_hex_byte(n):
        assert n in range(BYTE_MAX + 1), f'n must be in [0, {BYTE_MAX}]'
        return f'x\'{n:02x}\''

    @staticmethod
    def sql_len(s):
        return f'length({s})'

    @staticmethod
    def sql_char_at(s, i):
        return f'substr({s}, {i + 1}, 1)'

    @staticmethod
    def sql_unicode(s):
        return f'unicode({s})'

    @staticmethod
    def sql_in_str(s, string):
        return f'instr({string}, {s})'

    @staticmethod
    def sql_in_str_set(s, strings):
        return f'{s} in ({",".join([DBMS.sql_hex_str(x) for x in strings])})'

    @staticmethod
    def sql_is_ascii(s):
        return f'is_ascii({s})'


    # Queries
    @abstractmethod
    def q_column_type_in_str_set(self, ctx, types):
        raise NotImplementedError()

    @abstractmethod
    def q_column_is_int(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_column_is_float(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_column_is_text(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_column_is_blob(self, ctx):
        raise NotImplementedError()

    def q_rows_have_null(self, ctx):
        query = self.jj.get_template('rows_have_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_null(self, ctx):
        query = self.jj.get_template('row_is_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_are_ascii(self, ctx):
        query = self.jj.get_template('rows_are_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_ascii(self, ctx):
        query = self.jj.get_template('row_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_char_is_ascii(self, ctx):
        query = self.jj.get_template('char_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_count_lt(self, ctx, n):
        query = self.jj.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_char_lt(self, ctx, n):
        query = self.jj.get_template('char_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_string_in_set(self, ctx, values):
        query = self.jj.get_template('string_in_set.jinja').render(ctx=ctx, values=values)
        return self.normalize(query)

    def q_int_lt(self, ctx, n):
        query = self.jj.get_template('int_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_float_char_in_set(self, ctx, values):
        return self.q_char_in_set(ctx, values)

    def q_byte_lt(self, ctx, n):
        query = self.jj.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)
