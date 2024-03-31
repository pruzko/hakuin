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
        self.jj = jinja2.Environment()
        self.jj.filters['sql_escape'] = self.sql_escape
        self.jj.filters['sql_str_lit'] = self.sql_str_lit
        self.jj.filters['sql_byte_lit'] = self.sql_byte_lit
        self.jj.filters['sql_len'] = self.sql_len
        self.jj.filters['sql_char_at'] = self.sql_char_at
        self.jj.filters['sql_in_str'] = self.sql_in_str
        self.jj.filters['sql_in_str_set'] = self.sql_in_str_set
        self.jj.filters['sql_is_ascii'] = self.sql_is_ascii
        self.jj.filters['sql_to_unicode'] = self.sql_to_unicode


    @staticmethod
    def normalize(s):
        return DBMS._RE_NORMALIZE.sub(' ', s).strip()


    # Template Filters
    @staticmethod
    def sql_escape(s):
        if s is None:
            return None

        if DBMS._RE_ESCAPE.match(s):
            return s

        assert ']' not in s, f'Cannot escape "{s}"'
        return f'[{s}]'

    @staticmethod
    def sql_str_lit(s):
        if not s.isascii() or not s.isprintable() or "'" in s:
            return f"x'{s.encode('utf-8').hex()}'"
        return f"'{s}'"

    @staticmethod
    def sql_byte_lit(n):
        assert n in range(BYTE_MAX + 1), f'n must be in [0, {BYTE_MAX}]'
        return f"0x{n:02x}"

    @staticmethod
    def sql_len(s):
        return f'length({s})'

    @staticmethod
    def sql_char_at(s, i):
        return f'substr({s}, {i + 1}, 1)'

    @staticmethod
    def sql_to_unicode(s):
        return f'unicode({s})'

    @staticmethod
    def sql_in_str(s, string):
        return f'instr({string}, {s})'

    @staticmethod
    def sql_in_str_set(s, strings):
        return f'{s} in ({",".join([DBMS.sql_str_lit(x) for x in strings])})'

    @staticmethod
    def sql_is_ascii(s):
        return f'is_ascii({s})'


    # Queries
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

    @abstractmethod
    def q_rows_have_null(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_row_is_null(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_rows_are_ascii(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_row_is_ascii(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_char_is_ascii(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def q_rows_count_lt(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def q_char_in_set(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def q_char_lt(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def q_string_in_set(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def q_int_lt(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def q_float_char_in_set(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def q_byte_lt(self, ctx, n):
        raise NotImplementedError()
