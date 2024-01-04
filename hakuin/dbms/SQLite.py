import os

import jinja2

from hakuin.utils import EOS, DIR_QUERIES, BYTE_MAX
from .DBMS import DBMS



class SQLite(DBMS):
    DATA_TYPES = ['integer', 'text', 'real', 'numeric', 'blob']


    def __init__(self):
        super().__init__()
        self.jj_sqlite = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(DIR_QUERIES, 'SQLite')))
        self.jj_sqlite.filters = self.jj.filters


    # Template Filters
    @staticmethod
    def sql_str_lit(s):
        if not s.isascii() or not s.isprintable() or "'" in s:
            return f"cast(x'{s.encode('utf-8').hex()}' as TEXT)"
        return f"'{s}'"

    @staticmethod
    def sql_byte_lit(n):
        assert n in range(BYTE_MAX + 1), f'n must be in [0, {BYTE_MAX}]'
        return f"x'{n:02x}'"

    @staticmethod
    def sql_in_str_set(s, strings):
        return f'{s} in ({",".join([SQLite.sql_str_lit(x) for x in strings])})'

    @staticmethod
    def sql_is_ascii(s):
        # SQLite does not have native "isascii" function. As a workaround we try to look for
        # non-ascii characters with "*[^\x01-0x7f]*" glob patterns. The pattern does not need to
        # include the null terminator (0x00) because SQLite will never pass it to the GLOB expression.
        # Also, the pattern is hex-encoded because SQLite does not support special characters in
        # string literals.
        return f'{s} not glob cast(x\'2a5b5e012d7f5d2a\' as TEXT)'


    # Queries
    def q_column_type_in_str_set(self, ctx, types):
        query = self.jj_sqlite.get_template('column_type_in_str_set.jinja').render(ctx=ctx, types=types)
        return self.normalize(query)

    def q_column_is_int(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['integer'])

    def q_column_is_float(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['real'])

    def q_column_is_text(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['text'])

    def q_column_is_blob(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['blob'])

    def q_rows_have_null(self, ctx):
        query = self.jj_sqlite.get_template('rows_have_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_null(self, ctx):
        query = self.jj_sqlite.get_template('row_is_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_are_ascii(self, ctx):
        query = self.jj_sqlite.get_template('rows_are_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_ascii(self, ctx):
        query = self.jj_sqlite.get_template('row_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_char_is_ascii(self, ctx):
        query = self.jj_sqlite.get_template('char_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_count_lt(self, ctx, n):
        query = self.jj_sqlite.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_sqlite.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_char_lt(self, ctx, n):
        query = self.jj_sqlite.get_template('char_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_string_in_set(self, ctx, values):
        query = self.jj_sqlite.get_template('string_in_set.jinja').render(ctx=ctx, values=values)
        return self.normalize(query)

    def q_int_lt(self, ctx, n):
        query = self.jj_sqlite.get_template('int_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_float_char_in_set(self, ctx, values):
        return self.q_char_in_set(ctx, values)

    def q_byte_lt(self, ctx, n):
        query = self.jj_sqlite.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)