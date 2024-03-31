import os

import jinja2

from hakuin.utils import EOS, DIR_QUERIES, BYTE_MAX
from .DBMS import DBMS



class PSQL(DBMS):
    DATA_TYPES = ['TODO']


    def __init__(self):
        super().__init__()
        self.jj_psql = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(DIR_QUERIES, 'PSQL')))
        self.jj_psql.filters = self.jj.filters


    # Template Filters
    @staticmethod
    def sql_escape(s):
        if s is None:
            return None

        if DBMS._RE_ESCAPE.match(s):
            return s

        assert '"' not in s, f'Cannot escape "{s}"'
        return f'"{s}"'

    @staticmethod
    def sql_str_lit(s):
        if not s.isascii() or not s.isprintable() or "'" in s:
            return f"convert_from('\\x{s.encode('utf-8').hex()}', 'UTF8')"
        return f"'{s}'"

    @staticmethod
    def sql_byte_lit(n):
        assert n in range(BYTE_MAX + 1), f'n must be in [0, {BYTE_MAX}]'
        return f"'\\x{n:02x}'"

    @staticmethod
    def sql_to_unicode(s):
        return f'ascii({s})'

    @staticmethod
    def sql_in_str(s, string):
        return f'position({s} in {string})::bool'

    @staticmethod
    def sql_is_ascii(s):
        return f"({s} ~ '^[[:ascii:]]*$')"



    # Queries
    def q_column_type_in_str_set(self, ctx, types):
        query = self.jj_psql.get_template('column_type_in_str_set.jinja').render(ctx=ctx, types=types)
        return self.normalize(query)

    def q_column_is_int(self, ctx):
        types = ['integer', 'int', 'smallint', 'bigint', 'serial', 'smallserial', 'bigserial']
        return self.q_column_type_in_str_set(ctx, types=types)

    def q_column_is_float(self, ctx):
        types = ['decimal', 'numeric', 'real', 'float', 'double precision']
        return self.q_column_type_in_str_set(ctx, types=types)

    def q_column_is_text(self, ctx):
        types = ['character varying', 'varchar', 'character', 'char', 'bpchar', 'text']
        return self.q_column_type_in_str_set(ctx, types=types)

    def q_column_is_blob(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['bytea'])

    def q_rows_have_null(self, ctx):
        query = self.jj_psql.get_template('rows_have_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_null(self, ctx):
        query = self.jj_psql.get_template('row_is_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_are_ascii(self, ctx):
        query = self.jj_psql.get_template('rows_are_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_ascii(self, ctx):
        query = self.jj_psql.get_template('row_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_char_is_ascii(self, ctx):
        query = self.jj_psql.get_template('char_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_count_lt(self, ctx, n):
        query = self.jj_psql.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_psql.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_char_lt(self, ctx, n):
        query = self.jj_psql.get_template('char_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_string_in_set(self, ctx, values):
        query = self.jj_psql.get_template('string_in_set.jinja').render(ctx=ctx, values=values)
        return self.normalize(query)

    def q_int_lt(self, ctx, n):
        query = self.jj_psql.get_template('int_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_float_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_psql.get_template('float_char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_byte_lt(self, ctx, n):
        query = self.jj_psql.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)