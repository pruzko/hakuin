import os

import jinja2

from hakuin.utils import EOS, DIR_QUERIES
from .DBMS import DBMS



class OracleDB(DBMS):
    DATA_TYPES = [
        'char', 'nchar', 'varchar2', 'nvarchar2', 'clob', 'nclob', 'number', 'float',
        'binary_float', 'binary_double', 'blob', 'bfile'
    ]
    def __init__(self):
        super().__init__()
        self.jj_oracledb = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(DIR_QUERIES, 'OracleDB')))
        self.jj_oracledb.filters = self.jj.filters
        self.jj_oracledb.filters['sql_to_text'] = self.sql_to_text
        self.jj_oracledb.filters['sql_byte_at'] = self.sql_byte_at


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
        if not s.isascii() or not s.isprintable() or any(c in s for c in "?:'"):
            hex_str = ''.join([f'\\{ord(c):04x}' for c in s])
            return f"unistr('{hex_str}')"
        return f"'{s}'"

    @staticmethod
    def sql_to_unicode(s):
        return f'ascii(unistr({s}))'

    @classmethod
    def sql_in_str_set(cls, s, strings):
        return f'{s} in ({",".join([cls.sql_str_lit(x) for x in strings])})'

    @staticmethod
    def sql_is_ascii(s):
        return f"nvl(regexp_like({s}, '^['||chr(1)||'-'||chr(127)||']*$'),1)"

    @staticmethod
    def sql_to_text(s):
        return f'to_char({s})'

    @staticmethod
    def sql_byte_at(s, i):
        return f"to_number(substr(cast({s} as raw(2000)),{i * 2 + 1},2), 'XX')"


    # # Queries
    def q_column_type_in_str_set(self, ctx, types):
        query = self.jj_oracledb.get_template('column_type_in_str_set.jinja').render(ctx=ctx, types=types)
        return self.normalize(query)

    def q_column_is_int(self, ctx):
        query = self.jj_oracledb.get_template('column_type_is_int.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_column_is_float(self, ctx):
        query = self.jj_oracledb.get_template('column_type_is_float.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_column_is_text(self, ctx):
        types = ['char', 'nchar', 'varchar2', 'nvarchar2', 'clob', 'nclob']
        return self.q_column_type_in_str_set(ctx, types=types)

    def q_column_is_blob(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['blob'])

    def q_rows_have_null(self, ctx):
        query = self.jj_oracledb.get_template('rows_have_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_null(self, ctx):
        query = self.jj_oracledb.get_template('row_is_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_are_ascii(self, ctx):
        query = self.jj_oracledb.get_template('rows_are_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_ascii(self, ctx):
        query = self.jj_oracledb.get_template('row_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_char_is_ascii(self, ctx):
        query = self.jj_oracledb.get_template('char_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_count_lt(self, ctx, n):
        query = self.jj_oracledb.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_oracledb.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_char_lt(self, ctx, n):
        query = self.jj_oracledb.get_template('char_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_string_in_set(self, ctx, values):
        query = self.jj_oracledb.get_template('string_in_set.jinja').render(ctx=ctx, values=values)
        return self.normalize(query)

    def q_int_lt(self, ctx, n):
        query = self.jj_oracledb.get_template('int_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_float_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_oracledb.get_template('float_char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_byte_lt(self, ctx, n):
        query = self.jj_oracledb.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)