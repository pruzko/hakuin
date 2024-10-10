import os

import jinja2

from hakuin.utils import DIR_QUERY_TEMPLATES, EOS
from .DBMS import DBMS



class MSSQL(DBMS):
    def __init__(self):
        super().__init__()
        self.jj_mssql = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(DIR_QUERY_TEMPLATES, 'MSSQL')))
        self.jj_mssql.filters = self.jj.filters
        self.jj_mssql.filters['sql_to_text'] = self.sql_to_text


    # Template Filters
    @staticmethod
    def sql_cast(s, type):
        allowed_types = ['int', 'float', 'text', 'blob']
        assert type in allowed_types, f'Type "{type}" not allowed. Use one of {allowed_types}'

        translate = {
            'int': 'int',
            'float': 'float',
            'text': 'nvarchar(max)',
            'blob': 'varbinary',
        }
        return f'cast({s} as {translate[type]})'

    @staticmethod
    def sql_lit(s):
        if not s.isascii() or not s.isprintable() or any(c in s for c in "?:'"):
            hex_str = s.encode('utf-16').hex()
            return f'convert(nvarchar(MAX), 0x{hex_str})'
        return f"'{s}'"

    @staticmethod
    def sql_len(s):
        return f'len({s})'

    @staticmethod
    def sql_char_at(s, i):
        return f'substring({s}, {i + 1}, 1)'

    @staticmethod
    def sql_in_str(s, string):
        return f'charindex({s},{string} COLLATE Latin1_General_CS_AS)'

    @classmethod
    def sql_in_list(cls, s, values):
        if str not in [type(v) for v in values]:
            return f'{s} in ({",".join(values)})'

        values = [cls.sql_lit(v) if type(v) is str else str(v) for v in values]
        return f'{s} COLLATE Latin1_General_CS_AS in ({",".join(values)})'


    @staticmethod
    def sql_is_ascii(s):
        # MSSQL does not have native "isascii" function. As a workaround we try to look for
        # non-ascii characters with "%[^\x00-0x7f]%" patterns.
        return f"CASE WHEN patindex('%[^'+char(0x00)+'-'+char(0x7f)+']%' COLLATE Latin1_General_BIN,{s}) = 0 THEN 1 ELSE 0 END"

    @staticmethod
    def sql_to_text(s):
        return f'convert(nvarchar(MAX), {s})'


    # Queries
    def q_column_type_in_str_set(self, ctx, types):
        query = self.jj_mssql.get_template('column_type_in_str_set.jinja').render(ctx=ctx, types=types)
        return self.normalize(query)

    def q_column_is_int(self, ctx):
        types = ['int', 'bigint', 'smallint', 'bit']
        return self.q_column_type_in_str_set(ctx, types=types)

    def q_column_is_float(self, ctx):
        types = ['float', 'real', 'decimal', 'dec', 'numeric', 'money', 'smallmoney']
        return self.q_column_type_in_str_set(ctx, types=types)

    def q_column_is_text(self, ctx):
        types = ['char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext']
        return self.q_column_type_in_str_set(ctx, types=types)

    def q_column_is_blob(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['binary', 'varbinary', 'image'])

    def q_rows_have_null(self, ctx):
        query = self.jj_mssql.get_template('rows_have_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_null(self, ctx):
        query = self.jj_mssql.get_template('row_is_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_are_positive(self, ctx):
        query = self.jj_mssql.get_template('rows_are_positive.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_are_ascii(self, ctx):
        query = self.jj_mssql.get_template('rows_are_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_ascii(self, ctx):
        query = self.jj_mssql.get_template('row_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_char_is_ascii(self, ctx):
        query = self.jj_mssql.get_template('char_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_count_lt(self, ctx, n):
        query = self.jj_mssql.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_mssql.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_char_lt(self, ctx, n):
        query = self.jj_mssql.get_template('char_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_value_in_list(self, ctx, values):
        query = self.jj_mssql.get_template('value_in_list.jinja').render(ctx=ctx, values=values)
        return self.normalize(query)

    def q_int_lt(self, ctx, n):
        query = self.jj_mssql.get_template('int_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_int_eq(self, ctx, n):
        query = self.jj_mssql.get_template('int_eq.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_float_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_mssql.get_template('float_char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_byte_lt(self, ctx, n):
        query = self.jj_mssql.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)