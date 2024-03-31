import os

import jinja2

from hakuin.utils import EOS, DIR_QUERIES, BYTE_MAX
from .DBMS import DBMS



class MySQL(DBMS):
    DATA_TYPES = [
        'integer', 'int', 'smallint', 'tinyint', 'mediumint', 'bigint', 'decimal',
        'numeric', 'float', 'double', 'bit', 'date', 'datetime', 'timestamp',
        'time', 'year', 'char', 'varchar', 'binary', 'varbinary', 'blob', 'text',
        'enum', 'set', 'geometry', 'point', 'linestring', 'polygon ', 'multipoint',
        'multilinestring', 'multipolygon', 'geometrycollection ', 'json'
    ]


    def __init__(self):
        super().__init__()
        self.jj_mysql = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(DIR_QUERIES, 'MySQL')))
        self.jj_mysql.filters = self.jj.filters


    # Template Filters
    @staticmethod
    def sql_escape(s):
        if s is None:
            return None

        if DBMS._RE_ESCAPE.match(s):
            return s

        assert '`' not in s, f'Cannot escape "{s}"'
        return f'`{s}`'

    @staticmethod
    def sql_str_lit(s):
        if not s.isascii() or not s.isprintable() or "'" in s:
            return f"x'{s.encode('utf-8').hex()}'"
        return f"'{s}'"

    @staticmethod
    def sql_len(s):
        return f'char_length({s})'

    @staticmethod
    def sql_to_unicode(s):
        return f'ord(convert({s} using utf32))'

    @staticmethod
    def sql_in_str(s, string):
        return f'locate({s}, BINARY {string})'

    @staticmethod
    def sql_in_str_set(s, strings):
        return f'{s} in (BINARY {",".join([DBMS.sql_str_lit(x) for x in strings])})'

    @staticmethod
    def sql_is_ascii(s):
        return f'({s} = convert({s} using ASCII))'


    # Queries
    def q_column_type_in_str_set(self, ctx, types):
        query = self.jj_mysql.get_template('column_type_in_str_set.jinja').render(ctx=ctx, types=types)
        return self.normalize(query)

    def q_column_is_int(self, ctx):
        query = self.jj_mysql.get_template('column_is_int.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_column_is_float(self, ctx):
        return self.q_column_type_in_str_set(ctx, types=['decimal', 'numeric', 'float', 'double'])

    def q_column_is_text(self, ctx):
        # *text* types are covered in the jinja template
        types = ['char', 'varchar', 'linestring', 'multilinestring', 'json']
        query = self.jj_mysql.get_template('column_is_text.jinja').render(ctx=ctx, types=types)
        return self.normalize(query)

    def q_column_is_blob(self, ctx):
        # *blob* types are covered in the jinja template
        types = ['binary', 'varbinary']
        query = self.jj_mysql.get_template('column_is_blob.jinja').render(ctx=ctx, types=types)
        return self.normalize(query)

    def q_rows_have_null(self, ctx):
        query = self.jj_mysql.get_template('rows_have_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_null(self, ctx):
        query = self.jj_mysql.get_template('row_is_null.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_are_ascii(self, ctx):
        query = self.jj_mysql.get_template('rows_are_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_row_is_ascii(self, ctx):
        query = self.jj_mysql.get_template('row_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_char_is_ascii(self, ctx):
        query = self.jj_mysql.get_template('char_is_ascii.jinja').render(ctx=ctx)
        return self.normalize(query)

    def q_rows_count_lt(self, ctx, n):
        query = self.jj_mysql.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_char_in_set(self, ctx, values):
        has_eos = EOS in values
        values = ''.join([v for v in values if v != EOS])
        query = self.jj_mysql.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
        return self.normalize(query)

    def q_char_lt(self, ctx, n):
        query = self.jj_mysql.get_template('char_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_string_in_set(self, ctx, values):
        query = self.jj_mysql.get_template('string_in_set.jinja').render(ctx=ctx, values=values)
        print(self.normalize(query))
        return self.normalize(query)

    def q_int_lt(self, ctx, n):
        query = self.jj_mysql.get_template('int_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)

    def q_float_char_in_set(self, ctx, values):
        return self.q_char_in_set(ctx, values)

    def q_byte_lt(self, ctx, n):
        query = self.jj_mysql.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
        return self.normalize(query)
