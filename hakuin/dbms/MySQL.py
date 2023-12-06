import os

import jinja2

from hakuin.utils import EOS, DIR_QUERIES
from .DBMS import DBMS



# class MySQLColumnQueries(ColumnQueries):
#     def column_data_type(self, ctx, values):
#         values = [f"'{v}'" for v in values]
#         query = f'''
#             SELECT  lower(DATA_TYPE) in ({','.join(values)})
#             FROM    information_schema.columns
#             WHERE   TABLE_SCHEMA=database() AND
#                     TABLE_NAME=x'{self.hex(ctx.table)}' AND
#                     COLUMN_NAME=x'{self.hex(ctx.column)}'
#         '''
#         return self.normalize(query)


#     def rows_count(self, ctx, n):
#         query = f'''
#             SELECT  count(*) < {n}
#             FROM    {MySQL.escape(ctx.table)}
#         '''
#         return self.normalize(query)


#     def rows_have_null(self, ctx):
#         query = f'''
#             SELECT  count(*)
#             FROM    {MySQL.escape(ctx.table)}
#             WHERE   {MySQL.escape(ctx.column)} is NULL
#         '''
#         return self.normalize(query)


#     def row_is_null(self, ctx):
#         query = f'''
#             SELECT  {MySQL.escape(ctx.column)} is NULL
#             FROM    {MySQL.escape(ctx.table)}
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)



# class MySQLTextQueries(TextQueries, MySQLColumnQueries):
#     def rows_are_ascii(self, ctx):
#         query = f'''
#             SELECT  min({MySQL.escape(ctx.column)} = convert({MySQL.escape(ctx.column)} using ASCII))
#             FROM    {MySQL.escape(ctx.table)}
#         '''
#         return self.normalize(query)


#     def row_is_ascii(self, ctx):
#         query = f'''
#             SELECT  min({MySQL.escape(ctx.column)} = convert({MySQL.escape(ctx.column)} using ASCII))
#             FROM    {MySQL.escape(ctx.table)}
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def char_is_ascii(self, ctx):
#         query = f'''
#             SELECT  ord(convert(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1) using utf32)) < {ASCII_MAX + 1}
#             FROM    {MySQL.escape(ctx.table)}
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def char(self, ctx, values):
#         has_eos = EOS in values
#         values = [v for v in values if v != EOS]
#         values = ''.join(values).encode('utf-8').hex()

#         if has_eos:
#             query = f'''
#                 SELECT  locate(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1), x'{values}')
#                 FROM    {MySQL.escape(ctx.table)}
#                 LIMIT   1
#                 OFFSET  {ctx.row_idx}
#             '''
#         else:
#             query = f'''
#                 SELECT  char_length({MySQL.escape(ctx.column)}) != {len(ctx.s)} AND
#                         locate(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1), x'{values}')
#                 FROM    {MySQL.escape(ctx.table)}
#                 LIMIT   1
#                 OFFSET  {ctx.row_idx}
#             '''
#         return self.normalize(query)


#     def char_unicode(self, ctx, n):
#         query = f'''
#             SELECT  ord(convert(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1) using utf32)) < {n}
#             FROM    {MySQL.escape(ctx.table)}
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def string(self, ctx, values):
#         values = [f"x'{v.encode('utf-8').hex()}'" for v in values]
#         query = f'''
#             SELECT  {MySQL.escape(ctx.column)} in ({','.join(values)})
#             FROM    {MySQL.escape(ctx.table)}
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)



# class MySQLTableNamesQueries(TextQueries, MySQLColumnQueries):
#     def rows_count(self, ctx, n):
#         query = f'''
#             SELECT  count(*) < {n}
#             FROM    information_schema.TABLES
#             WHERE   TABLE_SCHEMA=database()
#         '''
#         return self.normalize(query)


#     def rows_are_ascii(self, ctx):
#         # min() simulates the logical ALL operator here
#         query = f'''
#             SELECT  min(TABLE_NAME = convert(TABLE_NAME using ASCII))
#             FROM    information_schema.TABLES
#             WHERE   TABLE_SCHEMA=database()
#         '''
#         return self.normalize(query)


#     def row_is_ascii(self, ctx):
#         query = f'''
#             SELECT  TABLE_NAME = convert(TABLE_NAME using ASCII)
#             FROM    information_schema.TABLES
#             WHERE   TABLE_SCHEMA=database()
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)
        

#     def char_is_ascii(self, ctx):
#         query = f'''
#             SELECT  ord(convert(substr(TABLE_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {ASCII_MAX + 1}
#             FROM    information_schema.TABLES
#             WHERE   TABLE_SCHEMA=database()
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def char(self, ctx, values):
#         has_eos = EOS in values
#         values = [v for v in values if v != EOS]
#         values = ''.join(values).encode('utf-8').hex()

#         if has_eos:
#             query = f'''
#                 SELECT  locate(substr(TABLE_NAME, {len(ctx.s) + 1}, 1), x'{values}')
#                 FROM    information_schema.TABLES
#                 WHERE   TABLE_SCHEMA=database()
#                 LIMIT   1
#                 OFFSET  {ctx.row_idx}
#             '''
#         else:
#             query = f'''
#                 SELECT  char_length(TABLE_NAME) != {len(ctx.s)} AND
#                         locate(substr(TABLE_NAME, {len(ctx.s) + 1}, 1), x'{values}')
#                 FROM    information_schema.TABLES
#                 WHERE   TABLE_SCHEMA=database()
#                 LIMIT   1
#                 OFFSET  {ctx.row_idx}
#             '''
#         return self.normalize(query)


#     def char_unicode(self, ctx, n):
#         query = f'''
#             SELECT  ord(convert(substr(TABLE_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {n}
#             FROM    information_schema.TABLES
#             WHERE   TABLE_SCHEMA=database()
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def string(self, ctx):
#         raise NotImplementedError('TODO?')



# class MySQLColumnNamesQueries(TextQueries, MySQLColumnQueries):
#     def rows_count(self, ctx, n):
#         query = f'''
#             SELECT  count(*) < {n}
#             FROM    information_schema.COLUMNS
#             WHERE   TABLE_SCHEMA=database() AND
#                     TABLE_NAME=x'{self.hex(ctx.table)}'
#         '''
#         return self.normalize(query)


#     def rows_are_ascii(self, ctx):
#         query = f'''
#             SELECT  min(COLUMN_NAME = convert(COLUMN_NAME using ASCII))
#             FROM    information_schema.COLUMNS
#             WHERE   TABLE_SCHEMA=database() AND
#                     TABLE_NAME=x'{self.hex(ctx.table)}'
#         '''
#         return self.normalize(query)


#     def row_is_ascii(self, ctx):
#         query = f'''
#             SELECT  min(COLUMN_NAME = convert(COLUMN_NAME using ASCII))
#             FROM    information_schema.COLUMNS
#             WHERE   TABLE_SCHEMA=database() AND
#                     TABLE_NAME=x'{self.hex(ctx.table)}'
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def char_is_ascii(self, ctx):
#         query = f'''
#             SELECT  ord(convert(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {ASCII_MAX + 1}
#             FROM    information_schema.COLUMNS
#             WHERE   TABLE_SCHEMA=database() AND
#                     TABLE_NAME=x'{self.hex(ctx.table)}'
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def char(self, ctx, values):
#         has_eos = EOS in values
#         values = [v for v in values if v != EOS]
#         values = ''.join(values).encode('utf-8').hex()
        
#         if has_eos:
#             query = f'''
#                 SELECT  locate(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1), x'{values}')
#                 FROM    information_schema.COLUMNS
#                 WHERE   TABLE_SCHEMA=database() AND
#                         TABLE_NAME=x'{self.hex(ctx.table)}'
#                 LIMIT   1
#                 OFFSET  {ctx.row_idx}
#             '''
#         else:
#             query = f'''
#                 SELECT  char_length(COLUMN_NAME) != {len(ctx.s)} AND
#                         locate(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1), x'{values}')
#                 FROM    information_schema.COLUMNS
#                 WHERE   TABLE_SCHEMA=database() AND
#                         TABLE_NAME=x'{self.hex(ctx.table)}'
#                 LIMIT   1
#                 OFFSET  {ctx.row_idx}
#             '''
#         return self.normalize(query)


#     def char_unicode(self, ctx, n):
#         query = f'''
#             SELECT  ord(convert(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {n}
#             FROM    information_schema.COLUMNS
#             WHERE   TABLE_SCHEMA=database() AND
#                     TABLE_NAME=x'{self.hex(ctx.table)}'
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)


#     def string(self, ctx):
#         raise NotImplementedError('TODO?')



# class MySQLIntQueries(MySQLColumnQueries):
#     def int(self, ctx, n):
#         query = f'''
#             SELECT  {MySQL.escape(ctx.column)} < {n}
#             FROM    {MySQL.escape(ctx.table)}
#             LIMIT   1
#             OFFSET  {ctx.row_idx}
#         '''
#         return self.normalize(query)



class MySQL(DBMS):
    # single char ascii. Check if the query is not tool long. TODO
    #             SELECT  ord(convert(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1) using utf32)) < {ASCII_MAX + 1}
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
        self.jj_mysql.filters['sql_escape'] = self.sql_escape
        self.jj_mysql.filters['sql_is_ascii'] = self.sql_is_ascii


    @staticmethod
    def sql_escape(s):
        if DBMS._RE_ESCAPE.match(s):
            return s
        assert '`' not in s, f'Cannot escape "{s}"'
        return f'`{s}`'


    # Template Filters
    @staticmethod
    def sql_len(s):
        return f'char_length({s})'

    @staticmethod
    def sql_unicode(s):
        return f'ord(convert({s} using utf32))'

    @staticmethod
    def sql_in_str(s, string):
        return f'locate({s}, {DBMS.sql_hex_lit(string)})'

    @staticmethod
    def sql_is_ascii(s):
        return f'{s} = convert({s} using ASCII)'


    # Queries
    def q_column_data_type(self, ctx, types):
        query = self.jj_mysql.get_template('column_data_type.jinja').render(ctx=ctx, types=types)
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