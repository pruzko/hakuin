from hakuin.utils import EOS, ASCII_MAX

from .DBMS import DBMS, MetaQueries, UniformQueries



class MySQLMetaQueries(MetaQueries):
    def column_data_type(self, ctx, values):
        values = [f"'{v}'" for v in values]
        query = f'''
            SELECT  lower(DATA_TYPE) in ({','.join(values)})
            FROM    information_schema.columns
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}' AND
                    COLUMN_NAME=x'{self.hex(ctx.column)}'
        '''
        return self.normalize(query)


    def column_is_nullable(self, ctx):
        query = f'''
            SELECT  IS_NULLABLE='YES'
            FROM    information_schema.columns
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}' AND
                    COLUMN_NAME=x'{self.hex(ctx.column)}'
        '''
        return self.normalize(query)


    def column_is_pk(self, ctx):
        query = f'''
            SELECT  COLUMN_KEY='PRI'
            FROM    information_schema.columns
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}' AND
                    COLUMN_NAME=x'{self.hex(ctx.column)}'
        '''
        return self.normalize(query)



class MySQLTablesQueries(UniformQueries):
    def rows_count(self, ctx, n):
        query = f'''
            SELECT  count(*) < {n}
            FROM    information_schema.TABLES
            WHERE   TABLE_SCHEMA=database()
        '''
        return self.normalize(query)


    def rows_are_ascii(self, ctx):
        # min() simulates the logical ALL operator here
        query = f'''
            SELECT  min(TABLE_NAME = CONVERT(TABLE_NAME using ASCII))
            FROM    information_schema.TABLES
            WHERE   TABLE_SCHEMA=database()
        '''
        return self.normalize(query)


    def row_is_ascii(self, ctx):
        query = f'''
            SELECT  TABLE_NAME = CONVERT(TABLE_NAME using ASCII)
            FROM    information_schema.TABLES
            WHERE   TABLE_SCHEMA=database()
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)
        

    def char_is_ascii(self, ctx):
        query = f'''
            SELECT  ord(convert(substr(TABLE_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {ASCII_MAX + 1}
            FROM    information_schema.TABLES
            WHERE   TABLE_SCHEMA=database()
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char(self, ctx, values):
        has_eos = EOS in values
        values = [v for v in values if v != EOS]
        values = ''.join(values).encode('utf-8').hex()

        if has_eos:
            query = f'''
                SELECT  locate(substr(TABLE_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.TABLES
                WHERE   TABLE_SCHEMA=database()
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  char_length(TABLE_NAME) != {len(ctx.s)} AND
                        locate(substr(TABLE_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.TABLES
                WHERE   TABLE_SCHEMA=database()
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_unicode(self, ctx, n):
        query = f'''
            SELECT  ord(convert(substr(TABLE_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {n}
            FROM    information_schema.TABLES
            WHERE   TABLE_SCHEMA=database()
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def string(self, ctx):
        raise NotImplementedError('TODO?')



class MySQLColumnsQueries(UniformQueries):
    def rows_count(self, ctx, n):
        query = f'''
            SELECT  count(*) < {n}
            FROM    information_schema.COLUMNS
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}'
        '''
        return self.normalize(query)


    def rows_are_ascii(self, ctx):
        query = f'''
            SELECT  min(COLUMN_NAME = CONVERT(COLUMN_NAME using ASCII))
            FROM    information_schema.COLUMNS
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}'
        '''
        return self.normalize(query)


    def row_is_ascii(self, ctx):
        query = f'''
            SELECT  min(COLUMN_NAME = CONVERT(COLUMN_NAME using ASCII))
            FROM    information_schema.COLUMNS
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}'
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char_is_ascii(self, ctx):
        query = f'''
            SELECT  ord(convert(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {ASCII_MAX + 1}
            FROM    information_schema.COLUMNS
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}'
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char(self, ctx, values):
        has_eos = EOS in values
        values = [v for v in values if v != EOS]
        values = ''.join(values).encode('utf-8').hex()
        
        if has_eos:
            query = f'''
                SELECT  locate(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.COLUMNS
                WHERE   TABLE_SCHEMA=database() AND
                        TABLE_NAME=x'{self.hex(ctx.table)}'
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  char_length(COLUMN_NAME) != {len(ctx.s)} AND
                        locate(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.COLUMNS
                WHERE   TABLE_SCHEMA=database() AND
                        TABLE_NAME=x'{self.hex(ctx.table)}'
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_unicode(self, ctx, n):
        query = f'''
            SELECT  ord(convert(substr(COLUMN_NAME, {len(ctx.s) + 1}, 1) using utf32)) < {n}
            FROM    information_schema.COLUMNS
            WHERE   TABLE_SCHEMA=database() AND
                    TABLE_NAME=x'{self.hex(ctx.table)}'
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def string(self, ctx):
        raise NotImplementedError('TODO?')



class MySQLRowsQueries(UniformQueries):
    def rows_count(self, ctx, n):
        query = f'''
            SELECT  count(*) < {n}
            FROM    {MySQL.escape(ctx.table)}
        '''
        return self.normalize(query)


    def rows_are_ascii(self, ctx):
        query = f'''
            SELECT  min({MySQL.escape(ctx.column)} = CONVERT({MySQL.escape(ctx.column)} using ASCII))
            FROM    {MySQL.escape(ctx.table)}
        '''
        return self.normalize(query)


    def row_is_ascii(self, ctx):
        query = f'''
            SELECT  min({MySQL.escape(ctx.column)} = CONVERT({MySQL.escape(ctx.column)} using ASCII))
            FROM    {MySQL.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char_is_ascii(self, ctx):
        query = f'''
            SELECT  ord(convert(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1) using utf32)) < {ASCII_MAX + 1}
            FROM    {MySQL.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char(self, ctx, values):
        has_eos = EOS in values
        values = [v for v in values if v != EOS]
        values = ''.join(values).encode('utf-8').hex()

        if has_eos:
            query = f'''
                SELECT  locate(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    {MySQL.escape(ctx.table)}
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  char_length({MySQL.escape(ctx.column)}) != {len(ctx.s)} AND
                        locate(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    {MySQL.escape(ctx.table)}
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_unicode(self, ctx, n):
        query = f'''
            SELECT  ord(convert(substr({MySQL.escape(ctx.column)}, {len(ctx.s) + 1}, 1) using utf32)) < {n}
            FROM    {MySQL.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def string(self, ctx, values):
        values = [f"x'{v.encode('utf-8').hex()}'" for v in values]
        query = f'''
            SELECT  {MySQL.escape(ctx.column)} in ({','.join(values)})
            FROM    {MySQL.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def int(self, ctx, n):
        query = f'''
            SELECT  {MySQL.escape(ctx.column)} < {n}
            FROM    {MySQL.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)



class MySQL(DBMS):
    DATA_TYPES = [
        'integer', 'smallint', 'tinyint', 'mediumint', 'bigint', 'decimal',
        'numeric', 'float', 'double', 'bit', 'date', 'datetime', 'timestamp',
        'time', 'year', 'char', 'varchar', 'binary', 'varbinary', 'blob', 'text',
        'enum', 'set', 'geometry', 'point', 'linestring', 'polygon ', 'multipoint',
        'multilinestring', 'multipolygon', 'geometrycollection ', 'json'
    ]

    MetaQueries = MySQLMetaQueries()
    TablesQueries = MySQLTablesQueries()
    ColumnsQueries = MySQLColumnsQueries()
    RowsQueries = MySQLRowsQueries()


    @staticmethod
    def escape(s):
        if DBMS._RE_ESCAPE.match(s):
            return s
        assert '`' not in s, f'Cannot escape "{s}"'
        return f'`{s}`'
