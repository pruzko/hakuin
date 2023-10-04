from hakuin.utils import EOS

from .DBMS import DBMS, MetaQueries, UniformQueries



class SQLiteMetaQueries(MetaQueries):
    def column_data_type(self, ctx, values):
        values = [f"'{v}'" for v in values]
        query = f'''
            SELECT  type in ({','.join(values)})
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
            WHERE   name=x'{self.hex(ctx.column)}'
        '''
        return self.normalize(query)


    def column_is_nullable(self, ctx):
        query = f'''
            SELECT  [notnull] == 0
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
            WHERE   name=x'{self.hex(ctx.column)}'
        '''
        return self.normalize(query)


    def column_is_pk(self, ctx):
        query = f'''
            SELECT  pk
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
            WHERE   name=x'{self.hex(ctx.column)}'
        '''
        return self.normalize(query)



class SQLiteTablesQueries(UniformQueries):
    def rows_count(self, ctx, n):
        query = f'''
            SELECT  count(*) < {n}
            FROM    sqlite_master
            WHERE   type='table'
        '''
        return self.normalize(query)


    def rows_are_ascii(self, ctx):
        # SQLite does not have native "isascii" function. As a workaround we try to look for
        # non-ascii characters with "*[^\x01-0x7f]*" glob patterns. The pattern does not need to
        # include the null terminator (0x00) because SQLite will never pass it to the GLOB expression.
        # Also, the pattern is hex-encoded because SQLite does not support special characters in
        # string literals. Lastly, sum() simulates the logical ANY operator here. Note that an empty string
        # resolves to True, which is correct.
        query = f'''
            SELECT  sum(name not glob cast(x'2a5b5e012d7f5d2a' as TEXT))
            FROM    sqlite_master
            WHERE   type='table'
        '''
        return self.normalize(query)


    def row_is_ascii(self, ctx):
        query = f'''
            SELECT  name not glob cast(x'2a5b5e012d7f5d2a' as TEXT)
            FROM    sqlite_master
            WHERE   type='table'
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char_is_ascii(self, ctx):
        query = f'''
            SELECT  substr(name, {len(ctx.s) + 1}, 1) not glob cast(x'2a5b5e012d7f5d2a' as TEXT)
            FROM    sqlite_master
            WHERE   type='table'
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
                SELECT  instr(x'{values}', substr(name, {len(ctx.s) + 1}, 1))
                FROM    sqlite_master
                WHERE   type='table'
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  length(name) != {len(ctx.s)} AND
                        instr(x'{values}', substr(name, {len(ctx.s) + 1}, 1))
                FROM    sqlite_master
                WHERE   type='table'
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_unicode(self, ctx, n):
        query = f'''
            SELECT  unicode(substr(name, {len(ctx.s) + 1}, 1)) < {n}
            FROM    sqlite_master
            WHERE   type='table'
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def string(self, ctx):
        raise NotImplementedError('TODO?')



class SQLiteColumnsQueries(UniformQueries):
    def rows_count(self, ctx, n):
        query = f'''
            SELECT  count(*) < {n}
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
        '''
        return self.normalize(query)


    def rows_are_ascii(self, ctx):
        query = f'''
            SELECT  sum(name not glob cast(x'2a5b5e012d7f5d2a' as TEXT))
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
        '''
        return self.normalize(query)


    def row_is_ascii(self, ctx):
        query = f'''
            SELECT  name not glob cast(x'2a5b5e012d7f5d2a' as TEXT)
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char_is_ascii(self, ctx):
        query = f'''
            SELECT  substr(name, {len(ctx.s) + 1}, 1) not glob cast(x'2a5b5e012d7f5d2a' as TEXT)
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
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
                SELECT  instr(x'{values}', substr(name, {len(ctx.s) + 1}, 1))
                FROM    pragma_table_info(x'{self.hex(ctx.table)}')
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  length(name) != {len(ctx.s)} AND
                        instr(x'{values}', substr(name, {len(ctx.s) + 1}, 1))
                FROM    pragma_table_info(x'{self.hex(ctx.table)}')
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_unicode(self, ctx, n):
        query = f'''
            SELECT  unicode(substr(name, {len(ctx.s) + 1}, 1)) < {n}
            FROM    pragma_table_info(x'{self.hex(ctx.table)}')
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def string(self, ctx):
        raise NotImplementedError('TODO?')



class SQLiteRowsQueries(UniformQueries):
    def rows_count(self, ctx, n):
        query = f'''
            SELECT  count(*) < {n}
            FROM    {SQLite.escape(ctx.table)}
        '''
        return self.normalize(query)


    def rows_are_ascii(self, ctx):
        query = f'''
            SELECT  sum({SQLite.escape(ctx.column)} not glob cast(x'2a5b5e012d7f5d2a' as TEXT))
            FROM    {SQLite.escape(ctx.table)}
        '''
        return self.normalize(query)


    def row_is_ascii(self, ctx):
        query = f'''
            SELECT  {SQLite.escape(ctx.column)} not glob cast(x'2a5b5e012d7f5d2a' as TEXT)
            FROM    {SQLite.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def char_is_ascii(self, ctx):
        query = f'''
            SELECT  substr({SQLite.escape(ctx.column)}, {len(ctx.s) + 1}, 1) not glob cast(x'2a5b5e012d7f5d2a' as TEXT)
            FROM    {SQLite.escape(ctx.table)}
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
                SELECT  instr(x'{values}', substr({SQLite.escape(ctx.column)}, {len(ctx.s) + 1}, 1))
                FROM    {SQLite.escape(ctx.table)}
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  length({SQLite.escape(ctx.column)}) != {len(ctx.s)} AND
                        instr(x'{values}', substr({SQLite.escape(ctx.column)}, {len(ctx.s) + 1}, 1))
                FROM    {SQLite.escape(ctx.table)}
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_unicode(self, ctx, n):
        query = f'''
            SELECT  unicode(substr({SQLite.escape(ctx.column)}, {len(ctx.s) + 1}, 1)) < {n}
            FROM    {SQLite.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)


    def string(self, ctx, values):
        values = [f"x'{v.encode('utf-8').hex()}'" for v in values]
        query = f'''
            SELECT  cast({SQLite.escape(ctx.column)} as BLOB) in ({','.join(values)})
            FROM    {SQLite.escape(ctx.table)}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)





class SQLite(DBMS):
    DATA_TYPES = ['INTEGER', 'TEXT', 'REAL', 'NUMERIC', 'BLOB']

    MetaQueries = SQLiteMetaQueries()
    TablesQueries = SQLiteTablesQueries()
    ColumnsQueries = SQLiteColumnsQueries()
    RowsQueries = SQLiteRowsQueries()
