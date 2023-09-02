from hakuin.utils import EOS

from .DBMS import DBMS



class SQLite(DBMS):
    DATA_TYPES = ['INTEGER', 'TEXT', 'REAL', 'NUMERIC', 'BLOB']



    def count_rows(self, ctx, n):
        query = f'''
            SELECT  COUNT(*) < {n}
            FROM    {ctx.table}
        '''
        return self.normalize(query)


    def count_tables(self, ctx, n):
        query = f'''
            SELECT  COUNT(*) < {n}
            FROM    sqlite_master
            WHERE   type='table'
        '''
        return self.normalize(query)


    def count_columns(self, ctx, n):
        query = f'''
            SELECT  COUNT(*) < {n}
            FROM    pragma_table_info('{ctx.table}')
        '''
        return self.normalize(query)


    def meta_type(self, ctx, values):
        values = [f"'{v}'" for v in values]
        query = f'''
            SELECT  type in ({','.join(values)})
            FROM    pragma_table_info('{ctx.table}')
            WHERE   name='{ctx.column}'
        '''
        return self.normalize(query)


    def meta_is_nullable(self, ctx):
        query = f'''
            SELECT  [notnull] == 0
            FROM    pragma_table_info('{ctx.table}')
            WHERE   name='{ctx.column}'
        '''
        return self.normalize(query)


    def meta_is_pk(self, ctx):
        query = f'''
            SELECT  pk
            FROM    pragma_table_info('{ctx.table}')
            WHERE   name='{ctx.column}'
        '''
        return self.normalize(query)


    def char_rows(self, ctx, values):
        has_eos = EOS in values
        values = [v for v in values if v != EOS]
        values = ''.join(values).encode('ascii').hex()

        if has_eos:
            # if the next char is EOS, substr() resolves to "" and subsequently instr(..., "") resolves to True
            query = f'''
                SELECT  instr(x'{values}', substr({ctx.column}, {len(ctx.s) + 1}, 1))
                FROM    {ctx.table}
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  substr({ctx.column}, {len(ctx.s) + 1}, 1) != '' AND
                        instr(x'{values}', substr({ctx.column}, {len(ctx.s) + 1}, 1))
                FROM    {ctx.table}
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_tables(self, ctx, values):
        has_eos = EOS in values
        values = [v for v in values if v != EOS]
        values = ''.join(values).encode('ascii').hex()

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
                SELECT  substr(name, {len(ctx.s) + 1}, 1) != '' AND
                        instr(x'{values}', substr(name, {len(ctx.s) + 1}, 1))
                FROM    sqlite_master
                WHERE   type='table'
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def char_columns(self, ctx, values):
        has_eos = EOS in values
        values = [v for v in values if v != EOS]
        values = ''.join(values).encode('ascii').hex()

        if has_eos:
            query = f'''
                SELECT  instr(x'{values}', substr(name, {len(ctx.s) + 1}, 1))
                FROM    pragma_table_info('{ctx.table}')
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  substr(name, {len(ctx.s) + 1}, 1) != '' AND
                        instr(x'{values}', substr(name, {len(ctx.s) + 1}, 1))
                FROM    pragma_table_info('{ctx.table}')
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def string_rows(self, ctx, values):
        values = [f"x'{v.encode('ascii').hex()}'" for v in values]
        query = f'''
            SELECT  cast({ctx.column} as BLOB) in ({','.join(values)})
            FROM    {ctx.table}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)
