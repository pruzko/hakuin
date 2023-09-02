from hakuin.utils import EOS

from .DBMS import DBMS




class MySQL(DBMS):
    DATA_TYPES = [
        'integer', 'smallint', 'tinyint', 'mediumint', 'bigint', 'decimal',
        'numeric', 'float', 'double', 'bit', 'date', 'datetime', 'timestamp',
        'time', 'year', 'char', 'varchar', 'binary', 'varbinary', 'blob', 'text',
        'enum', 'set', 'geometry', 'point', 'linestring', 'polygon ', 'multipoint',
        'multilinestring', 'multipolygon', 'geometrycollection ', 'json'
    ]



    def count_rows(self, ctx, n):
        query = f'''
            SELECT  COUNT(*) < {n}
            FROM    {ctx.table}
        '''
        return self.normalize(query)


    def count_tables(self, ctx, n):
        query = f'''
            SELECT  COUNT(*) < {n}
            FROM    information_schema.TABLES
            WHERE   TABLE_SCHEMA=DATABASE()
        '''
        return self.normalize(query)


    def count_columns(self, ctx, n):
        query = f'''
            SELECT  COUNT(*) < {n}
            FROM    information_schema.COLUMNS
            WHERE   TABLE_SCHEMA=DATABASE() AND
                    TABLE_NAME='{ctx.table}'
        '''
        return self.normalize(query)


    def meta_type(self, ctx, values):
        values = [f"'{v}'" for v in values]
        query = f'''
            SELECT  LOWER(DATA_TYPE) in ({','.join(values)})
            FROM    information_schema.columns
            WHERE   TABLE_SCHEMA=DATABASE() AND
                    TABLE_NAME='{ctx.table}' AND
                    COLUMN_NAME='{ctx.column}'
        '''
        return self.normalize(query)


    def meta_is_nullable(self, ctx):
        query = f'''
            SELECT  IS_NULLABLE='YES'
            FROM    information_schema.columns
            WHERE   TABLE_SCHEMA=DATABASE() AND
                    TABLE_NAME='{ctx.table}' AND
                    COLUMN_NAME='{ctx.column}'
        '''
        return self.normalize(query)


    def meta_is_pk(self, ctx):
        query = f'''
            SELECT  COLUMN_KEY='PRI'
            FROM    information_schema.columns
            WHERE   TABLE_SCHEMA=DATABASE() AND
                    TABLE_NAME='{ctx.table}' AND
                    COLUMN_NAME='{ctx.column}'
        '''
        return self.normalize(query)


    def char_rows(self, ctx, values):
        has_eos = EOS in values
        values = [v for v in values if v != EOS]
        values = ''.join(values).encode('ascii').hex()

        if has_eos:
            # if the next char is EOS, substr() resolves to "" and subsequently instr(..., "") resolves to True
            query = f'''
                SELECT  LOCATE(SUBSTRING({ctx.column}, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    {ctx.table}
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  SUBSTRING({ctx.column}, {len(ctx.s) + 1}, 1) != '' AND
                        LOCATE(SUBSTRING({ctx.column}, {len(ctx.s) + 1}, 1), x'{values}')
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
                SELECT  LOCATE(SUBSTRING(TABLE_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.TABLES
                WHERE   TABLE_SCHEMA=DATABASE()
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  SUBSTRING(TABLE_NAME, {len(ctx.s) + 1}, 1) != '' AND
                        LOCATE(SUBSTRING(TABLE_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.TABLES
                WHERE   TABLE_SCHEMA=DATABASE()
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
                SELECT  LOCATE(SUBSTRING(COLUMN_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.COLUMNS
                WHERE   TABLE_SCHEMA=DATABASE() AND
                        TABLE_NAME='{ctx.table}'
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        else:
            query = f'''
                SELECT  SUBSTRING(COLUMN_NAME, {len(ctx.s) + 1}, 1) != '' AND
                        LOCATE(SUBSTRING(COLUMN_NAME, {len(ctx.s) + 1}, 1), x'{values}')
                FROM    information_schema.COLUMNS
                WHERE   TABLE_SCHEMA=DATABASE() AND
                        TABLE_NAME='{ctx.table}'
                LIMIT   1
                OFFSET  {ctx.row}
            '''
        return self.normalize(query)


    def string_rows(self, ctx, values):
        values = [f"x'{v.encode('ascii').hex()}'" for v in values]
        query = f'''
            SELECT  {ctx.column} in ({','.join(values)})
            FROM    {ctx.table}
            LIMIT   1
            OFFSET  {ctx.row}
        '''
        return self.normalize(query)
