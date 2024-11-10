from sqlglot import parse_one, exp

from .dbms import DBMS



class SQLite(DBMS):
    DIALECT = 'sqlite'


    class QueryResolver(DBMS.QueryResolver):
        AST_TABLE_NAMES_FILTER = parse_one(
            sql="schema=@schema_name and type='table' and name != 'sqlite_schema'",
            dialect='sqlite',
        )


        def resolve_target_schema_names(self):
            self.resolve_params(params={
                'table': exp.func('pragma_database_list'),
                'column': exp.to_column('name'),
            })


        def resolve_target_table_names(self):
            table_names_filter = self.AST_TABLE_NAMES_FILTER.copy()
            orig_where_cond = self.ast.args.get('where')
            if orig_where_cond:
                self.ast.where(table_names_filter and orig_where_cond.this, copy=False)
            else:
                self.ast.where(table_names_filter, copy=False)

            self.resolve_params(params={
                'table': exp.func('pragma_table_list'),    
                'column': exp.to_column('name'),
                'schema_name': self.ctx.schema or 'main',
            })


        def resolve_target_column_names(self):
            self.resolve_params(params={
                'table': exp.func('pragma_table_info', exp.Literal.string(self.ctx.table)),
                'column': exp.to_column('name'),
            })



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='select lower(type) in (@types) from pragma_table_info(@table_name) where name=@column_name',
            dialect='sqlite',
        )

        def ast(self):
            return super().ast(params={
                'types': ['integer'],
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='select lower(type) in (@types) from pragma_table_info(@table_name) where name=@column_name',
            dialect='sqlite',
        )

        def ast(self):
            return super().ast(params={
                'types': ['float', 'real'],
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='select lower(type) in (@types) from pragma_table_info(@table_name) where name=@column_name',
            dialect='sqlite',
        )

        def ast(self):
            return super().ast(params={
                'types': ['text'],
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql='select lower(type) in (@types) from pragma_table_info(@table_name) where name=@column_name',
            dialect='sqlite',
        )

        def ast(self):
            return super().ast(params={
                'types': ['blob'],
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryRowsAreAscii(DBMS.QueryRowsAreAscii):
        PATTERN = b'*[^\x01-\x7f]*'
        AST_TEMPLATE = parse_one(
            sql=f"select min(not @column glob cast(x'{PATTERN.hex()}' as text)) from @table",
            dialect='sqlite'
        )



    class QueryRowIsAscii(DBMS.QueryRowIsAscii):
        PATTERN = b'*[^\x01-\x7f]*'
        AST_TEMPLATE = parse_one(
            sql=f"select not @column glob cast(x'{PATTERN.hex()}' as text) from @table limit 1 offset @row_idx",
            dialect='sqlite',
        )



    class QueryCharIsAscii(DBMS.QueryCharIsAscii):
        PATTERN = b'*[^\x01-\x7f]*'
        AST_TEMPLATE = parse_one(
            sql=f'''
                select not substr(@column, @char_offset, 1) glob cast(x'{PATTERN.hex()}' as text)
                from @table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )



    class QueryTextCharLt(DBMS.QueryTextCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select length(@column) > @str_length and unicode(substr(@column, @char_offset, 1)) < @n
                from @table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )
