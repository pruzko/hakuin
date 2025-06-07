from sqlglot import parse_one, exp

from .dbms import DBMS



class MSSQL(DBMS):
    DIALECT = 'tsql'


    def wrap_ast(self, ast):
        ast.set('expressions', [exp.If(
            this=ast.expressions[0],
            true=self.literal_int(1),
            false=self.literal_int(0),
        )])
        return ast


    def target_schema_names(self, query, ast, ctx):
        query.table = exp.to_table('schemata', db='information_schema')
        query.column = exp.to_column('schema_name')
        return ast


    def target_table_names(self, query, ast, ctx):
        query.table = exp.to_table('tables', db='information_schema')
        query.column = exp.to_column('table_name')

        where_filter = parse_one(
            sql="table_schema=@schema_name and table_type='BASE TABLE'",
            dialect='tsql',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
        })
        ast.where(where_filter, copy=False)

        return ast


    def target_column_names(self, query, ast, ctx):
        query.table = exp.to_table('columns', db='information_schema')
        query.column = exp.to_column('column_name')

        where_filter = parse_one(
            sql='table_schema=@schema_name and table_name=@table_name',
            dialect='tsql',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
            'table_name': self.literal_text(ctx.table),
        })
        ast.where(where_filter, copy=False)

        return ast            


    def target_column_type(self, query, ast, ctx):
        query.table = exp.to_table('columns', db='information_schema')
        query.column = exp.to_column('data_type')

        where_filter = parse_one(
            sql='''
                table_schema=@schema_name and table_name=@table_name and
                column_name=@column_name
            ''',
            dialect='tsql',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
            'table_name': self.literal_text(ctx.table),
            'column_name': self.literal_text(ctx.column),
        })
        ast.where(where_filter, copy=False)

        return ast


    def literal_text(self, value):
        return exp.National(this=value)


    def force_server_error(self):
        return exp.func('log', 0).eq(0)



    class QueryTernary(DBMS.QueryTernary):
        def ast(self, ctx):
            ast1 = self.query1.ast(ctx)
            ast2 = self.query2.ast(ctx)

            ternary_exp = self.resolve_params(ast=self.AST_TERNARY.copy(), ctx=ctx, params={
                'cond1': ast1.expressions[0].this,
                'cond2': ast2.expressions[0].this,
                'error': self.dbms.force_server_error(),
            })

            ast1.set('expressions', [ternary_exp])
            return self.dbms.wrap_ast(ast=ast1)



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('int', 'bigint', 'smallint', 'bit') from table",
            dialect='tsql',
        )



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(column) in
                    ('float', 'real', 'decimal', 'dec', 'numeric', 'money', 'smallmoney')
                from table
            ''',
            dialect='tsql',
        )



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(column) in
                    ('char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext')
                from table
            ''',
            dialect='tsql',
        )



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('binary', 'varbinary', 'image') from table",
            dialect='tsql',
        )


    class QueryColumnHasNull(DBMS.QueryColumnHasNull):
        AST_TEMPLATE = parse_one(
            sql='select count(*) > 0 from table where column is null',
            dialect='tsql',
        )


    class QueryColumnIsAscii(DBMS.QueryColumnIsAscii):
        AST_TEMPLATE = parse_one(
            sql='select count(*) = 0 from table where not is_ascii(column)',
            dialect='tsql',
        )