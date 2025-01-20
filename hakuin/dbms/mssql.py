from sqlglot import parse_one, exp

from .dbms import DBMS



class MSSQL(DBMS):
    DIALECT = 'tsql'


    class QueryUtils(DBMS.QueryUtils):
        @classmethod
        def resolve_query(cls, query, ast, ctx, params):
            ast = super().resolve_query(query, ast, ctx, params)
            exp_if = exp.If(
                this=ast.expressions[0],
                true=exp.Literal.number(1),
                false=exp.Literal.number(0),
            )
            ast.set('expressions', [exp_if])
            return ast


        @classmethod
        def target_schema_names(cls, query, ast, ctx):
            query.table = exp.to_table('schemata', db='information_schema')
            query.column = exp.to_column('schema_name')
            return ast


        @classmethod
        def target_table_names(cls, query, ast, ctx):
            query.table = exp.to_table('tables', db='information_schema')
            query.column = exp.to_column('table_name')

            where_filter = parse_one(
                sql="table_schema=@schema_name and table_type='BASE TABLE'",
                dialect='tsql',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.National(this=ctx.schema) if ctx.schema else exp.func('schema_name'),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast


        @classmethod
        def target_column_names(cls, query, ast, ctx):
            query.table = exp.to_table('columns', db='information_schema')
            query.column = exp.to_column('column_name')

            where_filter = parse_one(
                sql='table_schema=@schema_name and table_name=@table_name',
                dialect='tsql',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.National(this=ctx.schema) if ctx.schema else exp.func('schema_name'),
                'table_name': exp.National(this=ctx.table),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast            


        @classmethod
        def target_column_type(cls, query, ast, ctx):
            query.table = exp.to_table('columns', db='information_schema')
            query.column = exp.to_column('data_type')

            where_filter = parse_one(
                sql='table_schema=@schema_name and table_name=@table_name and column_name=@column_name',
                dialect='tsql',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.National(this=ctx.schema) if ctx.schema else exp.func('schema_name'),
                'table_name': exp.National(this=ctx.table),
                'column_name': exp.National(this=ctx.column),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast


        @classmethod
        def to_literal(cls, value):
            if isinstance(value, str):
                return exp.National(this=value)
            return super().to_literal(value)



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('int', 'bigint', 'smallint', 'bit') from table",
            dialect='tsql',
        )



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(column) in ('float', 'real', 'decimal', 'dec', 'numeric', 'money', 'smallmoney')
                from table
            ''',
            dialect='tsql',
        )



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext') from table",
            dialect='tsql',
        )



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('binary', 'varbinary', 'image') from table",
            dialect='tsql',
        )


    class QueryRowsHaveNull(DBMS.QueryRowsHaveNull):
        AST_TEMPLATE = parse_one(
            sql='select count(*) > 0 from table where column is null',
            dialect='tsql',
        )


    class QueryRowsAreAscii(DBMS.QueryRowsAreAscii):
        AST_TEMPLATE = parse_one(
            sql='select count(*) = 0 from table where not is_ascii(column)',
            dialect='tsql',
        )