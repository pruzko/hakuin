from sqlglot import parse_one, exp
from sqlglot.dialects import SQLite as SQLiteDialect

from .dbms import DBMS



class SQLite(DBMS):
    DIALECT = 'sqlite'


    class QueryUtils(DBMS.QueryUtils):
        @classmethod
        def target_schema_names(cls, query, ast, ctx):
            query.table = exp.func('pragma_database_list')
            query.column = exp.column('name')
            return ast


        @classmethod
        def target_table_names(cls, query, ast, ctx):
            query.table = exp.func('pragma_table_list')
            query.column = exp.column('name')

            where_filter = parse_one(
                sql="schema=@schema_name and type='table' and name != 'sqlite_schema'",
                dialect='sqlite',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema or 'main'),
            })
            cls.add_where(ast=ast, condition=where_filter)
            
            return ast


        @classmethod
        def target_column_names(cls, query, ast, ctx):
            query.table = exp.func('pragma_table_info', exp.Literal.string(ctx.table))
            query.column = exp.column('name')
            return ast


        @classmethod
        def target_column_type(cls, query, ast, ctx):
            query.table = exp.func('pragma_table_info', exp.Literal.string(ctx.table))
            query.column = exp.column('type')

            where_filter = parse_one(
                sql='name=@column_name',
                dialect='sqlite',
            )
            column_name = exp.Literal.string(ctx.column)
            where_filter.find(exp.Parameter).replace(column_name)
            cls.add_where(ast=ast, condition=where_filter)
            
            return ast



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('integer') from table",
            dialect='sqlite',
        )



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('float', 'real') from table",
            dialect='sqlite',
        )



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('text') from table",
            dialect='sqlite',
        )



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('blob') from table",
            dialect='sqlite',
        )

