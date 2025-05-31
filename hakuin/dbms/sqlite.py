from sqlglot import parse_one, exp
from sqlglot.dialects import SQLite as SQLiteDialect

from .dbms import DBMS



class SQLite(DBMS):
    DIALECT = 'sqlite'


    def target_schema_names(self, query, ast, ctx):
        query.table = exp.func('pragma_database_list')
        query.column = exp.column('name')
        return ast


    def target_table_names(self, query, ast, ctx):
        query.table = exp.func('pragma_table_list')
        query.column = exp.column('name')

        where_filter = parse_one(
            sql="schema=@schema_name and type='table' and name != 'sqlite_schema'",
            dialect='sqlite',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
        })
        ast.where(where_filter, copy=False)

        return ast


    def target_column_names(self, query, ast, ctx):
        query.table = exp.func('pragma_table_info', self.literal_text(ctx.table))
        query.column = exp.column('name')
        return ast


    def target_column_type(self, query, ast, ctx):
        query.table = exp.func('pragma_table_info', self.literal_text(ctx.table))
        query.column = exp.column('type')

        where_filter = parse_one(sql='name=@column_name', dialect='sqlite')
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'column_name': self.literal_text(ctx.column),
        })
        ast.where(where_filter, copy=False)

        return ast


    def force_server_error(self):
        return exp.func('load_extension', 0)



    class QueryTernary(DBMS.QueryTernary):
        AST_TERNARY = parse_one(
            # sqlite does not short-circuit logical expressions, only case statements
            sql='case when @cond1 then true when @cond2 then false else @error end',
            dialect='sqlite',
        )



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

