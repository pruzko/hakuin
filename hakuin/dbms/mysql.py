from sqlglot import parse_one, exp

from .dbms import DBMS



class MySQL(DBMS):
    DIALECT = 'mysql'


    class QueryUtils(DBMS.QueryUtils):
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
                dialect='mysql',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema) if ctx.schema else exp.func('schema'),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast


        @classmethod
        def target_column_names(cls, query, ast, ctx):
            query.table = exp.to_table('columns', db='information_schema')
            query.column = exp.to_column('column_name')

            where_filter = parse_one(
                sql='table_schema=@schema_name and table_name=@table_name',
                dialect='mysql',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema) if ctx.schema else exp.func('schema'),
                'table_name': exp.Literal.string(ctx.table),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast            


        @classmethod
        def target_column_type(cls, query, ast, ctx):
            query.table = exp.to_table('columns', db='information_schema')
            query.column = exp.to_column('data_type')

            where_filter = parse_one(
                sql='table_schema=@schema_name and table_name=@table_name and column_name=@column_name',
                dialect='mysql',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema) if ctx.schema else exp.func('schema'),
                'table_name': exp.Literal.string(ctx.table),
                'column_name': exp.Literal.string(ctx.column),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql="select regexp_like(column, 'int') and not regexp_like(column, 'point') from table",
            dialect='mysql',
        )



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql="select column in ('decimal', 'numeric', 'float', 'double') from table",
            dialect='mysql',
        )



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(column, 'text')
                    or data_type in ('char', 'varchar', 'linestring', 'multilinestring', 'json')
                from table
            ''',
            dialect='mysql',
        )



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(column, 'blob')
                    or data_type in ('binary', 'varbinary')
                from table
            ''',
            dialect='mysql',
        )



    class QueryValueInList(DBMS.QueryValueInList):
        AST_TEMPLATE = parse_one(
            sql='select binary(column) in (@values) from @table limit 1 offset @row_idx',
            dialect='mysql',
        )



    class QueryTextCharInString(DBMS.QueryTextCharInString):
        AST_TEMPLATE = parse_one(
            sql='''
                select instr(@values_str, binary(substr(column, @char_offset, 1))) > 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )



    class QueryBlobCharInString(DBMS.QueryBlobCharInString):
        AST_TEMPLATE = parse_one(
            sql='''
                select instr(@values_str, binary(substr(column, @char_offset, 1))) > 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )
