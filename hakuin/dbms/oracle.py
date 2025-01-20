from sqlglot import parse_one, exp

from hakuin.utils import EOS

from .dbms import DBMS



class Oracle(DBMS):
    DIALECT = 'oracle'


    class QueryUtils(DBMS.QueryUtils):
        @classmethod
        def target_schema_names(cls, query, ast, ctx):
            query.table = exp.to_table('all_users')
            query.column = exp.to_column('username')
            return ast


        @classmethod
        def target_table_names(cls, query, ast, ctx):
            query.table = exp.to_table('all_tables')
            query.column = exp.to_column('table_name')

            where_filter = parse_one(
                sql="owner=@schema_name",
                dialect='oracle',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema.upper()) if ctx.schema else exp.to_identifier('user'),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast


        @classmethod
        def target_column_names(cls, query, ast, ctx):
            query.table = exp.to_table('all_tab_columns')
            query.column = exp.to_column('column_name')

            where_filter = parse_one(
                sql='owner=@schema_name and table_name=@table_name',
                dialect='oracle',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema.upper()) if ctx.schema else exp.to_identifier('user'),
                'table_name': exp.Literal.string(ctx.table.upper()),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast            


        @classmethod
        def target_column_type(cls, query, ast, ctx):
            query.table = exp.to_table('all_tab_columns')
            query.column = exp.to_column('data_type')

            where_filter = parse_one(
                sql='owner=@schema_name and table_name=@table_name and column_name=@column_name',
                dialect='oracle',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema.upper()) if ctx.schema else exp.to_identifier('user'),
                'table_name': exp.Literal.string(ctx.table.upper()),
                'column_name': exp.Literal.string(ctx.column.upper()),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast


        @classmethod
        def cast_to_text(cls, query, ast, ctx):
            query.column = exp.cast(query.column, to='varchar(4000)')
            return ast


        @classmethod
        def to_literal(cls, value):
            if isinstance(value, bytes):
                return exp.func('hextoraw', exp.Literal.string(value.hex()))
            return super().to_literal(value)



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) = 'number' and nvl(data_scale, 0) = 0 from table",
            dialect='oracle',
        )



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('float', 'number') and nvl(data_scale, 1) != 0 from table",
            dialect='oracle',
        )



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql="select lower(data_type) in ('char', 'nchar', 'varchar2', 'nvarchar2', 'clob', 'nclob') from table",
            dialect='oracle',
        )



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql="select lower(data_type) = 'blob' from table",
            dialect='oracle',
        )


    class QueryBlobCharLt(DBMS.QueryBlobCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select char_length(column) > @buffer_length and dbms_lob.substr(column, 1, @char_offset) < @byte
                from table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )
