from sqlglot import parse_one, exp

from .dbms import DBMS



class Postgres(DBMS):
    DIALECT = 'postgres'


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
                dialect='postgres',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema) if ctx.schema else exp.func('current_schema'),
            })
            cls.add_where(ast=ast, condition=where_filter)
            
            return ast


        @classmethod
        def target_column_names(cls, query, ast, ctx):
            query.table = exp.to_table('columns', db='information_schema')
            query.column = exp.to_column('column_name')

            where_filter = parse_one(
                sql='table_schema=@schema_name and table_name=@table_name',
                dialect='postgres',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema) if ctx.schema else exp.func('current_schema'),
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
                dialect='postgres',
            )
            where_filter = cls.resolve_params(query=query, ast=where_filter, ctx=ctx, params={
                'schema_name': exp.Literal.string(ctx.schema) if ctx.schema else exp.func('current_schema'),
                'table_name': exp.Literal.string(ctx.table),
                'column_name': exp.Literal.string(ctx.column),
            })
            cls.add_where(ast=ast, condition=where_filter)

            return ast


        @classmethod
        def to_literal(cls, value):
            if isinstance(value, bytes):
                return exp.Decode(
                    this=exp.Literal.string(value.hex()),
                    charset=exp.Literal.string('hex'),
                )
            return super().to_literal(value)



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(column) in ('integer', 'int', 'smallint', 'bigint', 'serial', 'smallserial', 'bigserial')
                from table
            ''',
            dialect='postgres',
        )



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(column) in ('decimal', 'numeric', 'real', 'float', 'double precision')
                from table
            ''',
            dialect='postgres',
        )



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(column) in ('character varying', 'varchar', 'character', 'char', 'bpchar', 'text')
                from table
            ''',
            dialect='postgres',
        )



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql="select lower(column) in ('bytea') from table",
            dialect='postgres',
        )



    class QueryRowsAreAscii(DBMS.QueryRowsAreAscii):
        AST_TEMPLATE = parse_one(
            sql="select bool_and(is_ascii(column)) from table",
            dialect='postgres'
        )



    class QueryRowIsAscii(DBMS.QueryRowIsAscii):
        AST_TEMPLATE = parse_one(
            sql="select bool_and(is_ascii(column)) from table limit 1 offset @row_idx",
            dialect='postgres',
        )



    class QueryCharIsAscii(DBMS.QueryCharIsAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select is_ascii(substr(column, @char_offset, 1))
                from table limit 1 offset @row_idx
            ''',
            dialect='postgres',
        )



    class QueryBlobCharInString(DBMS.QueryBlobCharInString):
        def ast_template(self):
            # sqlglot incorrectly parses position into StringPosition, which does not work for blobs
            # so we replace it with anonymous call to position(needle in haystack)
            ast = super().ast_template()

            original_position = ast.find(exp.StrPosition)
            if not original_position:
                return ast

            position_in = exp.In(
                this=original_position.args['substr'],
                field=original_position.this,
            )
            position = exp.func('POSITION', position_in)
            original_position.replace(position)
            return ast
