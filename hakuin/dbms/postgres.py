from sqlglot import parse_one, exp

from .dbms import DBMS



class Postgres(DBMS):
    DIALECT = 'postgres'


    def target_schema_names(self, query, ast, ctx):
        query.table = exp.to_table('schemata', db='information_schema')
        query.column = exp.to_column('schema_name')
        return ast


    def target_table_names(self, query, ast, ctx):
        query.table = exp.to_table('tables', db='information_schema')
        query.column = exp.to_column('table_name')

        where_filter = parse_one(
            sql="table_schema=@schema_name and table_type='BASE TABLE'",
            dialect='postgres',
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
            dialect='postgres',
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
            dialect='postgres',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
            'table_name': self.literal_text(ctx.table),
            'column_name': self.literal_text(ctx.column),
        })
        ast.where(where_filter, copy=False)

        return ast


    def literal_blob(self, value):
        return exp.Decode(
            this=self.literal_text(value.hex()),
            charset=self.literal_text('hex'),
        )


    def get_schema_name(self, ctx):
        # TODO this should be in DBMS after sqlglot
        if ctx.schema:
            return self.literal_text(ctx.schema)
        return exp.func('current_schema')





    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(column) in
                    ('integer', 'int', 'smallint', 'bigint', 'serial', 'smallserial', 'bigserial')
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
                select lower(column) in
                    ('character varying', 'varchar', 'character', 'char', 'bpchar', 'text')
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


    # TODO delete this after sqlglot
    class QueryBlobCharInString(DBMS.QueryBlobCharInString):
        def ast_template(self):
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
