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


    def force_server_error(self):
        return exp.func('pg_has_role', exp.Literal.string(''), exp.Literal.string(''))



    class QueryTernary(DBMS.QueryTernary):
        AST_TEMPLATE = parse_one(
            sql='select @ternary_exp from (@select_row) as t',
            dialect='postgres',
        )

        def ast(self, ctx):
            ast1 = self.query1.ast(ctx)
            ast2 = self.query2.ast(ctx)

            ternary_exp = self.resolve_params(ast=self.AST_TERNARY.copy(), ctx=ctx, params={
                'cond1': ast1.expressions[0],
                'cond2': ast2.expressions[0],
                'error': self.dbms.force_server_error(),
            })

            ast1.set('expressions', [exp.Star()])
            return self.resolve_params(ast=self.AST_TEMPLATE.copy(), ctx=ctx, params={
                'ternary_exp': ternary_exp,
                'select_row': ast1,
            })



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



    class QueryColumnIsAscii(DBMS.QueryColumnIsAscii):
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
