from sqlglot import parse_one, exp

from .dbms import DBMS



class Postgres(DBMS):
    DIALECT = 'postgres'


    class QueryResolver(DBMS.QueryResolver):
        AST_TABLE_NAMES_FILTER = parse_one(
            sql="table_schema=@schema_name and table_type='BASE TABLE'",
            dialect='postgres',
        )
        AST_COLUMN_NAMES_FILTER = parse_one(
            sql='table_schema=@schema_name and table_name=@table_name',
            dialect='postgres',
        )


        def resolve_target_schema_names(self):
            self.resolve_params(params={
                'table': exp.to_table('schemata', db='information_schema'),
                'column': exp.to_column('schema_name'),
            })


        def resolve_target_table_names(self):
            table_names_filter = self.AST_TABLE_NAMES_FILTER.copy()
            orig_where_cond = self.ast.args.get('where')
            if orig_where_cond:
                self.ast.where(table_names_filter and orig_where_cond.this, copy=False)
            else:
                self.ast.where(table_names_filter, copy=False)

            self.resolve_params(params={
                'table': exp.to_table('tables', db='information_schema'),
                'column': exp.to_column('table_name'),
                'schema_name': self.ctx.schema or exp.func('current_schema'),
            })


        def resolve_target_column_names(self):
            column_names_filter = self.AST_COLUMN_NAMES_FILTER.copy()
            orig_where_cond = self.ast.args.get('where')
            if orig_where_cond:
                self.ast.where(column_names_filter and orig_where_cond.this, copy=False)
            else:
                self.ast.where(column_names_filter, copy=False)

            self.resolve_params(params={
                'table': exp.to_table('columns', db='information_schema'),
                'column': exp.to_column('column_name'),
                'table_name': self.ctx.table,
                'schema_name': self.ctx.schema or exp.func('current_schema'),
            })


        def _to_literal(self, value):
            if isinstance(value, bytes):
                return exp.Decode(
                    this=exp.Literal.string(value.hex()),
                    charset=exp.Literal.string('hex'),
                )
            return super()._to_literal(value)



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('integer', 'int', 'smallint', 'bigint', 'serial', 'smallserial', 'bigserial')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='postgres',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='current_schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('decimal', 'numeric', 'real', 'float', 'double precision')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='postgres',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='current_schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('character varying', 'varchar', 'character', 'char', 'bpchar', 'text')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='postgres',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='current_schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('bytea')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='postgres',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='current_schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryRowsHaveNull(DBMS.QueryRowsHaveNull):
        AST_TEMPLATE = parse_one(
            sql='select bool_or(@column is null) from @table',
            dialect='postgres',
        )



    class QueryRowsArePositive(DBMS.QueryRowsArePositive):
        AST_TEMPLATE = parse_one(
            sql='select bool_and(@column >= 0) from @table',
            dialect='postgres',
        )



    class QueryRowsAreAscii(DBMS.QueryRowsAreAscii):
        AST_TEMPLATE = parse_one(
            sql="select bool_and(@column ~ '^[[:ascii:]]*$') from @table",
            dialect='postgres'
        )



    class QueryRowIsAscii(DBMS.QueryRowIsAscii):
        AST_TEMPLATE = parse_one(
            sql="select bool_and(@column ~ '^[[:ascii:]]*$') from @table limit 1 offset @row_idx",
            dialect='postgres',
        )



    class QueryCharIsAscii(DBMS.QueryCharIsAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select substr(@column, @char_offset, 1) ~ '^[[:ascii:]]*$'
                from @table limit 1 offset @row_idx
            ''',
            dialect='postgres',
        )



    class QueryBlobCharInString(DBMS.QueryBlobCharInString):
        def ast(self):
            # sqlglot incorrectly parses position into StringPosition, which does not work for blobs
            # so we replace it with anonymous call to position(needle in haystack)
            ast = super().ast()
            str_pos = ast.find(exp.StrPosition)
            if str_pos:
                str_pos.replace(
                    exp.Anonymous(
                        this='POSITION',
                        expressions=[
                            exp.In(
                                this=str_pos.args['substr'],
                                field=str_pos.this,
                            )
                        ],
                    )
                )
            return ast



    class QueryTextCharLt(DBMS.QueryTextCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select length(@column) > @str_length and ascii(substr(@column, @char_offset, 1)) < @n
                from @table limit 1 offset @row_idx
            ''',
            dialect='postgres',
        )
