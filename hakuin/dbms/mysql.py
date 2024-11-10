from sqlglot import parse_one, exp

from hakuin.utils import EOS

from .dbms import DBMS



class MySQL(DBMS):
    DIALECT = 'mysql'


    class QueryResolver(DBMS.QueryResolver):
        AST_TABLE_NAMES_FILTER = parse_one(
            sql="table_schema=@schema_name and table_type='BASE TABLE'",
            dialect='mysql',
        )
        AST_COLUMN_NAMES_FILTER = parse_one(
            sql='table_schema=@schema_name and table_name=@table_name',
            dialect='mysql',
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
                'schema_name': self.ctx.schema or exp.func('schema'),
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
                'schema_name': self.ctx.schema or exp.func('schema'),
            })



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(data_type, 'int') and not regexp_like(data_type, 'point')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='mysql',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='''
                select data_type in ('decimal', 'numeric', 'float', 'double')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='mysql',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(data_type, 'text') or data_type in ('char', 'varchar', 'linestring', 'multilinestring', 'json')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='mysql',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(data_type, 'blob') or data_type in ('binary', 'varbinary')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='mysql',
        )

        def ast(self):
            return super().ast(params={
                'schema_name': self.ctx.schema or exp.func(name='schema'),
                'table_name': self.ctx.table,
                'column_name': self.ctx.column,
            })



    class QueryRowsAreAscii(DBMS.QueryRowsAreAscii):
        AST_TEMPLATE = parse_one(
            sql=f"select min(not regexp_like(@column, '[^[:ascii:]]')) from @table",
            dialect='mysql'
        )



    class QueryRowIsAscii(DBMS.QueryRowIsAscii):
        AST_TEMPLATE = parse_one(
            sql=f"select not regexp_like(@column, '[^[:ascii:]]') from @table limit 1 offset @row_idx",
            dialect='mysql',
        )



    class QueryCharIsAscii(DBMS.QueryCharIsAscii):
        AST_TEMPLATE = parse_one(
            sql=f'''
                select not regexp_like(substr(@column, @char_offset, 1), '[^[:ascii:]]')
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )



    class QueryValueInList(DBMS.QueryValueInList):
        AST_TEMPLATE = parse_one(
            sql='select binary(@column) in (@values) from @table limit 1 offset @row_idx',
            dialect='mysql',
        )



    class QueryCharInString(DBMS.QueryCharInString):
        AST_TEMPLATE_EOS = parse_one(
            sql='''
                select char_length(@column) = @str_length or
                    instr(@values_str, binary(substr(@column, @char_offset, 1)))
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )
        AST_TEMPLATE_NO_EOS = parse_one(
            sql='''
                select char_length(@column) > @str_length and
                    instr(@values_str, binary(substr(@column, @char_offset, 1)))
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )



    class QueryTextCharInString(QueryCharInString):
        def __init__(self, dbms, ctx, values):
            super().__init__(dbms, ctx)
            assert values, f'No values provided.'
            self.values_str = ''.join([v for v in values if v != EOS])
            self.has_eos = EOS in values



    class QueryBlobCharInString(QueryCharInString):
        def __init__(self, dbms, ctx, values):
            super().__init__(dbms, ctx)
            assert values, f'No values provided.'
            self.values_str = b''.join([v for v in values if v != EOS])
            self.has_eos = EOS in values



    class QueryTextCharLt(DBMS.QueryTextCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select char_length(@column) > @str_length and
                    ord(convert(substr(@column, @char_offset, 1) using utf32)) < @n
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )


    class QueryBlobCharLt(DBMS.QueryBlobCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select char_length(@column) > @str_length and
                    substr(@column, @char_offset, 1) < @byte
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )
