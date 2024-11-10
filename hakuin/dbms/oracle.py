from sqlglot import parse_one, exp

from hakuin.utils import EOS

from .dbms import DBMS



class Oracle(DBMS):
    DIALECT = 'oracle'


    class QueryResolver(DBMS.QueryResolver):
        AST_CAST_TO_TEXT = parse_one(sql='cast(@column as varchar2(4000))', dialect='oracle')

        AST_TABLE_NAMES_FILTER = parse_one(
            sql='owner=@schema_name',
            dialect='oracle',
        )
        AST_COLUMN_NAMES_FILTER = parse_one(
            sql='owner=@schema_name and table_name=@table_name',
            dialect='oracle',
        )


        def resolve_target_schema_names(self):
            self.resolve_params(params={
                'table': exp.to_table('all_users'),
                'column': exp.to_column('username'),
            })


        def resolve_target_table_names(self):
            table_names_filter = self.AST_TABLE_NAMES_FILTER.copy()
            orig_where_cond = self.ast.args.get('where')
            if orig_where_cond:
                self.ast.where(table_names_filter and orig_where_cond.this, copy=False)
            else:
                self.ast.where(table_names_filter, copy=False)

            self.resolve_params(params={
                'table': exp.to_table('all_tables'),
                'column': exp.to_column('table_name'),
                'schema_name': self.ctx.schema.upper() if self.ctx.schema else exp.to_identifier('user'),
            })


        def resolve_target_column_names(self):
            column_names_filter = self.AST_COLUMN_NAMES_FILTER.copy()
            orig_where_cond = self.ast.args.get('where')
            if orig_where_cond:
                self.ast.where(column_names_filter and orig_where_cond.this, copy=False)
            else:
                self.ast.where(column_names_filter, copy=False)

            self.resolve_params(params={
                'table': exp.to_table('all_tab_columns'),
                'column': exp.to_column('column_name'),
                'table_name': self.ctx.table.upper(),
                'schema_name': self.ctx.schema.upper() if self.ctx.schema else exp.to_identifier('user'),
            })


        def resolve_params(self, params={}):
            params = self._process_params(params)
            for param in self.ast.find_all(exp.Parameter):
                if param.name == 'column':
                    node = param.parent if isinstance(param.parent, exp.Column) else param
                    column = params.get('column') or exp.column(self.ctx.column.upper(), quoted=True)
                    node.replace(column)
                elif param.name == 'table':
                    node = param.parent if isinstance(param.parent, exp.Table) else param
                    table = params.get('table') or exp.table_(self.ctx.table.upper(), quoted=True)
                    node.replace(table)
                elif param.name in params:
                    param.replace(params[param.name])


        def _to_literal(self, value):
            if isinstance(value, bytes):
                return exp.Anonymous(
                    this='hextoraw',
                    expressions=[exp.Literal.string(value.hex())],
                )
            return super()._to_literal(value)



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) = 'number' and data_scale = 0
                from all_tab_columns
                where owner = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='oracle',
        )

        def ast(self):
            return super().ast(params={
                'table_name': self.ctx.table.upper(),
                'column_name': self.ctx.column.upper(),
                'schema_name': self.ctx.schema.upper() if self.ctx.schema else exp.to_identifier('user'),
            })



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) = 'float' or lower(data_type) = 'number' and data_scale != 0
                from all_tab_columns
                where owner = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='oracle',
        )

        def ast(self):
            return super().ast(params={
                'table_name': self.ctx.table.upper(),
                'column_name': self.ctx.column.upper(),
                'schema_name': self.ctx.schema.upper() if self.ctx.schema else exp.to_identifier('user'),
            })



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('char', 'nchar', 'varchar2', 'nvarchar2', 'clob', 'nclob')
                from all_tab_columns
                where owner = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='oracle',
        )

        def ast(self):
            return super().ast(params={
                'table_name': self.ctx.table.upper(),
                'column_name': self.ctx.column.upper(),
                'schema_name': self.ctx.schema.upper() if self.ctx.schema else exp.to_identifier('user'),
            })



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('blob')
                from all_tab_columns
                where owner = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='oracle',
        )

        def ast(self):
            return super().ast(params={
                'table_name': self.ctx.table.upper(),
                'column_name': self.ctx.column.upper(),
                'schema_name': self.ctx.schema.upper() if self.ctx.schema else exp.to_identifier('user'),
            })



    class QueryRowsAreAscii(DBMS.QueryRowsAreAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select max(
                    nvl(
                        regexp_like(
                            @column,
                            '^[' || chr(1) || '-' || chr(127) || ']*$'
                        ),
                        true
                    )
                )
                from @table
            ''',
            dialect='oracle'
        )



    class QueryRowIsAscii(DBMS.QueryRowIsAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select nvl(
                    regexp_like(
                        @column,
                        '^[' || chr(1) || '-' || chr(127) || ']*$'
                    ),
                    true
                )
                from @table limit 1 offset @row_idx
            ''',
            dialect='oracle'
        )



    class QueryCharIsAscii(DBMS.QueryCharIsAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select nvl(
                    regexp_like(
                        substr(@column, @char_offset, 1),
                        '^[' || chr(1) || '-' || chr(127) || ']*$'
                    ),
                    true
                )
                from @table limit 1 offset @row_idx
            ''',
            dialect='oracle'
        )



    class QueryCharInString(DBMS.QueryCharInString):
        def ast(self):
            # sqlglot incorrectly converts instr to str_pos, which is not supported by oracle
            # so we convert it an anonymous call to instr()
            ast = super().ast()
            for node in ast.find_all(exp.StrPosition):
                node.replace(
                    exp.Anonymous(
                        this='instr',
                        expressions=[node.args['this'], node.args['substr']],
                    )
                )
            return ast



    class QueryTextCharInString(QueryCharInString):
        def __init__(self, dbms, ctx, values):
            super().__init__(dbms, ctx)
            assert values, f'No values provided.'
            self.values_str = ''.join([v for v in values if v != EOS])
            self.has_eos = EOS in values



    class QueryBlobCharInString(QueryCharInString):
        AST_TEMPLATE_EOS = parse_one(
            sql='''
                select length(@column) = @str_length or instr(@values_str, substr(@column, @char_offset, 1)) > 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )
        AST_TEMPLATE_NO_EOS = parse_one(
            sql='''
                select length(@column) > @str_length and instr(@values_str, substr(@column, @char_offset, 1)) > 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )


        def __init__(self, dbms, ctx, values):
            super().__init__(dbms, ctx)
            assert values, f'No values provided.'
            self.values_str = b''.join([v for v in values if v != EOS])
            self.has_eos = EOS in values



    class QueryTextCharLt(DBMS.QueryTextCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select length(@column) > @str_length and ascii(unistr(substr(@column, @char_offset, 1))) < @n
                from @table limit 1 offset @row_idx
            ''',
            dialect='oracle',
        )



    class QueryBlobCharLt(DBMS.QueryBlobCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select length(@column) > @str_length and dbms_lob.substr(@column, 1, @char_offset) < @byte
                from @table limit 1 offset @row_idx
            ''',
            dialect='oracle',
        )
