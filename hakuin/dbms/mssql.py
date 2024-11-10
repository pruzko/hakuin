from sqlglot import parse_one, exp

from .dbms import DBMS



class MSSQL(DBMS):
    DIALECT = 'tsql'


    class QueryResolver(DBMS.QueryResolver):
        AST_TABLE_NAMES_FILTER = parse_one(
            sql="table_schema=@schema_name and table_type='BASE TABLE'",
            dialect='tsql',
        )
        AST_COLUMN_NAMES_FILTER = parse_one(
            sql='table_schema=@schema_name and table_name=@table_name',
            dialect='tsql',
        )


        def resolve(self, params={}):
            self.ast.set('expressions', [
                exp.If(
                    this=self.ast.expressions[0],
                    true=exp.Literal.number(1),
                    false=exp.Literal.number(0),
                )
            ])
            return super().resolve(params=params)


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
                'schema_name': self._get_schema_name(),
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
                'table_name': exp.National(this=self.ctx.table),
                'schema_name': self._get_schema_name(),
            })


        def _get_schema_name(self):
            return exp.National(this=self.ctx.schema) if self.ctx.schema else exp.func(name='schema_name')


        def _to_literal(self, value):
            if isinstance(value, str):
                return exp.National(this=value)
            return super()._to_literal(value)



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('int', 'bigint', 'smallint', 'bit')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='tsql',
        )

        def ast(self):
            if self.ctx.schema:
                schema_name = exp.National(this=self.ctx.schema)
            else:
                schema_name = exp.func(name='schema_name')

            return super().ast(params={
                'schema_name': schema_name,
                'table_name': exp.National(this=self.ctx.table),
                'column_name': exp.National(this=self.ctx.column),
            })



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('float', 'real', 'decimal', 'dec', 'numeric', 'money', 'smallmoney')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='tsql',
        )

        def ast(self):
            if self.ctx.schema:
                schema_name = exp.National(this=self.ctx.schema)
            else:
                schema_name = exp.func(name='schema_name')

            return super().ast(params={
                'schema_name': schema_name,
                'table_name': exp.National(this=self.ctx.table),
                'column_name': exp.National(this=self.ctx.column),
            })



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='tsql',
        )

        def ast(self):
            if self.ctx.schema:
                schema_name = exp.National(this=self.ctx.schema)
            else:
                schema_name = exp.func(name='schema_name')

            return super().ast(params={
                'schema_name': schema_name,
                'table_name': exp.National(this=self.ctx.table),
                'column_name': exp.National(this=self.ctx.column),
            })



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql='''
                select lower(data_type) in ('binary', 'varbinary', 'image')
                from information_schema.columns
                where table_schema = @schema_name and table_name = @table_name and column_name = @column_name
            ''',
            dialect='tsql',
        )

        def ast(self):
            if self.ctx.schema:
                schema_name = exp.National(this=self.ctx.schema)
            else:
                schema_name = exp.func(name='schema_name')

            return super().ast(params={
                'schema_name': schema_name,
                'table_name': exp.National(this=self.ctx.table),
                'column_name': exp.National(this=self.ctx.column),
            })



    class QueryRowsAreAscii(DBMS.QueryRowsAreAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select max(patindex(
                    '%[^' + char(0x0) + '-' + char(0x7f) + ']%' COLLATE Latin1_General_BIN,
                    @column
                )) = 0
                from @table
            ''',
            dialect='tsql'
        )


        def ast(self):
            # sqlglot incorrectly converts char to chr, which is not supported by mssql
            # so we convert it an anonymous call to char()
            ast = super().ast()
            for node in ast.find_all(exp.Chr):
                node.replace(
                    exp.Anonymous(
                        this='char',
                        expressions=node.expressions,
                    )
                )
            return ast



    class QueryRowIsAscii(DBMS.QueryRowIsAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select patindex(
                    '%[^' + char(0x0) + '-' + char(0x7f) + ']%' COLLATE Latin1_General_BIN,
                    @column
                ) = 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='tsql'
        )


        def ast(self):
            # sqlglot incorrectly converts char to chr, which is not supported by mssql
            # so we convert it an anonymous call to char()
            ast = super().ast()
            for node in ast.find_all(exp.Chr):
                node.replace(
                    exp.Anonymous(
                        this='char',
                        expressions=node.expressions,
                    )
                )
            return ast



    class QueryCharIsAscii(DBMS.QueryCharIsAscii):
        AST_TEMPLATE = parse_one(
            sql='''
                select patindex(
                    '%[^' + char(0x0) + '-' + char(0x7f) + ']%' COLLATE Latin1_General_BIN,
                    substr(@column, @char_offset, 1)
                ) = 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='tsql'
        )


        def ast(self):
            # sqlglot incorrectly converts char to chr, which is not supported by mssql
            # so we convert it an anonymous call to char()
            ast = super().ast()
            for node in ast.find_all(exp.Chr):
                node.replace(
                    exp.Anonymous(
                        this='char',
                        expressions=node.expressions,
                    )
                )
            return ast



    class QueryTextCharLt(DBMS.QueryTextCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select length(@column) > @str_length and unicode(substr(@column, @char_offset, 1)) < @n
                from @table limit 1 offset @row_idx
            ''',
            dialect='tsql',
        )



    # # Template Filters
    # @staticmethod
    # def sql_cast(s, type):
    #     allowed_types = ['int', 'float', 'text', 'blob']
    #     assert type in allowed_types, f'Type "{type}" not allowed. Use one of {allowed_types}'

    #     translate = {
    #         'int': 'int',
    #         'float': 'float',
    #         'text': 'nvarchar(max)',
    #         'blob': 'varbinary',
    #     }
    #     return f'cast({s} as {translate[type]})'

    # @staticmethod
    # def sql_lit(s):
    #     if not s.isascii() or not s.isprintable() or any(c in s for c in "?:'"):
    #         hex_str = s.encode('utf-16').hex()
    #         return f'convert(nvarchar(MAX), 0x{hex_str})'
    #     return f"'{s}'"

    # @staticmethod
    # def sql_len(s):
    #     return f'len({s})'

    # @staticmethod
    # def sql_char_at(s, i):
    #     return f'substring({s}, {i + 1}, 1)'

    # @staticmethod
    # def sql_in_str(s, string):
    #     return f'charindex({s},{string} COLLATE Latin1_General_CS_AS)'

    # @classmethod
    # def sql_in_list(cls, s, values):
    #     if str not in [type(v) for v in values]:
    #         return f'{s} in ({",".join(values)})'

    #     values = [cls.sql_lit(v) if type(v) is str else str(v) for v in values]
    #     return f'{s} COLLATE Latin1_General_CS_AS in ({",".join(values)})'


    # @staticmethod
    # def sql_is_ascii(s):
    #     # MSSQL does not have native "isascii" function. As a workaround we try to look for
    #     # non-ascii characters with "%[^\x00-0x7f]%" patterns.
    #     return f"CASE WHEN patindex('%[^'+char(0x00)+'-'+char(0x7f)+']%' COLLATE Latin1_General_BIN,{s}) = 0 THEN 1 ELSE 0 END"

    # @staticmethod
    # def sql_to_text(s):
    #     return f'convert(nvarchar(MAX), {s})'


    # # Queries
    # def q_column_type_in_str_set(self, ctx, types):
    #     query = self.jj_mssql.get_template('column_type_in_str_set.jinja').render(ctx=ctx, types=types)
    #     return self.normalize(query)

    # def q_column_is_int(self, ctx):
    #     types = ['int', 'bigint', 'smallint', 'bit']
    #     return self.q_column_type_in_str_set(ctx, types=types)

    # def q_column_is_float(self, ctx):
    #     types = ['float', 'real', 'decimal', 'dec', 'numeric', 'money', 'smallmoney']
    #     return self.q_column_type_in_str_set(ctx, types=types)

    # def q_column_is_text(self, ctx):
    #     types = ['char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext']
    #     return self.q_column_type_in_str_set(ctx, types=types)

    # def q_column_is_blob(self, ctx):
    #     return self.q_column_type_in_str_set(ctx, types=['binary', 'varbinary', 'image'])

    # def q_rows_have_null(self, ctx):
    #     query = self.jj_mssql.get_template('rows_have_null.jinja').render(ctx=ctx)
    #     return self.normalize(query)

    # def q_row_is_null(self, ctx):
    #     query = self.jj_mssql.get_template('row_is_null.jinja').render(ctx=ctx)
    #     return self.normalize(query)

    # def q_rows_are_positive(self, ctx):
    #     query = self.jj_mssql.get_template('rows_are_positive.jinja').render(ctx=ctx)
    #     return self.normalize(query)

    # def q_rows_are_ascii(self, ctx):
    #     query = self.jj_mssql.get_template('rows_are_ascii.jinja').render(ctx=ctx)
    #     return self.normalize(query)

    # def q_row_is_ascii(self, ctx):
    #     query = self.jj_mssql.get_template('row_is_ascii.jinja').render(ctx=ctx)
    #     return self.normalize(query)

    # def q_char_is_ascii(self, ctx):
    #     query = self.jj_mssql.get_template('char_is_ascii.jinja').render(ctx=ctx)
    #     return self.normalize(query)

    # def q_rows_count_lt(self, ctx, n):
    #     query = self.jj_mssql.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
    #     return self.normalize(query)

    # def q_char_in_set(self, ctx, values):
    #     has_eos = EOS in values
    #     values = ''.join([v for v in values if v != EOS])
    #     query = self.jj_mssql.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
    #     return self.normalize(query)

    # def q_char_lt(self, ctx, n):
    #     query = self.jj_mssql.get_template('char_lt.jinja').render(ctx=ctx, n=n)
    #     return self.normalize(query)

    # def q_value_in_list(self, ctx, values):
    #     query = self.jj_mssql.get_template('value_in_list.jinja').render(ctx=ctx, values=values)
    #     return self.normalize(query)

    # def q_int_lt(self, ctx, n):
    #     query = self.jj_mssql.get_template('int_lt.jinja').render(ctx=ctx, n=n)
    #     return self.normalize(query)

    # def q_int_eq(self, ctx, n):
    #     query = self.jj_mssql.get_template('int_eq.jinja').render(ctx=ctx, n=n)
    #     return self.normalize(query)

    # def q_float_char_in_set(self, ctx, values):
    #     has_eos = EOS in values
    #     values = ''.join([v for v in values if v != EOS])
    #     query = self.jj_mssql.get_template('float_char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
    #     return self.normalize(query)

    # def q_byte_lt(self, ctx, n):
    #     query = self.jj_mssql.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
    #     return self.normalize(query)