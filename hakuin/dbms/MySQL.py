from sqlglot import parse_one, exp

from .DBMS import DBMS



class MySQL(DBMS):
    DIALECT = 'mysql'

    AST_ISASCII = parse_one(
        sql="not regexp_like(@query, '[^[:ascii:]]')",
        dialect='mysql',
    )
    AST_UNICODE = parse_one(
        sql='ord(cast(@query as char character set utf32))',
        dialect='mysql',
    )
    AST_TABLE_NAMES_FILTER = parse_one(
        sql="table_schema=@schema_name and table_type='BASE TABLE'",
        dialect='mysql',
    )
    AST_COLUMN_NAMES_FILTER = parse_one(
        sql="table_schema=@schema_name and table_name=@table_name",
        dialect='mysql',
    )
    AST_COLUMN_TYPE_TEMPLATE = parse_one(
        sql=(
            'select @select_query from information_schema.columns '
            'where table_schema = @schema_name and table_name = @table_name and column_name = @column_name'
        ),
        dialect='mysql',
    )
    AST_COLUMN_TYPE_SELECT_INT = parse_one(
        sql=r"regexp_like(data_type, 'int') and not regexp_like(data_type, 'point')",
        dialect='mysql',
    )
    AST_COLUMN_TYPE_SELECT_FLOAT = parse_one(
        sql="data_type in ('decimal', 'numeric', 'float', 'double')",
        dialect='mysql',
    )
    AST_COLUMN_TYPE_SELECT_TEXT = parse_one(
        sql="regexp_like(data_type, 'text') or data_type in ('char', 'varchar', 'linestring', 'multilinestring', 'json')",
        dialect='mysql',
    )
    AST_COLUMN_TYPE_SELECT_BLOB = parse_one(
        sql="regexp_like(data_type, 'blob') or data_type in ('binary', 'varbinary')",
        dialect='mysql',
    )


    def __init__(self):
        pass


    def template_resolve_target(self, template):
        if template.ctx.target == 'schema_names':
            return self.template_resolve_target_schema_names(template=template)
        if template.ctx.target == 'table_names':
            return self.template_resolve_target_table_names(template=template)
        if template.ctx.target == 'column_names':
            return self.template_resolve_target_column_names(template=template)


    def template_resolve_target_schema_names(self, template):
        template.resolve_params(params={
            'table': exp.to_table('schemata', db='information_schema'),
            'column': exp.to_column('schema_name'),
        })


    def template_resolve_target_table_names(self, template):
        table_names_filter = self.AST_TABLE_NAMES_FILTER.copy()
        orig_where_cond = template.ast.args.get('where')
        if orig_where_cond:
            template.ast.where(table_names_filter and orig_where_cond.this, copy=False)
        else:
            template.ast.where(table_names_filter, copy=False)

        schema_name = exp.Literal.string(template.ctx.schema) if template.ctx.schema else exp.func('schema')
        template.resolve_params(params={
            'table': exp.to_table('tables', db='information_schema'),
            'column': exp.to_column('table_name'),
            'schema_name': schema_name,
        })


    def template_resolve_target_column_names(self, template):
        # TODO refactor, should be tempalte.assure_where(), @schema/table/column_name should be handled by Tempalte, + dbms.ast_current_schema()
        table_names_filter = self.AST_COLUMN_NAMES_FILTER.copy()
        orig_where_cond = template.ast.args.get('where')
        if orig_where_cond:
            template.ast.where(table_names_filter and orig_where_cond.this, copy=False)
        else:
            template.ast.where(table_names_filter, copy=False)

        schema_name = exp.Literal.string(template.ctx.schema) if template.ctx.schema else exp.func('schema')
        template.resolve_params(params={
            'table': exp.to_table('columns', db='information_schema'),
            'column': exp.to_column('column_name'),
            'table_name': exp.Literal.string(template.ctx.table),
            'schema_name': schema_name,
        })


    def query_column_type_is_int(self, ctx):
        return self.query_column_type_is_type(ctx, type='int')


    def query_column_type_is_float(self, ctx):
        return self.query_column_type_is_type(ctx, type='float')


    def query_column_type_is_text(self, ctx):
        return self.query_column_type_is_type(ctx, type='text')


    def query_column_type_is_blob(self, ctx):
        return self.query_column_type_is_type(ctx, type='blob')


    def query_column_type_is_type(self, ctx, type):
        select = {
            'int': self.AST_COLUMN_TYPE_SELECT_INT,
            'float': self.AST_COLUMN_TYPE_SELECT_FLOAT,
            'text': self.AST_COLUMN_TYPE_SELECT_TEXT,
            'blob': self.AST_COLUMN_TYPE_SELECT_BLOB,
        }
        return DBMS.QueryTemplate(
            dbms=self,
            ctx=ctx,
            ast=self.AST_COLUMN_TYPE_TEMPLATE.copy(),
        ).resolve(params={
            'select_query': select[type].copy(),
            'schema_name': ctx.schema or exp.Anonymous(this='schema'),
            'table_name': ctx.table,
            'column_name': ctx.column,
        })


    def ast_char_length(self, ctx, func):
        return exp.func('char_length', func.expressions[0])


    def ast_unicode(self, ctx, func):
        ast = self.AST_UNICODE.copy()
        ast.find(exp.Cast).set('this', func.expressions[0])
        return ast


    def ast_instr(self, ctx, func):
        # TODO case-sensitive
        # TODO case-sensitive guessing
        func.set('this', exp.to_identifier('instr'))
        return func


    def ast_isascii(self, ctx, func):
        ast = self.AST_ISASCII.copy()
        ast.find(exp.RegexpLike).set('this', func.expressions[0])
        return ast


















######################################################################################
######################################################################################
######################################################################################
######################################################################################
######################################################################################
# import os

# import jinja2

# from hakuin.utils import DIR_QUERY_TEMPLATES, BYTE_MAX, EOS
# from .DBMS import DBMS



# class MySQL(DBMS):
#     def __init__(self):
#         super().__init__()
#         self.jj_mysql = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(DIR_QUERY_TEMPLATES, 'MySQL')))
#         self.jj_mysql.filters = self.jj.filters
#         self.jj_mysql.filters['sql_float_dec_str'] = self.sql_float_dec_str
#         self.jj_mysql.filters['sql_reverse'] = self.sql_reverse


#     # Template Filters
#     @staticmethod
#     def sql_ident(s):
#         if s is None:
#             return None

#         if DBMS._RE_ESCAPE.match(s):
#             return s

#         assert '`' not in s, f'Cannot escape "{s}"'
#         return f'`{s}`'

#     @staticmethod
#     def sql_cast(s, type):
#         assert type in self.BASIC_TYPES, f'Type "{type}" not supported, use one of {self.BASIC_TYPES}'
#         cast_dict = {
#             'int': 'signed',
#             'float': 'double',
#             'text': 'char',
#             'blob': 'binary',
#         }
#         return f'cast({s} as {cast_dict[type]})'

#     @staticmethod
#     def sql_len(s):
#         return f'char_length({s})'

#     @staticmethod
#     def sql_to_unicode(s):
#         return f'ord(convert({s} using utf32))'

#     @staticmethod
#     def sql_in_str(s, string):
#         return f'locate({s}, BINARY {string})'

#     @classmethod
#     def sql_in_list(cls, s, values):
#         if str not in [type(v) for v in values]:
#             return f'{s} in ({",".join(values)})'

#         values = [cls.sql_lit(v) if type(v) is str else str(v) for v in values]
#         return f'{s} in (BINARY {",".join(values)})'

#     @staticmethod
#     def sql_is_ascii(s):
#         return f'({s} = convert({s} using ASCII))'

#     @staticmethod
#     def sql_float_dec_str(s):
#         return f"substr({s}, locate('.', {s}) + 1)"

#     @staticmethod
#     def sql_reverse(s):
#         return f'reverse({s})'


#     # Queries
#     def q_column_type_in_str_set(self, ctx, types):
#         query = self.jj_mysql.get_template('column_type_in_str_set.jinja').render(ctx=ctx, types=types)
#         return self.normalize(query)

#     def q_column_is_int(self, ctx):
#         query = self.jj_mysql.get_template('column_is_int.jinja').render(ctx=ctx)
#         return self.normalize(query)

#     def q_column_is_float(self, ctx):
#         return self.q_column_type_in_str_set(ctx, types=['decimal', 'numeric', 'float', 'double'])

#     def q_column_is_text(self, ctx):
#         # *text* types are covered in the jinja template
#         types = ['char', 'varchar', 'linestring', 'multilinestring', 'json']
#         query = self.jj_mysql.get_template('column_is_text.jinja').render(ctx=ctx, types=types)
#         return self.normalize(query)

#     def q_column_is_blob(self, ctx):
#         # *blob* types are covered in the jinja template
#         types = ['binary', 'varbinary']
#         query = self.jj_mysql.get_template('column_is_blob.jinja').render(ctx=ctx, types=types)
#         return self.normalize(query)

#     def q_rows_have_null(self, ctx):
#         query = self.jj_mysql.get_template('rows_have_null.jinja').render(ctx=ctx)
#         return self.normalize(query)

#     def q_row_is_null(self, ctx):
#         query = self.jj_mysql.get_template('row_is_null.jinja').render(ctx=ctx)
#         return self.normalize(query)

#     def q_rows_are_positive(self, ctx):
#         query = self.jj_mysql.get_template('rows_are_positive.jinja').render(ctx=ctx)
#         return self.normalize(query)

#     def q_rows_are_ascii(self, ctx):
#         query = self.jj_mysql.get_template('rows_are_ascii.jinja').render(ctx=ctx)
#         return self.normalize(query)

#     def q_row_is_ascii(self, ctx):
#         query = self.jj_mysql.get_template('row_is_ascii.jinja').render(ctx=ctx)
#         return self.normalize(query)

#     def q_char_is_ascii(self, ctx):
#         query = self.jj_mysql.get_template('char_is_ascii.jinja').render(ctx=ctx)
#         return self.normalize(query)

#     def q_rows_count_lt(self, ctx, n):
#         query = self.jj_mysql.get_template('rows_count_lt.jinja').render(ctx=ctx, n=n)
#         return self.normalize(query)

#     def q_char_in_set(self, ctx, values):
#         has_eos = EOS in values
#         values = ''.join([v for v in values if v != EOS])
#         query = self.jj_mysql.get_template('char_in_set.jinja').render(ctx=ctx, values=values, has_eos=has_eos)
#         return self.normalize(query)

#     def q_char_lt(self, ctx, n):
#         query = self.jj_mysql.get_template('char_lt.jinja').render(ctx=ctx, n=n)
#         return self.normalize(query)

#     def q_value_in_list(self, ctx, values):
#         query = self.jj_mysql.get_template('value_in_list.jinja').render(ctx=ctx, values=values)
#         return self.normalize(query)

#     def q_int_lt(self, ctx, n):
#         query = self.jj_mysql.get_template('int_lt.jinja').render(ctx=ctx, n=n)
#         return self.normalize(query)

#     def q_int_eq(self, ctx, n):
#         query = self.jj_mysql.get_template('int_eq.jinja').render(ctx=ctx, n=n)
#         return self.normalize(query)

#     def q_float_char_in_set(self, ctx, values):
#         return self.q_char_in_set(ctx, values)

#     def q_float_dec_rev_lt(self, ctx, n):
#         query = self.jj_mysql.get_template('float_dec_rev_lt.jinja').render(ctx=ctx, n=n)
#         return self.normalize(query)

#     def q_byte_lt(self, ctx, n):
#         query = self.jj_mysql.get_template('byte_lt.jinja').render(ctx=ctx, n=n)
#         return self.normalize(query)
