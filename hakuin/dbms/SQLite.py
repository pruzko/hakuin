from sqlglot import parse_one, exp

from .DBMS import DBMS, QueryTemplate



class SQLite(DBMS):
    DIALECT = 'sqlite'

    AST_ISASCII = parse_one(
        "not @query glob cast(x'2a5b5e012d7f5d2a' as text)",
        dialect='sqlite',
    )
    '''SQLite does not have native isascii() function. As a workaround, we try to look for
    non-ascii characters with "*[^\\x01-\\x7f]*" glob patterns. The pattern is hex-encoded
    because SQLite does not support special characters in string literals.
    '''

    AST_TABLE_NAMES_FILTER = parse_one(
        "schema=@schema_name and type='table' and name != 'sqlite_schema'",
        dialect='sqlite',
    )

    AST_COLUMN_TYPE_IN_LIST = parse_one(
        'select lower(type) in (@types) from pragma_table_info(@table_name) where name=@column_name',
        dialect='sqlite',
    )


    def template_resolve_target(self, template):
        if template.ctx.target == 'schema_names':
            return self.template_resolve_target_schema_names(template=template)
        if template.ctx.target == 'table_names':
            return self.template_resolve_target_table_names(template=template)
        if template.ctx.target == 'column_names':
            return self.template_resolve_target_column_names(template=template)


    def template_resolve_target_schema_names(self, template):
        template.resolve_params(params={
            'table': exp.func('pragma_database_list'),
            'column': exp.to_column('name'),
        })


    def template_resolve_target_table_names(self, template):
        table_names_filter = self.AST_TABLE_NAMES_FILTER.copy()
        orig_where_cond = template.ast.args.get('where')
        if orig_where_cond:
            template.ast.where(table_names_filter and orig_where_cond.this, copy=False)
        else:
            template.ast.where(table_names_filter, copy=False)

        template.resolve_params(params={
            'table': exp.func('pragma_table_list'),    
            'column': exp.to_column('name'),
            'schema_name': template.ctx.schema or 'main',
        })


    def template_resolve_target_column_names(self, template):
        template.resolve_params(params={
            'table': exp.func('pragma_table_info', exp.Literal.string(template.ctx.table)),
            'column': exp.to_column('name'),
        })


    def ast_unicode(self, ctx, func):
        func.set('this', exp.to_identifier('unicode'))
        return func


    def ast_instr(self, ctx, func):
        func.set('this', exp.to_identifier('instr'))
        return func


    def ast_isascii(self, ctx, func):
        ast = self.AST_ISASCII.copy()
        ast.find(exp.Glob).set('this', func.expressions[0])
        return ast


    def ast_column_type_in_list(self, ctx, types):
        return QueryTemplate(
            dbms=self,
            ctx=ctx,
            ast=self.AST_COLUMN_TYPE_IN_LIST.copy(),
        ).resolve(params={
            'types': types,
            'table_name': ctx.table,
            'column_name': ctx.column,
        })



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        def ast(self):
            return self.dbms.ast_column_type_in_list(self.ctx, types=['integer'])



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        def ast(self):
            return self.dbms.ast_column_type_in_list(self.ctx, types=['float', 'real'])



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        def ast(self):
            return self.dbms.ast_column_type_in_list(self.ctx, types=['text'])



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        def ast(self):
            return self.dbms.ast_column_type_in_list(self.ctx, types=['blob'])
