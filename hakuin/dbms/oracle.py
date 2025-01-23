from sqlglot import parse_one, exp

from hakuin.utils import EOS

from .dbms import DBMS



class Oracle(DBMS):
    DIALECT = 'oracle'


    def target_schema_names(self, query, ast, ctx):
        query.table = exp.to_table('all_users')
        query.column = exp.to_column('username')
        return ast


    def target_table_names(self, query, ast, ctx):
        query.table = exp.to_table('all_tables')
        query.column = exp.to_column('table_name')

        where_filter = parse_one(
            sql='owner=@schema_name',
            dialect='oracle',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
        })
        # TODO clean after sqlglot
        self.prepend_where(ast=ast, condition=where_filter)

        return ast


    def target_column_names(self, query, ast, ctx):
        query.table = exp.to_table('all_tab_columns')
        query.column = exp.to_column('column_name')

        where_filter = parse_one(
            sql='owner=@schema_name and table_name=@table_name',
            dialect='oracle',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
            # TODO remove upper() after case sensitivity is fixed
            'table_name': self.literal_text(ctx.table.upper()),
        })
        # TODO clean after sqlglot
        self.prepend_where(ast=ast, condition=where_filter)

        return ast            


    def target_column_type(self, query, ast, ctx):
        query.table = exp.to_table('all_tab_columns')
        query.column = exp.to_column('data_type')

        where_filter = parse_one(
            sql='owner=@schema_name and table_name=@table_name and column_name=@column_name',
            dialect='oracle',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
            # TODO remove upper() after case sensitivity is fixed
            'table_name': self.literal_text(ctx.table.upper()),
            'column_name': self.literal_text(ctx.column.upper()),
        })
        # TODO clean after sqlglot
        self.prepend_where(ast=ast, condition=where_filter)

        return ast


    def cast_to_text(self, query, ast, ctx):
        query.column = exp.cast(query.column, to='varchar(4000)')
        return ast


    def literal_blob(self, value):
        return exp.func('hextoraw', self.literal_text(value.hex()))


    def get_schema_name(self, ctx):
        # TODO this should be in DBMS after sqlglot
        if ctx.schema:
            # TODO remove upper() after case sensitivity is fixed
            return self.literal_text(ctx.schema.upper())
        return exp.to_identifier('user')



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
