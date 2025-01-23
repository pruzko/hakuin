from sqlglot import parse_one, exp

from .dbms import DBMS



class MySQL(DBMS):
    DIALECT = 'mysql'


    def target_schema_names(self, query, ast, ctx):
        query.table = exp.to_table('schemata', db='information_schema')
        query.column = exp.to_column('schema_name')
        return ast


    def target_table_names(self, query, ast, ctx):
        query.table = exp.to_table('tables', db='information_schema')
        query.column = exp.to_column('table_name')

        where_filter = parse_one(
            sql="table_schema=@schema_name and table_type='BASE TABLE'",
            dialect='mysql',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
        })
        # TODO clean after sqlglot
        self.prepend_where(ast=ast, condition=where_filter)

        return ast


    def target_column_names(self, query, ast, ctx):
        query.table = exp.to_table('columns', db='information_schema')
        query.column = exp.to_column('column_name')

        where_filter = parse_one(
            sql='table_schema=@schema_name and table_name=@table_name',
            dialect='mysql',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
            'table_name': self.literal_text(ctx.table),
        })
        # TODO clean after sqlglot
        self.prepend_where(ast=ast, condition=where_filter)

        return ast            


    def target_column_type(self, query, ast, ctx):
        query.table = exp.to_table('columns', db='information_schema')
        query.column = exp.to_column('data_type')

        where_filter = parse_one(
            sql='''
                table_schema=@schema_name and table_name=@table_name and
                column_name=@column_name
            ''',
            dialect='mysql',
        )
        where_filter = query.resolve_params(ast=where_filter, ctx=ctx, params={
            'schema_name': self.get_schema_name(ctx),
            'table_name': self.literal_text(ctx.table),
            'column_name': self.literal_text(ctx.column),
        })
        # TODO clean after sqlglot
        self.prepend_where(ast=ast, condition=where_filter)

        return ast


    def get_schema_name(self, ctx):
        # TODO this should be in DBMS after sqlglot
        if ctx.schema:
            return self.literal_text(ctx.schema)
        return exp.func('schema')



    class QueryColumnTypeIsInt(DBMS.QueryColumnTypeIsInt):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(column, 'int') and not regexp_like(column, 'point')
                from table
            ''',
            dialect='mysql',
        )



    class QueryColumnTypeIsFloat(DBMS.QueryColumnTypeIsFloat):
        AST_TEMPLATE = parse_one(
            sql="select column in ('decimal', 'numeric', 'float', 'double') from table",
            dialect='mysql',
        )



    class QueryColumnTypeIsText(DBMS.QueryColumnTypeIsText):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(column, 'text') or
                    data_type in ('char', 'varchar', 'linestring', 'multilinestring', 'json')
                from table
            ''',
            dialect='mysql',
        )



    class QueryColumnTypeIsBlob(DBMS.QueryColumnTypeIsBlob):
        AST_TEMPLATE = parse_one(
            sql='''
                select regexp_like(column, 'blob') or
                    data_type in ('binary', 'varbinary')
                from table
            ''',
            dialect='mysql',
        )



    class QueryValueInList(DBMS.QueryValueInList):
        AST_TEMPLATE = parse_one(
            sql='select binary(column) in (@values) from @table limit 1 offset @row_idx',
            dialect='mysql',
        )



    class QueryTextCharInString(DBMS.QueryTextCharInString):
        AST_TEMPLATE = parse_one(
            sql='''
                select instr(@values_str, binary(substr(column, @char_offset, 1))) > 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )



    class QueryBlobCharInString(DBMS.QueryBlobCharInString):
        AST_TEMPLATE = parse_one(
            sql='''
                select instr(@values_str, binary(substr(column, @char_offset, 1))) > 0
                from @table limit 1 offset @row_idx
            ''',
            dialect='mysql',
        )
