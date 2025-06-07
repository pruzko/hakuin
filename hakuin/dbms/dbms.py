from sqlglot import exp, parse_one

from hakuin.collectors import StringContext
from hakuin.exceptions import ServerError
from hakuin.utils import BYTE_MAX, Symbol, snake_to_pascal_case



class DBMS:
    DIALECT = None


    def query_cls_lookup(self, name):
        '''Retrieves the target query class.

        Params:
            name (str): query name

        Returns:
            Type[Query]: query class
        '''
        if hasattr(self, name):
            query_cls = getattr(self, name)
            if issubclass(query_cls, self.Query):
                return query_cls

        raise AttributeError(f'Query "{name}" not found.')


    def wrap_ast(self, ast):
        return ast


    def target_column(self, query, ast, ctx):
        query.table = exp.table_(ctx.table, db=ctx.schema)
        query.column = exp.column(ctx.column)
        return ast


    def target_schema_names(self, query, ast, ctx):
        raise NotImplementedError

    
    def target_table_names(self, query, ast, ctx):
        raise NotImplementedError

    
    def target_column_names(self, query, ast, ctx):
        raise NotImplementedError


    def target_column_type(self, query, ast, ctx):
        raise NotImplementedError


    def cast_to_int(self, query, ast, ctx):
        query.column = exp.cast(query.column, to='INT')
        return ast


    def cast_to_text(self, query, ast, ctx):
        query.column = exp.cast(query.column, to='TEXT')
        return ast


    def literal_int(self, value):
        return exp.Literal.number(value)


    def literal_float(self, value):
        return exp.Literal.number(value)


    def literal_text(self, value):
        return exp.Literal.string(value)


    def literal_blob(self, value):
        return exp.HexString(this=value.hex())


    def force_server_error(self):
        return exp.Literal.number(1) / 0


    def get_schema_name(self, ctx):
        return self.literal_text(ctx.schema) if ctx.schema else exp.CurrentSchema()



    class Query:
        '''Base class for queries.

        Attributes:
            AST_TEMPLATE (sqlglot.Expression): query AST template
        '''
        AST_TEMPLATE = None


        def __init__(self, dbms):
            '''Constructor.

            Params:
                dbms (DBMS): DBMS instance used to render queries
            '''
            self.dbms = dbms
            self.table = None
            self.column = None


        def render(self, ctx):
            '''Renders the query as a string.

            Params:
                ctx (Context): collection context

            Returns:
                str: rendered query
            '''
            return self.ast(ctx).sql(dialect=self.dbms.DIALECT)


        def emulate(self, correct):
            '''Emulates the query and retrieves its result.

            Params:
                correct (value): correct value

            Returns:
                bool: query result
            '''
            raise NotImplementedError


        def ast(self, ctx, params=None):
            '''Generates the query AST.

            Params:
                ctx (Context): collection context
                params (dict): query parameters

            Returns:
                sqlglot.Expression: resolved AST
            '''
            ast = self.ast_template()
            ast = self.resolve_target(ast=ast, ctx=ctx)
            ast = self.resolve_casts(ast=ast, ctx=ctx)
            ast = self.resolve_params(ast=ast, ctx=ctx, params=params or {})
            return self.dbms.wrap_ast(ast=ast)


        def ast_template(self):
            '''Retrieves the AST template.

            Returns:
                sqlglot.Expression: AST template
            '''
            return self.AST_TEMPLATE.copy()


        def resolve_target(self, ast, ctx):
            '''Resolves the query target by invoking the corresponding dbms.target_* function.

            Params:
                ast (sqlglot.Expression): AST
                ctx (Context): collection context

            Returns:
                sqlglot.Expression: resolved AST
            '''
            resolver = f'target_{ctx.target or "column"}'
            if hasattr(self.dbms, resolver):
                return getattr(self.dbms, resolver)(query=self, ast=ast, ctx=ctx)

            raise NotImplementedError(f'{self.dbms} does not implement "{resolver}".')


        def resolve_casts(self, ast, ctx):
            '''Resolves the query casts by invoking the corresponding dbms.cast_to_* functions.

            Params:
                ast (sqlglot.Expression): AST
                ctx (Context): collection context

            Returns:
                sqlglot.Expression: resolved AST
            '''
            if not ctx.cast_to:
                return ast

            resolver = f'cast_to_{ctx.cast_to}'
            if hasattr(self.dbms, resolver):
                return getattr(self.dbms, resolver)(query=self, ast=ast, ctx=ctx)

            raise NotImplementedError(f'{self.dbms} does not implement "{resolver}".')


        def resolve_params(self, ast, ctx, params):
            '''Resolves the query casts by invoking the corresponding dbms.cast_to_* functions.

            Params:
                ast (sqlglot.Expression): AST
                ctx (Context): collection context
                params (dict): query parameters

            Returns:
                sqlglot.Expression: resolved AST
            '''
            params['row_idx'] = ctx.row_idx or 0

            if isinstance(ctx, StringContext):
                buffer_length = ctx.start_offset + len(ctx.buffer)
                params['buffer_length'] = params.get('buffer_length', buffer_length)
                params['char_offset'] = params.get('char_offset', buffer_length + 1)

            for name, param in params.items():
                if not isinstance(param, exp.Expression):
                    params[name] = self.resolve_literal(value=param)

            def tran(node):
                if isinstance(node, exp.Table) and node.name == 'table':
                    return self.table
                elif isinstance(node, exp.Column) and node.name == 'column':
                    return self.column
                elif isinstance(node, exp.Parameter) and node.name in params:
                    return params[node.name].transform(tran, copy=False)
                return node

            return ast.transform(tran, copy=False)


        def resolve_literal(self, value):
            '''Resolves a literal value as an AST.

            Params:
                value (value): literal value

            Returns:
                sqlglot.Expression: literal value AST
            '''
            type_map = {
                int: 'int',
                float: 'float',
                str: 'text',
                bytes: 'blob',
            }

            if isinstance(value, (list, tuple, set)):
                return exp.Tuple(expressions=[self.resolve_literal(item) for item in value])

            if type(value) not in type_map:
                raise TypeError(f'Literals of type "{type(value)}" are not supported.')

            resolver = f'literal_{type_map[type(value)]}'
            return getattr(self.dbms, resolver)(value)



    class QueryTernary(Query):
        '''Ternary query that combines two subqueries into one.'''
        AST_TERNARY = parse_one(
            sql='@cond1 or not(@cond2) and @error',
            dialect='sqlite',
        )


        def __init__(self, dbms, query1, query2):
            '''Constructor.

            Params:
                dbms (DBMS): DBMS instance used to render queries
                query1 (Query): first subquery
                query2 (Query): second subquery
            '''
            super().__init__(dbms)
            self.query1 = query1
            self.query2 = query2


        def ast(self, ctx):
            ast1 = self.query1.ast(ctx)
            ast2 = self.query2.ast(ctx)

            ternary_exp = self.resolve_params(ast=self.AST_TERNARY.copy(), ctx=ctx, params={
                'cond1': ast1.expressions[0],
                'cond2': ast2.expressions[0],
                'error': self.dbms.force_server_error(),
            })

            ast1.set('expressions', [ternary_exp])
            return ast1


        def emulate(self, correct):
            if self.query1.emulate(correct):
                return True
            elif self.query2.emulate(correct):
                return False
            else:
                raise ServerError




    class QueryColumnTypeIsInt(Query):
        def emulate(self, correct):
            return all([type(row) is int for row in correct if row is not None])



    class QueryColumnTypeIsFloat(Query):
        def emulate(self, correct):
            return all([type(row) is float for row in correct if row is not None])



    class QueryColumnTypeIsText(Query):
        def emulate(self, correct):
            return all([type(row) is str for row in correct if row is not None])



    class QueryColumnTypeIsBlob(Query):
        def emulate(self, correct):
            return all([type(row) is bytes for row in correct if row is not None])



    class QueryRowsCountLt(Query):
        AST_TEMPLATE = parse_one(
            sql='select count(*) < @n from table',
            dialect='sqlite',
        )

        def __init__(self, dbms, n):
            super().__init__(dbms)
            self.n = n


        def ast(self, ctx):
            return super().ast(ctx, params={'n': self.n})


        def emulate(self, correct):
            return correct < self.n



    class QueryColumnHasNull(Query):
        AST_TEMPLATE = parse_one(
            sql='select logical_or(column is null) from table',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return None in correct



    class QueryRowIsNull(Query):
        AST_TEMPLATE = parse_one(
            sql='select column is null from table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return correct is None



    class QueryColumnIsPositive(Query):
        AST_TEMPLATE = parse_one(
            sql='select min(column) >= 0 from table',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return all([row >= 0 for row in correct if row is not None])



    class QueryRowIsPositive(Query):
        AST_TEMPLATE = parse_one(
            sql='select column >= 0 from table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return correct >= 0



    class QueryColumnIsAscii(Query):
        AST_TEMPLATE = parse_one(
            sql='select logical_and(is_ascii(column)) from table',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return all([row.isascii() for row in correct if row is not None])



    class QueryRowIsAscii(Query):
        AST_TEMPLATE = parse_one(
            sql='select is_ascii(column) from table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return correct.isascii()



    class QueryCharIsAscii(Query):
        AST_TEMPLATE = parse_one(
            sql='''
                select is_ascii(substr(column, @char_offset, 1))
                from table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return True if type(correct) is Symbol else correct.isascii()



    class QueryValueInList(Query):
        AST_TEMPLATE = parse_one(
            sql='select column in @values from table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def __init__(self, dbms, values):
            super().__init__(dbms)
            self.values = values


        def emulate(self, correct):
            return correct in self.values


        def ast(self, ctx):
            return super().ast(ctx, params={'values': self.values})



    class QueryIntLt(Query):
        AST_TEMPLATE = parse_one(
            sql='select column < @n from table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def __init__(self, dbms, n):
            super().__init__(dbms)
            self.n = n 


        def emulate(self, correct):
            return correct < self.n


        def ast(self, ctx):
            return super().ast(ctx, params={'n': self.n})



    class QueryCharInString(Query):
        AST_TEMPLATE = parse_one(
            sql='''
                select instr(@values_str, substr(column, @char_offset, 1)) > 0
                from table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )
        AST_HAS_EOS = parse_one(
            sql='char_length(column) = @buffer_length',
            dialect='sqlite',
        )
        AST_NO_EOS = parse_one(
            sql='char_length(column) > @buffer_length',
            dialect='sqlite',
        )


        def emulate(self, correct):
            if correct == Symbol.EOS:
                return self.has_eos
            return correct in self.values_str


        def ast_template(self):
            ast = super().ast_template()
            if self.has_eos:
                select_exp = self.AST_HAS_EOS.copy().or_(ast.expressions[0])
            else:
                select_exp = self.AST_NO_EOS.copy().and_(ast.expressions[0])

            if not self.values_str:
                select_exp = select_exp.this

            ast.set('expressions', [select_exp])
            return ast


        def ast(self, ctx):
            return super().ast(ctx, params={'values_str': self.values_str})



    class QueryTextCharInString(QueryCharInString):
        def __init__(self, dbms, values):
            super().__init__(dbms)
            self.has_eos = Symbol.EOS in values
            self.values_str = ''.join([v for v in values if v != Symbol.EOS])



    class QueryBlobCharInString(QueryCharInString):
        def __init__(self, dbms, values):
            super().__init__(dbms)
            self.has_eos = Symbol.EOS in values
            self.values_str = b''.join([v for v in values if v != Symbol.EOS])



    class QueryCharLt(Query):
        def __init__(self, dbms, n):
            super().__init__(dbms)
            self.n = n



    class QueryTextCharLt(QueryCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select char_length(column) > @buffer_length
                    and unicode(substr(column, @char_offset, 1)) < @n
                from table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )


        def emulate(self, correct):
            if correct == Symbol.EOS:
                return False
            return ord(correct) < self.n


        def ast(self, ctx):
            return super().ast(ctx, params={'n': self.n})



    class QueryBlobCharLt(QueryCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select char_length(column) > @buffer_length
                    and substr(column, @char_offset, 1) < @byte
                from table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )


        def emulate(self, correct):
            if correct == Symbol.EOS:
                return False
            return correct[0] < self.n


        def ast_template(self):
            ast = super().ast_template()
            if self.n > BYTE_MAX:
                and_exp = ast.find(exp.And)
                and_exp.replace(and_exp.this)
            return ast


        def ast(self, ctx):
            byte = bytes([min(self.n, BYTE_MAX)])
            return super().ast(ctx, params={'byte': byte})
