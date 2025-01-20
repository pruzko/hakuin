from abc import ABCMeta, abstractmethod
from collections.abc import Iterable

from sqlglot import exp, parse_one

from hakuin.collectors import StringContext
from hakuin.utils import BYTE_MAX, EOS



class DBMS(metaclass=ABCMeta):
    DIALECT = None


    class QueryUtils(metaclass=ABCMeta):
        @classmethod
        def resolve_query(cls, query, ast, ctx, params):
            ast = cls.resolve_target(query=query, ast=ast, ctx=ctx)
            ast = cls.resolve_casts(query=query, ast=ast, ctx=ctx)
            return cls.resolve_params(query=query, ast=ast, ctx=ctx, params=params or {})


        @classmethod
        def resolve_target(cls, query, ast, ctx):
            if ctx.target == 'schema_names':
                return cls.target_schema_names(query=query, ast=ast, ctx=ctx)
            elif ctx.target == 'table_names':
                return cls.target_table_names(query=query, ast=ast, ctx=ctx)
            elif ctx.target == 'column_names':
                return cls.target_column_names(query=query, ast=ast, ctx=ctx)
            elif ctx.target == 'column_type':
                return cls.target_column_type(query=query, ast=ast, ctx=ctx)
            else:
                return cls.target_column(query=query, ast=ast, ctx=ctx)


        @classmethod
        def target_column(cls, query, ast, ctx):
            query.table = exp.table_(ctx.table, db=ctx.schema)
            query.column = exp.column(ctx.column)
            return ast


        @classmethod
        @abstractmethod
        def target_schema_names(cls, query, ast, ctx):
            raise NotImplementedError

        
        @classmethod
        @abstractmethod
        def target_table_names(cls, query, ast, ctx):
            raise NotImplementedError

        
        @classmethod
        @abstractmethod
        def target_column_names(cls, query, ast, ctx):
            raise NotImplementedError


        @classmethod
        @abstractmethod
        def target_column_type(cls, query, ast, ctx):
            raise NotImplementedError


        @classmethod
        def resolve_casts(cls, query, ast, ctx):
            if ctx.cast_to == 'int':
                return cls.cast_to_int(query=query, ast=ast, ctx=ctx)
            if ctx.cast_to == 'text':
                return cls.cast_to_text(query=query, ast=ast, ctx=ctx)
            return ast


        @classmethod
        def cast_to_int(cls, query, ast, ctx):
            query.column = exp.cast(query.column, to='INT')
            return ast


        @classmethod
        def cast_to_text(cls, query, ast, ctx):
            query.column = exp.cast(query.column, to='TEXT')
            return ast


        @classmethod
        def resolve_params(cls, query, ast, ctx, params):
            params = cls.process_params(ctx=ctx, params=params)
            return cls._resolve_params(query=query, ast=ast, params=params)


        @classmethod
        def _resolve_params(cls, query, ast, params):
            return ast.transform(cls._transform_params, query, params, copy=False)


        @classmethod
        def _transform_params(cls, node, query, params):
            if isinstance(node, exp.Column) and node.name.lower() == 'column':
                return query.column
            if isinstance(node, exp.Table) and node.name.lower() == 'table':
                return query.table
            if isinstance(node, exp.Parameter) and node.name in params:
                return cls._resolve_params(query=query, ast=params[node.name], params=params)
            return node


        @classmethod
        def process_params(cls, ctx, params):
            params['row_idx'] = ctx.row_idx or 0

            if isinstance(ctx, StringContext):
                buffer_length = ctx.start_offset + len(ctx.buffer)
                params['buffer_length'] = params.get('buffer_length', buffer_length)
                params['char_offset'] = params.get('char_offset', buffer_length + 1)

            return {
                k: v if isinstance(v, exp.Expression) else cls.to_literal(v)
                for k, v in params.items()
            }


        @classmethod
        def to_literal(cls, value):
            if isinstance(value, (int, float)):
                return exp.Literal.number(value)
            elif isinstance(value, str):
                return exp.Literal.string(value)
            elif isinstance(value, bytes):
                return exp.HexString(this=value.hex())
            elif isinstance(value, Iterable):
                return exp.Tuple(expressions=[cls.to_literal(item) for item in value])
            raise TypeError(f'Type not supported: {type(value)}')


        @staticmethod
        def add_where(ast, condition):
            original_where = ast.args.get('where')
            if original_where:
                condition = exp.and_(condition, original_where.this)
            ast.where(condition, copy=False)



    class Query(metaclass=ABCMeta):
        '''Abstract class for queries.'''
        AST_TEMPLATE = None


        def __init__(self, dbms):
            '''Constructor.

            Params:
                dbms (DBMS): DBMS instance used to render queries
            '''
            self.dbms = dbms
            self._table = None
            self._column = None


        @abstractmethod
        def emulate(self, correct):
            '''Emulates the query and retrieves the its result.

            Params:
                correct (value): correct value

            Returns:
                bool: query result
            '''
            raise NotImplementedError


        def ast_template(self):
            return self.AST_TEMPLATE.copy()


        def ast(self, ctx, params=None):
            ast = self.ast_template()
            return self.dbms.QueryUtils.resolve_query(query=self, ast=ast, ctx=ctx, params=params)


        def render(self, ctx):
            '''Renders the query as a string.

            Returns:
                str: rendered query
                ctx: todo
            '''
            return self.ast(ctx=ctx).sql(dialect=self.dbms.DIALECT)



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



    class QueryRowsHaveNull(Query):
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



    class QueryRowsArePositive(Query):
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



    class QueryRowsAreAscii(Query):
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
            sql='select is_ascii(substr(column, @char_offset, 1)) from table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def emulate(self, correct):
            return correct.isascii()



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
            if correct == EOS:
                return self.has_eos
            return correct in self.values_str


        def ast_template(self):
            ast = super().ast_template()
            if self.has_eos:
                select_exp = exp.or_(self.AST_HAS_EOS.copy(), ast.expressions[0])
            else:
                select_exp = exp.and_(self.AST_NO_EOS.copy(), ast.expressions[0])

            if not self.values_str:
                select_exp = select_exp.this

            ast.set('expressions', [select_exp])
            return ast


        def ast(self, ctx):
            return super().ast(ctx, params={'values_str': self.values_str})



    class QueryTextCharInString(QueryCharInString):
        def __init__(self, dbms, values):
            super().__init__(dbms)
            self.has_eos = EOS in values
            self.values_str = ''.join([v for v in values if v != EOS])



    class QueryBlobCharInString(QueryCharInString):
        def __init__(self, dbms, values):
            super().__init__(dbms)
            self.has_eos = EOS in values
            self.values_str = b''.join([v for v in values if v != EOS])



    class QueryCharLt(Query):
        def __init__(self, dbms, n):
            super().__init__(dbms)
            self.n = n



    class QueryTextCharLt(QueryCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select char_length(column) > @buffer_length and unicode(substr(column, @char_offset, 1)) < @n
                from table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )


        def emulate(self, correct):
            if correct == EOS:
                return False
            return ord(correct) < self.n


        def ast(self, ctx):
            return super().ast(ctx, params={'n': self.n})



    class QueryBlobCharLt(QueryCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select char_length(column) > @buffer_length and substr(column, @char_offset, 1) < @byte
                from table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )


        def emulate(self, correct):
            if correct == EOS:
                return False
            return correct[0] < self.n


        def ast_template(self):
            ast = super().ast_template()
            if self.n > BYTE_MAX:
                and_exp = ast.find(exp.And)
                and_exp = ast.find(exp.And)
                and_exp.replace(and_exp.this)
            return ast


        def ast(self, ctx):
            byte = bytes([min(self.n, BYTE_MAX)])
            return super().ast(ctx, params={'byte': byte})
