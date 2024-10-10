from abc import ABCMeta, abstractmethod
from collections.abc import Iterable

from sqlglot import exp, parse_one

from hakuin.collectors import BlobContext, TextContext
from hakuin.utils import BYTE_MAX, EOS



class QueryTemplate:
    def __init__(self, dbms, ctx, ast):
        self.dbms = dbms
        self.ctx = ctx
        self.ast = ast


    def resolve(self, params={}):
        self.dbms.template_resolve_target(template=self)
        self.resolve_params(params=params)
        self.resolve_functions()
        return self.ast


    def resolve_params(self, params={}):
        params = self._process_params(params)
        for param in self.ast.find_all(exp.Parameter):
            if param.name == 'column':
                node = param.parent if isinstance(param.parent, exp.Column) else param
                column = params.get('column') or exp.to_column(self.ctx.column)
                node.replace(column)
            elif param.name == 'table':
                node = param.parent if isinstance(param.parent, exp.Table) else param
                table = params.get('table') or exp.to_table(self.ctx.table)
                node.replace(table)
            elif param.name in params:
                param.replace(params[param.name])


    def resolve_functions(self):
        for func in self.ast.find_all(exp.Anonymous):
            if func.name.lower() == 'char_length':
                func.replace(self.dbms.ast_char_length(self.ctx, func=func))
            if func.name.lower() == 'instr':
                func.replace(self.dbms.ast_instr(self.ctx, func=func))
            if func.name.lower() == 'unicode':
                func.replace(self.dbms.ast_unicode(self.ctx, func=func))
            if func.name.lower() == 'isascii':
                func.replace(self.dbms.ast_isascii(self.ctx, func=func))


    def _process_params(self, params):
        params = {k: v if isinstance(v, exp.Expression) else self._to_literal(v) for k, v in params.items()}

        row_idx = self.ctx.row_idx or 0
        params['row_idx'] = params.get('row_idx', self._to_literal(row_idx))

        if isinstance(self.ctx, (BlobContext, TextContext)):
            str_length = self.ctx.start_offset + len(self.ctx.buffer)
            params['str_length'] = params.get('str_length', self._to_literal(str_length))
            params['char_offset'] = params.get('char_offset', self._to_literal(str_length + 1))
        return params


    def _to_literal(self, value):
        if isinstance(value, (int, float)):
            return exp.Literal.number(value)
        elif isinstance(value, str):
            return exp.Literal.string(value)
        elif isinstance(value, bytes):
            return exp.HexString(this=value.hex())
        elif isinstance(value, Iterable):
            return [self._to_literal(item) for item in value]
        raise TypeError(f'Type not supported: {type(value)}')



class DBMS(metaclass=ABCMeta):
    DIALECT = None


    def __init__(self):
        pass


    @abstractmethod
    def template_resolve_target(self, template):
        raise NotImplementedError

    def ast_char_length(self, ctx, func):
        return exp.Length(this=func.expressions[0])

    @abstractmethod
    def ast_unicode(self, ctx, func):
        raise NotImplementedError


    @abstractmethod
    def ast_instr(self, ctx, func):
        raise NotImplementedError


    @abstractmethod
    def ast_isascii(self, ctx, func):
        raise NotImplementedError



    class Query(metaclass=ABCMeta):
        '''Abstract class for queries.'''
        AST_TEMPLATE = None


        def __init__(self, dbms, ctx):
            '''Constructor.

            Params:
                dbms (DBMS): DBMS instance used to render queries
                ctx (Context): collection context
            '''
            self.dbms = dbms
            self.ctx = ctx


        @abstractmethod
        def emulate(self, correct):
            '''Retrieves the logical result of the query without constructing it.

            Params:
                correct (value): correct value

            Returns:
                bool: logical result
            '''
            raise NotImplementedError


        def ast(self, ast_template=None, params={}):
            assert ast_template or self.AST_TEMPLATE, 'AST template not provided.'
            return QueryTemplate(
                dbms=self.dbms,
                ctx=self.ctx,
                ast=ast_template or self.AST_TEMPLATE.copy(),
            ).resolve(params=params)


        def render(self):
            '''Renders the query as a string.

            Returns:
                str: rendered query
            '''
            return self.ast().sql(dialect=self.dbms.DIALECT)



    class QueryColumnTypeIsInt(Query):
        def emulate(self, correct):
            return all([type(row) is int for row in correct if row is not None])

        @abstractmethod
        def ast(self):
            raise NotImplementedError



    class QueryColumnTypeIsFloat(Query):
        def emulate(self, correct):
            return all([type(row) is float for row in correct if row is not None])

        @abstractmethod
        def ast(self):
            raise NotImplementedError



    class QueryColumnTypeIsText(Query):
        def emulate(self, correct):
            return all([type(row) is str for row in correct if row is not None])

        @abstractmethod
        def ast(self):
            raise NotImplementedError



    class QueryColumnTypeIsBlob(Query):
        def emulate(self, correct):
            return all([type(row) is bytes for row in correct if row is not None])

        @abstractmethod
        def ast(self):
            raise NotImplementedError



    class QueryRowsCountLt(Query):
        AST_TEMPLATE = parse_one('select count(*) < @n from @table', dialect='sqlite')

        def __init__(self, dbms, ctx, n):
            super().__init__(dbms, ctx)
            self.n = n

        def emulate(self, correct):
            return correct < self.n

        def ast(self):
            return super().ast(params={'n': self.n})



    class QueryRowsHaveNull(Query):
        AST_TEMPLATE = parse_one('select max(@column is null) from @table', dialect='sqlite')

        def emulate(self, correct):
            return None in correct



    class QueryRowIsNull(Query):
        AST_TEMPLATE = parse_one('select @column is null from @table limit 1 offset @row_idx', dialect='sqlite')

        def emulate(self, correct):
            return correct is None



    class QueryRowsArePositive(Query):
        AST_TEMPLATE = parse_one('select min(@column >= 0) from @table', dialect='sqlite')

        def emulate(self, correct):
            return all([row >= 0 for row in correct if row is not None])



    class QueryRowIsPositive(Query):
        AST_TEMPLATE = parse_one('select @column >= 0 from @table limit 1 offset @row_idx', dialect='sqlite')

        def emulate(self, correct):
            return correct >= 0



    class QueryRowsAreAscii(Query):
        AST_TEMPLATE = parse_one('select min(isascii(@column)) from @table', dialect='sqlite')

        def emulate(self, correct):
            return all([row.isascii() for row in correct if row is not None])



    class QueryRowIsAscii(Query):
        AST_TEMPLATE = parse_one('select isascii(@column) from @table limit 1 offset @row_idx', dialect='sqlite')

        def emulate(self, correct):
            return correct.isascii()



    class QueryValueInList(Query):
        AST_TEMPLATE = parse_one('select @column in (@values) from @table limit 1 offset @row_idx', dialect='sqlite')

        def __init__(self, dbms, ctx, values):
            super().__init__(dbms, ctx)
            self.values = values
            for v in self.values:
                assert isinstance(v, (int, float, str, bytes)), f'Type not supported: {type(v)}.'

        def emulate(self, correct):
            return correct in self.values

        def ast(self):
            return super().ast(params={'values': self.values})



    class QueryIntLt(Query):
        AST_TEMPLATE = parse_one('select @column < @n from @table limit 1 offset @row_idx', dialect='sqlite')

        def __init__(self, dbms, ctx, n):
            super().__init__(dbms, ctx)
            self.n = n

        def emulate(self, correct):
            return correct < self.n

        def ast(self):
            return super().ast(params={'n': self.n})



    class QueryFloatIntLt(Query):
        AST_TEMPLATE = parse_one('select cast(@column as int) < @n from @table limit 1 offset @row_idx', dialect='sqlite')

        def __init__(self, dbms, ctx, n):
            super().__init__(dbms, ctx)
            self.n = n

        def emulate(self, correct):
            return correct < self.n

        def ast(self):
            return super().ast(params={'n': self.n})



    class QueryCharIsAscii(Query):
        AST_TEMPLATE = parse_one('select isascii(substr(@column, @char_offset, 1)) from @table limit 1 offset @row_idx', dialect='sqlite')

        def emulate(self, correct):
            return correct.isascii()



    class QueryCharInString(Query):
        AST_TEMPLATE_EOS = parse_one(
            'select char_length(@column) = @str_length or instr(@values_str, substr(@column, @char_offset, 1)) from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )
        AST_TEMPLATE_NO_EOS = parse_one(
            'select char_length(@column) > @str_length and instr(@values_str, substr(@column, @char_offset, 1)) from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def __init__(self, dbms, ctx, values):
            super().__init__(dbms, ctx)
            assert values, f'No values provided.'
            if isinstance(self.ctx, TextContext):
                self.values_str = ''.join([v for v in values if v != EOS])
            else:
                self.values_str = b''.join([v for v in values if v != EOS])
            self.has_eos = EOS in values


        def emulate(self, correct):
            if correct == EOS:
                return self.has_eos
            return correct in self.values_str


        def ast(self):
            template = self.AST_TEMPLATE_EOS if self.has_eos else self.AST_TEMPLATE_NO_EOS
            ast = super().ast(ast_template=template.copy(), params={'values_str': self.values_str})

            if not self.values_str:
                and_or = ast.find((exp.And, exp.Or))
                and_or.replace(and_or.this)

            return ast



    class QueryCharLt(Query):
        AST_TEMPLATE_TEXT = parse_one(
            'select char_length(@column) > @str_length and unicode(substr(@column, @char_offset, 1)) < @n from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )
        AST_TEMPLATE_BLOB = parse_one(
            'select char_length(@column) > @str_length and substr(@column, @char_offset, 1) < @byte from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )


        def __init__(self, dbms, ctx, n):
            super().__init__(dbms, ctx)
            self.n = n


        def emulate(self, correct):
            if correct == EOS:
                return False

            correct_int = ord(correct) if isinstance(self.ctx, TextContext) else int.from_bytes(correct, byteorder='big')
            return correct_int < self.n


        def ast(self):
            if isinstance(self.ctx, TextContext):
                return super().ast(ast_template=self.AST_TEMPLATE_TEXT.copy(), params={'n': self.n})

            ast = super().ast(ast_template=self.AST_TEMPLATE_BLOB.copy(), params={'byte': bytes([min(self.n, BYTE_MAX)])})

            if self.n > BYTE_MAX:
                and_exp = ast.find(exp.And)
                and_exp.replace(and_exp.this)

            return ast
