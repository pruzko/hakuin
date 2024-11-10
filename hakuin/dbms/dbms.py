from abc import ABCMeta, abstractmethod
from collections.abc import Iterable

from sqlglot import exp, parse_one

from hakuin.collectors import BlobContext, TextContext
from hakuin.utils import BYTE_MAX, EOS



class DBMS(metaclass=ABCMeta):
    DIALECT = None



    class QueryResolver(metaclass=ABCMeta):
        AST_CAST_TO_INT = parse_one(sql='cast(@column as int)', dialect='sqlite')
        AST_CAST_TO_FLOAT = parse_one(sql='cast(@column as double)', dialect='sqlite')
        AST_CAST_TO_TEXT = parse_one(sql='cast(@column as text)', dialect='sqlite')
        AST_CAST_TO_BLOB = parse_one(sql='cast(@column as blob)', dialect='sqlite')


        def __init__(self, ast, ctx):
            self.ast = ast
            self.ctx = ctx


        def resolve(self, params={}):
            self.resolve_target()
            self.resolve_type_casts()
            self.resolve_params(params=params)
            return self.ast


        def resolve_target(self):
            if self.ctx.target == 'schema_names':
                self.resolve_target_schema_names()
            elif self.ctx.target == 'table_names':
                self.resolve_target_table_names()
            elif self.ctx.target == 'column_names':
                self.resolve_target_column_names()


        @abstractmethod
        def resolve_target_schema_names(self):
            raise NotImplementedError


        @abstractmethod
        def resolve_target_table_names(self):
            raise NotImplementedError


        @abstractmethod
        def resolve_target_column_names(self):
            raise NotImplementedError


        def resolve_type_casts(self):
            if self.ctx.cast_to == 'int':
                self.resolve_params(params={'column': self.AST_CAST_TO_INT.copy()})
            elif self.ctx.cast_to == 'float':
                self.resolve_params(params={'column': self.AST_CAST_TO_FLOAT.copy()})
            elif self.ctx.cast_to == 'text':
                self.resolve_params(params={'column': self.AST_CAST_TO_TEXT.copy()})
            elif self.ctx.cast_to == 'blob':
                self.resolve_params(params={'column': self.AST_CAST_TO_BLOB.copy()})


        def resolve_params(self, params={}):
            params = self._process_params(params)
            for param in self.ast.find_all(exp.Parameter):
                if param.name == 'column':
                    node = param.parent if isinstance(param.parent, exp.Column) else param
                    column = params.get('column') or exp.column(self.ctx.column, quoted=True)
                    node.replace(column)
                elif param.name == 'table':
                    node = param.parent if isinstance(param.parent, exp.Table) else param
                    table = params.get('table') or exp.table_(self.ctx.table, quoted=True)
                    node.replace(table)
                elif param.name in params:
                    param.replace(params[param.name])


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
            ast = ast_template or self.AST_TEMPLATE.copy()
            ast = self.dbms.QueryResolver(ast=ast, ctx=self.ctx).resolve(params=params)
            return ast


        def render(self):
            '''Renders the query as a string.

            Returns:
                str: rendered query
            '''
            return self.ast().sql(dialect=self.dbms.DIALECT)



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
            sql='select count(*) < @n from @table',
            dialect='sqlite',
        )

        def __init__(self, dbms, ctx, n):
            super().__init__(dbms, ctx)
            self.n = n

        def emulate(self, correct):
            return correct < self.n

        def ast(self):
            return super().ast(params={'n': self.n})



    class QueryRowsHaveNull(Query):
        AST_TEMPLATE = parse_one(
            sql='select count(*) > 0 from @table where @column is null',
            dialect='sqlite',
        )

        def emulate(self, correct):
            return None in correct



    class QueryRowIsNull(Query):
        AST_TEMPLATE = parse_one(
            sql='select @column is null from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )

        def emulate(self, correct):
            return correct is None



    class QueryRowsArePositive(Query):
        AST_TEMPLATE = parse_one(
            sql='select min(@column) >= 0 from @table',
            dialect='sqlite',
        )

        def emulate(self, correct):
            return all([row >= 0 for row in correct if row is not None])



    class QueryRowIsPositive(Query):
        AST_TEMPLATE = parse_one(
            sql='select @column >= 0 from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )

        def emulate(self, correct):
            return correct >= 0



    class QueryRowsAreAscii(Query):
        def emulate(self, correct):
            return all([row.isascii() for row in correct if row is not None])



    class QueryRowIsAscii(Query):
        def emulate(self, correct):
            return correct.isascii()



    class QueryCharIsAscii(Query):
        def emulate(self, correct):
            return correct.isascii()



    class QueryValueInList(Query):
        AST_TEMPLATE = parse_one(
            sql='select @column in (@values) from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )

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
        AST_TEMPLATE = parse_one(
            sql='select @column < @n from @table limit 1 offset @row_idx',
            dialect='sqlite',
        )

        def __init__(self, dbms, ctx, n):
            super().__init__(dbms, ctx)
            self.n = n

        def emulate(self, correct):
            return correct < self.n

        def ast(self):
            return super().ast(params={'n': self.n})



    class QueryCharInString(Query):
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


        def emulate(self, correct):
            if correct == EOS:
                return self.has_eos
            return correct in self.values_str


        def ast(self):
            template = self.AST_TEMPLATE_EOS if self.has_eos else self.AST_TEMPLATE_NO_EOS
            template = template.copy()

            if not self.values_str:
                and_or = template.find((exp.And, exp.Or))
                and_or.replace(and_or.this)

            return super().ast(ast_template=template, params={'values_str': self.values_str})



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



    class QueryCharLt(Query):
        def __init__(self, dbms, ctx, n):
            super().__init__(dbms, ctx)
            self.n = n



    class QueryTextCharLt(QueryCharLt):
        def emulate(self, correct):
            if correct == EOS:
                return False
            return ord(correct) < self.n


        def ast(self):
            return super().ast(params={'n': self.n})



    class QueryBlobCharLt(QueryCharLt):
        AST_TEMPLATE = parse_one(
            sql='''
                select length(@column) > @str_length and substr(@column, @char_offset, 1) < @byte
                from @table limit 1 offset @row_idx
            ''',
            dialect='sqlite',
        )


        def emulate(self, correct):
            if correct == EOS:
                return False
            return correct[0] < self.n


        def ast(self):
            ast = self.AST_TEMPLATE.copy()
            if self.n > BYTE_MAX:
                and_exp = ast.find(exp.And)
                and_exp.replace(and_exp.this)
                return super().ast(ast_template=ast)

            return super().ast(ast_template=ast, params={'byte': bytes([self.n])})
