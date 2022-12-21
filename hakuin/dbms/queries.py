from abc import ABCMeta, abstractmethod



class Queries(metaclass=ABCMeta):
    @abstractmethod
    def count_rows(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def count_tables(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def count_columns(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def meta_type(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def meta_is_nullable(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def meta_is_pk(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def meta_references(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def char_rows(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def char_tables(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def char_columns(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def string_rows(self, ctx, values):
        raise NotImplementedError()




class SQLiteQueries(Queries):
    def count_rows(self, ctx, n):
        return f'SELECT COUNT(*) < {n} FROM {ctx.table}'


    def count_tables(self, ctx, n):
        return f'SELECT COUNT(*) < {n} FROM sqlite_master WHERE type="table"'


    def count_columns(self, ctx, n):
        return f'SELECT COUNT(*) < {n} FROM pragma_table_info("{ctx.table}")'


    def meta_type(self, ctx, values):
        values = [f"'{v}'" for v in values]
        return f"SELECT type in ({','.join(values)}) FROM pragma_table_info('{ctx.table}') WHERE name='{ctx.column}'"


    def meta_is_nullable(self, ctx):
        return f"SELECT [notnull] == 0 FROM pragma_table_info('{ctx.table}') WHERE name='{ctx.column}'"


    def meta_is_pk(self, ctx):
        return f"SELECT pk FROM pragma_table_info('{ctx.table}') WHERE name='{ctx.column}'"


    def meta_references(self, ctx, values):
        raise NotImplementedError('TODO')


    def _char(self, ctx, values, mode):
        column = ctx.column if mode == 'row' else 'name'
        char_query = f'substr({column}, {len(ctx.s) + 1}, 1)'
        eos_query = '' if '</s>' in values else f'({char_query} != "") and '

        if mode == 'row':
            from_query = ctx.table
        elif mode == 'table':
            from_query = f'sqlite_master WHERE type="table"'
        else:
            from_query = f'pragma_table_info("{ctx.table}")'

        values = [v for v in values if v != '</s>']
        values = ''.join(values).encode('ascii').hex()
        return f"SELECT {eos_query} instr(x'{values}', {char_query}) FROM {from_query} LIMIT 1 OFFSET {ctx.row}"


    def char_rows(self, ctx, values):
        return self._char(ctx, values, 'row')


    def char_tables(self, ctx, values):
        return self._char(ctx, values, 'table')


    def char_columns(self, ctx, values):
        return self._char(ctx, values, 'column')


    def string_rows(self, ctx, values):
        values = [f"x'{v.encode('ascii').hex()}'" for v in values]
        return f"SELECT cast({ctx.column} as blob) in ({','.join(values)}) FROM {ctx.table} LIMIT 1 OFFSET {ctx.row}"
