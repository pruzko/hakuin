import asyncio
import sys
import tqdm
from abc import ABCMeta, abstractmethod

from hakuin.search_algorithms import BinarySearch

from .row_collectors import GuessingRowCollector



class Collector(metaclass=ABCMeta):
    '''Column collector base class. Column collectors repeatidly run row collectors
    to extract rows.
    '''
    def __init__(
            self, requester, dbms, row_collector, guessing_row_collector=None, n_tasks=1,
            query_cls_rows_count_lt=None, query_cls_rows_have_null=None, query_cls_row_is_null=None,
        ):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            row_collector (RowCollector): fallback row collector
            guessing_row_collector (GuessingRowCollector): guessing row collector
            n_tasks (int): number of extraction tasks to run in parallel
            query_cls_rows_count_lt (DBMS.Query): query class (default QueryRowsCountLt)
            query_cls_rows_have_null (DBMS.Query): query class (default QueryRowsHaveNull)
            query_cls_row_is_null (DBMS.Query): query class (default QueryRowIsNull)
        '''
        self.requester = requester
        self.dbms = dbms
        self.row_collector = row_collector
        self.guessing_row_collector = guessing_row_collector
        self.n_tasks = n_tasks
        self.query_cls_rows_count_lt = query_cls_rows_count_lt or self.dbms.QueryRowsCountLt
        self.query_cls_rows_have_null = query_cls_rows_have_null or self.dbms.QueryRowsHaveNull
        self.query_cls_row_is_null = query_cls_row_is_null or self.dbms.QueryRowIsNull


    async def run(self, ctx):
        '''Collects the whole column.

        Params:
            ctx (Context): collection context

        Returns:
            list: column rows
        '''
        if ctx.target == 'schema_names':
            tqdm.tqdm.write('Extracting Schema Names', file=sys.stderr)
        elif ctx.target == 'table_names':
            tqdm.tqdm.write('Extracting Table Names', file=sys.stderr)
        elif ctx.target == 'column_names':
            tqdm.tqdm.write(f'Extracting Column Names ({ctx.table})', file=sys.stderr)
        elif ctx.target == 'column':
            tqdm.tqdm.write(f'Extracting [{ctx.table}].[{ctx.column}]', file=sys.stderr)

        if ctx.n_rows is None:
            ctx.n_rows = await BinarySearch(
                requester=self.requester,
                dbms=self.dbms,
                query_cls=self.query_cls_rows_count_lt,
                lower=0,
                upper=128,
                find_lower=False,
                find_upper=True,
            ).run(ctx)

        if ctx.n_rows == 0:
            return []

        await self.check_rows(ctx)

        data = [None] * ctx.n_rows
        queue = asyncio.Queue()
        for row_idx in range(ctx.n_rows):
            await queue.put(row_idx)

        with tqdm.tqdm(total=ctx.n_rows, file=sys.stderr, leave=False) as progress:
            tasks = [self._task_collect_row(ctx, queue, data, progress) for _ in range(self.n_tasks)]
            await asyncio.gather(*tasks)

        return data


    async def _task_collect_row(self, ctx, queue, data, progress):
        while not queue.empty():
            try:
                row_idx = await queue.get()
            except asyncio.QueueEmpty:
                break

            row_ctx = ctx.clone()
            row_ctx.row_idx = row_idx

            if await self.check_row_is_null(row_ctx):
                res = None
            else:
                res = await self.collect_row(row_ctx)

            data[row_ctx.row_idx] = res

            progress.write(f'({row_ctx.row_idx + 1}/{row_ctx.n_rows}) [{row_ctx.table}].[{row_ctx.column}]: {res}', file=sys.stderr)
            progress.update(1)


    async def collect_row(self, ctx):
        '''Collects row and updates row collector and guessing row collector.

        Params:
            ctx (Context): collection context

        Returns:
            value: single row
        '''
        res = None
        tree = None
        row_guessed = False
        fallback_cost = await self.row_collector.stats.success_cost()

        if fallback_cost and self.guessing_row_collector:
            tree = await self.guessing_row_collector.make_tree(fallback_cost=fallback_cost)
            guessing_cost = await self.guessing_row_collector.stats.expected_cost(fallback_cost=fallback_cost)

            if tree and guessing_cost and guessing_cost < fallback_cost:
                res = await self.guessing_row_collector.run(ctx, tree=tree)
                row_guessed = res is not None

        if res is None:
            res = await self.row_collector.run(ctx)

        if self.guessing_row_collector:
            await self.guessing_row_collector.update(ctx, value=res, row_guessed=row_guessed, tree=tree)
        await self.row_collector.update(ctx, value=res, row_guessed=row_guessed)

        return res


    async def check_rows(self, ctx):
        '''Checks rows for various properties and sets the appropriate ctx settings.

        Params:
            ctx (Context): collection context
        '''
        ctx.rows_have_null = await self.check_rows_have_null(ctx)


    async def check_rows_have_null(self, ctx):
        '''Checks if rows have NULL.

        Params:
            ctx (Context): collection context

        Returns:
            bool: rows have NULL flag
        '''
        if ctx.rows_have_null is None:
            query = self.query_cls_rows_have_null(dbms=self.dbms)
            return await self.requester.run(query=query, ctx=ctx)

        return ctx.rows_have_null


    async def check_row_is_null(self, ctx):
        '''Checks if the current row is NULL.

        Params:
            ctx (Context): collection context

        Returns:
            bool: row is NULL flag
        '''
        if ctx.rows_have_null is False:
            return False

        query = self.query_cls_row_is_null(dbms=self.dbms)
        return await self.requester.run(query=query, ctx=ctx)



    class Builder(metaclass=ABCMeta):
        def __init__(self, requester, dbms, n_tasks=1):
            self.requester = requester
            self.dbms = dbms
            self.n_tasks = n_tasks
            self.row_collector = None
            self.guessing_row_collector = None


        def add_guessing_row_collector(self, query_cls_value_in_list=None):
            self.guessing_row_collector = GuessingRowCollector(
                requester=self.requester,
                dbms=self.dbms,
                query_cls_value_in_list=query_cls_value_in_list,
            )


        @abstractmethod
        def build(self, **kwargs):
            raise NotImplementedError