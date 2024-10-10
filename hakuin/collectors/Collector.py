import asyncio
import sys
import tqdm
from abc import ABCMeta, abstractmethod
from copy import deepcopy

from hakuin.search_algorithms import BinarySearch

from .row_collectors import GuessingRowCollector



class Collector(metaclass=ABCMeta):
    '''Abstract class for column collectors. Column collectors repeatidly run
    row collectors to extract rows.
    '''
    def __init__(self, requester, dbms, row_collector, guessing_row_collector=None, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            row_collector (RowCollector): fallback row collector
            guessing_row_collector (GuessingRowCollector): guessing row collector
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        self.requester = requester
        self.dbms = dbms
        self.n_tasks = n_tasks
        self.row_collector = row_collector
        self.guessing_row_collector = guessing_row_collector

        self._row_idx_ctr = 0
        self._row_idx_ctr_lock = asyncio.Lock()
        self._data_lock = asyncio.Lock()


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
                query_cls=self.dbms.QueryRowsCountLt,
                lower=0,
                upper=128,
                find_lower=False,
                find_upper=True,
            ).run(ctx)

        if ctx.n_rows == 0:
            return []

        await self.check_rows(ctx)

        with tqdm.tqdm(total=ctx.n_rows, file=sys.stderr, leave=False) as progress:
            data = [None] * ctx.n_rows
            await asyncio.gather(
                *[self._task_collect_row(ctx, data, progress) for _ in range(self.n_tasks)]
            )

        return data


    async def _task_collect_row(self, ctx, data, progress):
        while True:
            row_idx = None
            async with self._row_idx_ctr_lock:
                if self._row_idx_ctr >= ctx.n_rows:
                    return
                row_idx = self._row_idx_ctr
                self._row_idx_ctr += 1

            row_ctx = deepcopy(ctx)
            row_ctx.row_idx = row_idx

            await self.check_row(row_ctx)
            res = await self.collect_row(row_ctx) if not row_ctx.row_is_null else None

            async with self._data_lock:
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

        if self.guessing_row_collector:
            tree = await self.guessing_row_collector.make_tree(fallback_cost=fallback_cost)
            guessing_cost = await self.guessing_row_collector.stats.total_cost(fallback_cost=fallback_cost)

            if tree and guessing_cost < fallback_cost:
                res = await self.guessing_row_collector.run(ctx, tree=tree)
                row_guessed = res is not None

        if res is None:
            res = await self.row_collector.run(ctx)

        if self.guessing_row_collector:
            await self.guessing_row_collector.update(ctx, value=res, row_guessed=row_guessed, tree=tree)
        await self.row_collector.update(ctx, value=res, row_guessed=row_guessed)

        return res


    async def check_rows(self, ctx):
        '''Checks rows for various properties and updates the collection context.

        Params:
            ctx (Context): collection context
        '''
        if ctx.rows_have_null is None:
            ctx.rows_have_null = await self.check_rows_have_null(ctx)


    async def check_row(self, ctx):
        '''Checks the current row for various properties and updates the collection context.

        Params:
            ctx (Context): collection context
        '''
        if ctx.rows_have_null:
            ctx.row_is_null = await self.check_row_is_null(ctx)
        else:
            ctx.row_is_null = False


    async def check_rows_have_null(self, ctx):
        '''Checks if rows have NULL.

        Params:
            ctx (Context): collection context

        Returns:
            bool: rows have NULL flag
        '''
        query = self.dbms.QueryRowsHaveNull(dbms=self.dbms, ctx=ctx)
        return await self.requester.run(query)


    async def check_row_is_null(self, ctx):
        '''Checks if the current row is NULL.

        Params:
            ctx (Context): collection context

        Returns:
            bool: row is NULL flag
        '''
        query = self.dbms.QueryRowIsNull(dbms=self.dbms, ctx=ctx)
        return await self.requester.run(query)



class CollectorBuilder(metaclass=ABCMeta):
    def __init__(self, requester, dbms, n_tasks=1):
        self.requester = requester
        self.dbms = dbms
        self.n_tasks = n_tasks
        self.guessing_row_collector = None


    def add_guessing(self):
        self.guessing_row_collector = GuessingRowCollector(requester=self.requester, dbms=self.dbms)


    @abstractmethod
    def build(self):
        raise NotImplementedError