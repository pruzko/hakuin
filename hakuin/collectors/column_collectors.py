import asyncio
import sys
import tqdm

from hakuin.collectors.checks import check_flag
from hakuin.search_algorithms import SectionSearch, TernarySectionSearch
from hakuin.utils import info



class ColumnCollector:
    '''Column collector base class. Column collectors repeatidly run row collectors
        to extract rows.
    '''
    FLAG_CHECKS = ['column_has_null']


    def __init__(
            self, requester, dbms, row_collector, guessing_row_collector=None, use_ternary=False,
            n_tasks=1,
        ):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            row_collector (RowCollector): fallback row collector
            guessing_row_collector (GuessingRowCollector): guessing row collector
            use_ternary (bool): use ternary search flag
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        self.requester = requester
        self.dbms = dbms
        self.row_collector = row_collector
        self.guessing_row_collector = guessing_row_collector
        self.use_ternary = use_ternary
        self.n_tasks = n_tasks


    async def run(self, ctx):
        '''Collects the whole column.

        Params:
            ctx (Context): collection context

        Returns:
            list: column rows
        '''
        if ctx.target in ['schema_names', 'table_names', 'column_names', 'column']:
            info(f'extracting_{ctx.target}', ctx.table, ctx.column)

        if ctx.n_rows is None:
            SearchAlg = TernarySectionSearch if self.use_ternary else SectionSearch
            ctx.n_rows = await SearchAlg(
                requester=self.requester,
                dbms=self.dbms,
                query_cls=self.dbms.QueryRowsCountLt,
                lower=0,
                upper=128 if ctx.target == 'column' else 8,
                find_lower=False,
                find_upper=True,
            ).run(ctx)

        if ctx.n_rows == 0:
            return []

        for name in self.FLAG_CHECKS:
            flag = await check_flag(requester=self.requester, dbms=self.dbms, ctx=ctx, name=name)
            setattr(ctx, name, flag)

        data = [None] * ctx.n_rows
        queue = asyncio.Queue()
        for row_idx in range(ctx.n_rows):
            await queue.put(row_idx)

        with tqdm.tqdm(total=ctx.n_rows, file=sys.stderr, leave=False) as progress:
            await asyncio.gather(*[
                self._task_collect_row(ctx, queue, data, progress) for _ in range(self.n_tasks)
            ])

        return data


    async def _task_collect_row(self, ctx, queue, data, progress):
        while not queue.empty():
            try:
                row_idx = await queue.get()
            except asyncio.QueueEmpty:
                break

            row_ctx = ctx.clone()
            row_ctx.row_idx = row_idx

            row_is_null = await check_flag(
                requester=self.requester,
                dbms=self.dbms,
                ctx=row_ctx,
                name='row_is_null',
                false_if_false='column_has_null',
            )

            res = None if row_is_null else await self.collect_row(row_ctx)

            data[row_ctx.row_idx] = res

            info('row_extracted', row_idx + 1, ctx.n_rows, res, progress=progress)
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



class IntColumnCollector(ColumnCollector):
    '''Integer column collector. Column collectors repeatidly run row collectors to extract
        rows.
    '''
    FLAG_CHECKS = [*ColumnCollector.FLAG_CHECKS, 'column_is_positive']



class FloatColumnCollector(ColumnCollector):
    '''Float column collector. Column collectors repeatidly run row collectors to extract rows.'''
    FLAG_CHECKS = [*ColumnCollector.FLAG_CHECKS, 'column_is_positive']



class TextColumnCollector(ColumnCollector):
    '''Text column collector. Column collectors repeatidly run row collectors to extract rows.'''
    FLAG_CHECKS = [*ColumnCollector.FLAG_CHECKS, 'column_is_ascii']



class BlobColumnCollector(ColumnCollector):
    '''Blob column collector. Column collectors repeatidly run row collectors to extract rows.'''
    pass
