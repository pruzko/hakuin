import asyncio
import math
import statistics
from collections import deque

from hakuin.collectors import Stats
from hakuin.requesters import EmulationRequester
from hakuin.search_algorithms import SectionSearch, TernarySectionSearch

from .row_collector import RowCollector



class _Bounds:
    '''Helper class for computing search bounds.'''
    def __init__(self):
        '''Constructor.'''
        self.stats = {
            'const': Stats(),
            'min_max': Stats(),
            'norm_dist': Stats(),
        }
        self._hist = deque(maxlen=100)
        self._lock = asyncio.Lock()


    async def get_const(self):
        '''Retrieves cost bounds.

        Returns:
            (int, int): bounds
        '''
        return 0, 128


    async def get_min_max(self):
        '''Retrieves min-max bounds.

        Returns:
            (int, int): bounds
        '''
        async with self._lock:
            if len(self._hist) < 2:
                return await self.get_const()

            lower = min(self._hist)
            upper = max(self._hist)
            if lower == upper:
                upper += 1

            return lower, upper


    async def get_norm_dist(self):
        '''Retrieves normal distribution bounds.

        Returns:
            (int, int): bounds
        '''
        async with self._lock:
            if len(self._hist) < 2:
                return await self.get_const()

            mean = statistics.mean(self._hist)
            stdev = statistics.stdev(self._hist)

            lower = math.floor(mean - stdev / 2)
            upper = math.ceil(mean + stdev / 2)
            if lower == upper:
                upper += 1

            return lower, upper


    async def get_best(self):
        '''Retrieves the best bounds for binary search.

        Returns:
            (int, int): bounds
        '''
        costs = {bounds: await stats.success_cost() for bounds, stats in self.stats.items()}
        best_bounds = min(costs, key=lambda x: costs.get(x) or float('inf'))
        return await self.get_by_name(best_bounds)


    async def get_by_name(self, name):
        '''Retrieves target bounds.

        Params:
            name (str): bounds name

        return:
            (int, int): bounds
        '''
        assert name in self.stats, f'Unknown bounds "{name}".'
        return await getattr(self, f'get_{name}')()


    async def update(self, ctx, value, row_collector):
        '''Updates the bounds with a new value.

        Params:
            ctx (Context): collection context
            value (int): new value
            row_collector (BinaryRowCollector): row collector
        '''
        for bounds, stats in self.stats.items():
            lower, upper = await self.get_by_name(bounds)
            cost, _ = await row_collector._emulate(ctx, lower=lower, upper=upper, correct=value)
            await stats.update(is_success=True, cost=cost)

        async with self._lock:
            self._hist.append(value)



class BinaryRowCollector(RowCollector):
    '''Binary row collector for integers.'''
    def __init__(self, requester, dbms, use_ternary=False):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            use_ternary (bool): use ternary search flag
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.use_ternary = use_ternary
        self.bounds = _Bounds()


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (NumericContext): collection context

        Returns:
            int: collected row
        '''
        lower, upper = await self.bounds.get_best()
        return await self._run(requester=self.requester, ctx=ctx, lower=lower, upper=upper)


    async def _run(self, requester, ctx, lower, upper):
        '''Collects a single row.

        Params:
            requester (Requester): requester to be used
            ctx (NumericContext): collection context
            lower: initial lower bound
            upper: initial upper bound
        '''
        if ctx.column_is_positive:
            lower = max(lower, 0)
            find_lower = lower > 0
        else:
            find_lower = True

        SearchAlg = TernarySectionSearch if self.use_ternary else SectionSearch
        return await SearchAlg(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryIntLt,
            lower=lower,
            upper=upper,
            find_lower=find_lower,
            find_upper=True,
        ).run(ctx)


    async def _emulate(self, ctx, lower, upper, correct):
        '''Emulates collection of a single row.

        Params:
            ctx (NumericContext): collection context
            lower: initial lower bound
            upper: initial upper bound
            correct: correct value

        Returns:
            (int, int): request count and the result if available
        '''
        requester = EmulationRequester(correct=correct)
        res = await self._run(requester=requester, ctx=ctx, lower=lower, upper=upper)
        n_requests = await requester.n_requests()
        return n_requests, res


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        lower, upper = await self.bounds.get_best()
        cost, _ = await self._emulate(ctx, lower=lower, upper=upper, correct=value)
        await self.stats.update(is_success=True, cost=cost)
        await self.bounds.update(ctx, value=value, row_collector=self)
