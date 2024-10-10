import asyncio
import math
import statistics
from collections import deque

from hakuin.collectors import Stats
from hakuin.requesters import EmulationRequester
from hakuin.search_algorithms import BinarySearch

from .RowCollector import RowCollector



class IntBinaryRowCollector(RowCollector):
    def __init__(self, requester, dbms):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
        '''
        super().__init__(requester=requester, dbms=dbms)

        self._hist = deque(maxlen=100)
        self._hist_lock = asyncio.Lock()
        self._bounds_stats = {
            'const': Stats(),
            'min_max': Stats(),
            'norm_dist': Stats(),
        }


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (IntContext): collection context

        Returns:
            int: collected row
        '''
        lower, upper = await self.get_best_bounds(ctx=ctx)
        return await self._run(requester=self.requester, ctx=ctx, lower=lower, upper=upper)


    async def _emulate(self, ctx, lower, upper, correct):
        '''Emulates collection of a single row.

        Params:
            ctx (IntContext): collection context
            lower: initial lower bound
            upper: initial upper bound
            correct: correct value

        Returns:
            (int, int): number of requests and the result
        '''
        requester = EmulationRequester(correct=correct)
        res = await self._run(requester=requester, ctx=ctx, lower=lower, upper=upper)
        n_requests = await requester.n_requests()
        return n_requests, res


    async def _run(self, requester, ctx, lower, upper):
        '''Collects a single row.

        Params:
            requester (Requester): requester to be used
            ctx (IntContext): collection context
            lower: initial lower bound
            upper: initial upper bound
        '''
        if ctx.rows_are_positive:
            lower = max(lower, 0)
            find_lower = lower > 0
        else:
            find_lower = True

        return await BinarySearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryIntLt,
            lower=lower,
            upper=upper,
            find_lower=find_lower,
            find_upper=True,
        ).run(ctx)


    async def get_best_bounds(self, ctx):
        '''Gets the best bounds for binary search.

        Params:
            ctx (IntContext): collection context

        Returns:
            (int, int): bounds
        '''
        costs = {
            'const': await self._bounds_stats['const'].success_cost(),
            'min_max': await self._bounds_stats['min_max'].success_cost(),
            'norm_dist': await self._bounds_stats['norm_dist'].success_cost(),
        }

        best_bounds = min(costs, key=costs.get)
        # best_bounds = 'min_max'
        if best_bounds == 'const':
            return await self._get_const_bounds()
        elif best_bounds == 'min_max':
            return await self._get_min_max_bounds()
        else:
            return await self._get_norm_dist_bounds()


    async def _get_const_bounds(self):
        '''Gets cost bounds.

        Returns:
            (int, int): bounds
        '''
        return 0, 128


    async def _get_min_max_bounds(self):
        '''Gets min-max bounds.

        Returns:
            (int, int): bounds
        '''
        async with self._hist_lock:
            if len(self._hist) < 2:
                return await self._get_const_bounds()

            lower = min(self._hist)
            upper = max(self._hist)
            mean = (upper + lower) // 2
            diff = (upper - lower) / 2
            step = math.ceil(math.log2(diff)) if diff else 0
            step = max(step, 0)
            margin = 2 ** step
            return mean - margin, mean + margin


    async def _get_norm_dist_bounds(self):
        '''Gets normal distribution bounds.

        Returns:
            (int, int): bounds
        '''
        async with self._hist_lock:
            if len(self._hist) < 2:
                return await self._get_const_bounds()

            mean = int(round(statistics.mean(self._hist)))
            stdev = statistics.stdev(self._hist)
            step = math.ceil(math.log2(stdev / 4)) if stdev else 0
            step = max(step, 0)
            margin = 2 ** step
            return mean - margin, mean + margin


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        lower, upper = await self.get_best_bounds(ctx)
        cost, _ = await self._emulate(ctx, lower=lower, upper=upper, correct=value)
        await self.stats.update(is_success=True, cost=cost)

        bounds = {
            'const': await self._get_const_bounds(),
            'min_max': await self._get_min_max_bounds(),
            'norm_dist': await self._get_norm_dist_bounds(),
        }

        for key, (lower, upper) in bounds.items():
            cost, _ = await self._emulate(ctx, lower=lower, upper=upper, correct=value)
            await self._bounds_stats[key].update(is_success=True, cost=cost)

        async with self._hist_lock:
            self._hist.append(value)
