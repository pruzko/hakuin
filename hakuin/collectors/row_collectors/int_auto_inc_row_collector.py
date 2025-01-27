import asyncio

from .row_collector import RowCollector



class IntAutoIncRowCollector(RowCollector):
    def __init__(self, requester, dbms):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
        '''
        super().__init__(requester=requester, dbms=dbms)
        self._last = None
        self._last_lock = asyncio.Lock()


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (NumericContext): collection context

        Returns:
            int: collected row
        '''
        last = await self.get_last()
        if not last:
            return None

        n = await self.get_next(ctx, last)

        query = self.dbms.QueryValueInList(dbms=self.dbms, values=[n])
        if await self.requester.run(query=query, ctx=ctx):
            return n

        return None


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        last = await self.get_last()
        if last:
            is_success = await self.get_next(ctx, last) == value
            await self.stats.update(is_success=is_success, cost=1.0)

        async with self._last_lock:
            self._last = ctx.row_idx, value


    async def get_last(self):
        '''Coroutine-safe getter for the last row index and value.

        Returns:
            int, int: last row index and value
        '''
        async with self._last_lock:
            return self._last


    async def get_next(self, ctx, last):
        '''Computes the next auto-incremented value.

        Params:
            ctx (Context): collection context
            last (int, int): last row index and value

        Returns:
            int: next value
        '''
        last_row_idx, last_value = last
        offset = ctx.row_idx - last_row_idx
        return last_value + offset