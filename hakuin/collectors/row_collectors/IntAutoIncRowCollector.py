import asyncio

from .RowCollector import RowCollector



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
            ctx (IntContext): collection context

        Returns:
            int: collected row
        '''
        if self._last is None:
            return None

        n = await self._get_auto_inc(ctx)
        query = self.dbms.QueryValueInList(dbms=self.dbms, ctx=ctx, values=[n])
        if await self.requester.run(query):
            return n
        return None


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        if self._last:
            is_success = value == await self._get_auto_inc(ctx)
            await self.stats.update(is_success=is_success, cost=1)

        async with self._last_lock:
            self._last = ctx.row_idx, value


    async def _get_auto_inc(self, ctx):
        '''Computes the auto-incremented value.

        Params:
            ctx (Context): collection context
        '''
        async with self._last_lock:
            last_row_idx, last_value = self._last
            offset = ctx.row_idx - last_row_idx
            return last_value + offset