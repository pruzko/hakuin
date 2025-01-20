from .row_collector import RowCollector



class IntRowCollector(RowCollector):
    def __init__(self, requester, dbms, int_binary_row_collector, int_auto_inc_row_collector=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            int_binary_row_collector (IntBinaryRowCollector): int binary row collector
            int_auto_inc_row_collector (IntAutoIncRowCollector|None): int auto-inc row collector
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.int_binary_row_collector = int_binary_row_collector
        self.int_auto_inc_row_collector = int_auto_inc_row_collector


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (NumericContext): collection context

        Returns:
            int: collected row
        '''
        res = None
        bin_cost = await self.int_binary_row_collector.stats.success_cost()

        if bin_cost and self.int_auto_inc_row_collector:
            auto_inc_cost = await self.int_auto_inc_row_collector.stats.expected_cost(fallback_cost=bin_cost)

            if auto_inc_cost and auto_inc_cost < bin_cost:
                res = await self.int_auto_inc_row_collector.run(ctx)
                if res is not None:
                    return res

        return await self.int_binary_row_collector.run(ctx)


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        bin_cost = await self.int_binary_row_collector.stats.success_cost()

        if bin_cost:
            if self.int_auto_inc_row_collector:
                auto_inc_cost = await self.int_auto_inc_row_collector.stats.expected_cost(fallback_cost=bin_cost)
            else:
                auto_inc_cost = None

            cost = min(bin_cost, auto_inc_cost) if auto_inc_cost else bin_cost
            await self.stats.update(is_success=True, cost=cost)

        if self.int_auto_inc_row_collector:
            await self.int_auto_inc_row_collector.update(ctx, value=value, row_guessed=row_guessed)                

        await self.int_binary_row_collector.update(ctx, value=value, row_guessed=row_guessed)
