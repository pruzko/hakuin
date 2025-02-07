from .row_collector import RowCollector



class IntRowCollector(RowCollector):
    def __init__(self, requester, dbms, binary_row_collector, auto_inc_row_collector=None):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            binary_row_collector (BinaryRowCollector): int binary row collector
            auto_inc_row_collector (AutoIncRowCollector|None): int auto-inc row collector
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.binary_row_collector = binary_row_collector
        self.auto_inc_row_collector = auto_inc_row_collector


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (NumericContext): collection context

        Returns:
            int: collected row
        '''
        res = None
        bin_cost = await self.binary_row_collector.stats.success_cost()

        if bin_cost and self.auto_inc_row_collector:
            auto_inc_stats = self.auto_inc_row_collector.stats
            auto_inc_cost = await auto_inc_stats.expected_cost(fallback_cost=bin_cost)

            if auto_inc_cost and auto_inc_cost < bin_cost:
                res = await self.auto_inc_row_collector.run(ctx)
                if res is not None:
                    return res

        return await self.binary_row_collector.run(ctx)


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        bin_cost = await self.binary_row_collector.stats.success_cost()

        if bin_cost:
            if self.auto_inc_row_collector:
                auto_inc_stats = self.auto_inc_row_collector.stats
                auto_inc_cost = await auto_inc_stats.expected_cost(fallback_cost=bin_cost)
            else:
                auto_inc_cost = None

            cost = min(bin_cost, auto_inc_cost) if auto_inc_cost else bin_cost
            await self.stats.update(is_success=True, cost=cost)

        if self.auto_inc_row_collector:
            await self.auto_inc_row_collector.update(ctx, value=value, row_guessed=row_guessed)

        await self.binary_row_collector.update(ctx, value=value, row_guessed=row_guessed)
