from hakuin.utils import EOS, tokenize

from .row_collector import RowCollector



class StringRowCollector(RowCollector):
    def __init__(self, requester, dbms, binary_char_collector, unigram_char_collector=None, fivegram_char_collector=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            binary_char_collector (BinaryCharCollector|ListBinaryCharCollector): binary char collector
            unigram_char_collector (ModelCharCollector|None): unigram char collector
            fivegram_char_collector (ModelCharCollector|None): fivegram char collector
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.binary_char_collector = binary_char_collector
        self.unigram_char_collector = unigram_char_collector
        self.fivegram_char_collector = fivegram_char_collector


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (Context): collection context

        Returns:
            str: collected row
        '''
        while True:
            c = await self.collect_char(ctx)
            await self.update_char_collectors(ctx, value=c)

            if c == EOS:
                return ctx.buffer
            ctx.buffer += c


    async def collect_char(self, ctx):
        '''Collects a single char.

        Params:
            ctx (Context): collection context

        Returns:
            str: collected char
        '''
        char_collector = await self.get_best_char_collector(ctx)
        res = await char_collector.run(ctx)
        if res is not None:
            return res
        return await self.binary_char_collector.run(ctx)


    async def get_best_char_collector(self, ctx):
        '''Selects the char collector with the lowest expected cost.

        Params:
            ctx (Context): collection context

        Returns:
            CharCollector: best char collector
        '''
        bin_cost = await self.binary_char_collector.stats.success_cost()

        costs = [(bin_cost, self.binary_char_collector)]
        if self.unigram_char_collector:
            cost = await self.unigram_char_collector.stats.total_cost(fallback_cost=bin_cost)
            costs.append((cost, self.unigram_char_collector))
        if self.fivegram_char_collector:
            cost = await self.fivegram_char_collector.stats.total_cost(fallback_cost=bin_cost)
            costs.append((cost, self.fivegram_char_collector))

        return min(costs, key=lambda x: x[0])[1]


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (str|bytes): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        char_collector = await self.get_best_char_collector(ctx)
        bin_cost = await self.binary_char_collector.stats.success_cost()
        char_cost = await char_collector.stats.total_cost(fallback_cost=bin_cost)
        row_cost = char_cost * (len(value) + 1)
        await self.stats.update(is_success=True, cost=row_cost)

        if row_guessed:
            for c in tokenize(value, add_sos=False, add_eos=True):
                await self.update_char_collectors(ctx, value=c)
                if c == EOS:
                     break
                ctx.buffer += c


    async def update_char_collectors(self, ctx, value):
        '''Updates char collectors with a newly collected char.

        Params:
            ctx (Context): collection context
            value (str): collected char
        '''
        if self.unigram_char_collector:
            await self.unigram_char_collector.update(ctx, value=value)
        if self.fivegram_char_collector:
            await self.fivegram_char_collector.update(ctx, value=value)
        await self.binary_char_collector.update(ctx, value=value)



class TextRowCollector(StringRowCollector):
    def __init__(self, query_cls_row_is_ascii=None, **kwargs):
        '''TODO

            query_cls_row_is_ascii (DBMS.Query): query class (default QueryRowIsAscii)
        '''
        super().__init__(**kwargs)
        self.query_cls_row_is_ascii = query_cls_row_is_ascii or self.dbms.QueryRowIsAscii


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (Context): collection context

        Returns:
            str: collected row
        '''
        ctx.row_is_ascii = await self.check_row_is_ascii(ctx)
        return await super().run(ctx)


    async def check_row_is_ascii(self, ctx):
        '''Checks if row is ascii.

        Params:
            ctx (StringContext): collection context

        Returns:
            bool: row is ascii flag
        '''
        if ctx.rows_are_ascii:
            return True

        if ctx.row_is_ascii is None:
            query = self.query_cls_row_is_ascii(dbms=self.dbms, ctx=ctx)
            return await self.requester.run(query)

        return ctx.row_is_ascii



class MetaTextRowCollector(TextRowCollector):
    def __init__(self, requester, dbms, binary_char_collector, fivegram_char_collector=None, query_cls_row_is_ascii=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            binary_char_collector (BinaryCharCollector|ListBinaryCharCollector): binary char collector
            fivegram_char_collector (ModelCharCollector|None): fivegram char collector
            query_cls_row_is_ascii (DBMS.Query): query class (default QueryRowIsAscii)
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            binary_char_collector=binary_char_collector,
            fivegram_char_collector=fivegram_char_collector,
            query_cls_row_is_ascii=query_cls_row_is_ascii,
        )


    async def collect_char(self, ctx):
        '''Collects a single char.

        Params:
            ctx (Context): collection context

        Returns:
            str: collected char
        '''
        res = None
        if self.fivegram_char_collector:
            res = await self.fivegram_char_collector.run(ctx)
        if res is None:
            res = await self.binary_char_collector.run(ctx)

        return res


    async def update(self, ctx, value, row_guessed):
        '''Does nothing. Meta collectors are stateless.'''
        pass


    async def update_char_collectors(self, ctx, value):
        '''Does nothing. Meta collectors are stateless.'''
        # TODO THIS IS IMPROVING PERFORMANCE BUT IS WRONG
        pass



class BlobRowCollector(StringRowCollector):
    pass
