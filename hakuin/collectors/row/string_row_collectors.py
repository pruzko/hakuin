from hakuin import Model
from hakuin.utils import Symbol

from .row_collector import RowCollector



class StringRowCollector(RowCollector):
    def __init__(
            self, requester, dbms, binary_char_collector, unigram_char_collector=None,
            fivegram_char_collector=None,
        ):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            binary_char_collector (BinaryCharCollector|ListBinaryCharCollector): binary char
                collector
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
            if c == Symbol.EOS:
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
        costs = []

        bin_cost = await self.binary_char_collector.stats.success_cost()
        costs.append((bin_cost, self.binary_char_collector))

        if self.unigram_char_collector:
            cost = await self.unigram_char_collector.stats.expected_cost(fallback_cost=bin_cost)
            costs.append((cost, self.unigram_char_collector))

        if self.fivegram_char_collector:
            cost = await self.fivegram_char_collector.stats.expected_cost(fallback_cost=bin_cost)
            costs.append((cost, self.fivegram_char_collector))

        return min(costs, key=lambda x: x[0] or float('inf'))[1]


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (str|bytes): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        bin_cost = await self.binary_char_collector.stats.success_cost()

        char_collector = await self.get_best_char_collector(ctx)
        char_cost = await char_collector.stats.expected_cost(fallback_cost=bin_cost)

        row_cost = char_cost * (len(value) + 1)
        await self.stats.update(is_success=True, cost=row_cost)

        if row_guessed:
            for char in Model.tokenize(value, add_sos=False):
                await self.update_char_collectors(ctx, value=char)
                if char != Symbol.EOS:
                    ctx.buffer += char


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
    '''Text row collector.'''
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
            query = self.dbms.QueryRowIsAscii(dbms=self.dbms)
            return await self.requester.run(query=query, ctx=ctx)

        return ctx.row_is_ascii



class MetaTextRowCollector(TextRowCollector):
    '''Meta row collector.'''
    def __init__(self, requester, dbms, binary_char_collector, fivegram_char_collector=None):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            binary_char_collector (BinaryCharCollector|ListBinaryCharCollector): binary char
                collector
            fivegram_char_collector (ModelCharCollector|None): fivegram char collector
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            binary_char_collector=binary_char_collector,
            fivegram_char_collector=fivegram_char_collector,
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
        pass



class BlobRowCollector(StringRowCollector):
    '''Blob row collector.'''
    pass
