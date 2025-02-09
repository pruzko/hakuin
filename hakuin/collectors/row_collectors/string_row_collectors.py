from hakuin import Model
from hakuin.collectors.checks import check_flag
from hakuin.utils import Symbol

from .row_collector import RowCollector



class StringRowCollector(RowCollector):
    def __init__(self, requester, dbms, binary_char_collector, model_char_collectors=[]):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            binary_char_collector (BinaryCharCollector|ListBinaryCharCollector): binary char
                collector
            model_char_collectors (list): model char collectors
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.binary_char_collector = binary_char_collector
        self.model_char_collectors = model_char_collectors


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

        for model_char_collector in self.model_char_collectors:
            cost = await model_char_collector.stats.expected_cost(fallback_cost=bin_cost)
            costs.append((cost, model_char_collector))

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
        await self.binary_char_collector.update(ctx, value=value)
        for model_char_collector in self.model_char_collectors:
            await model_char_collector.update(ctx, value=value)



class TextRowCollector(StringRowCollector):
    '''Text row collector.'''
    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (Context): collection context

        Returns:
            str: collected row
        '''
        ctx.row_is_ascii = await check_flag(
            requester=self.requester,
            dbms=self.dbms,
            ctx=ctx,
            name='row_is_ascii',
            true_if_true='column_is_ascii',
        )
        return await super().run(ctx)



class MetaTextRowCollector(TextRowCollector):
    '''Meta row collector.'''
    async def collect_char(self, ctx):
        '''Collects a single char.

        Params:
            ctx (Context): collection context

        Returns:
            str: collected char
        '''
        res = None
        if self.model_char_collectors:
            res = await self.model_char_collectors[0].run(ctx)
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
