from dataclasses import asdict

from hakuin.collectors import TextContext

from .row_collector import RowCollector



class FloatRowCollector(RowCollector):
    '''Float row collector.'''
    def __init__(self, requester, dbms, binary_row_collector, dec_text_row_collector):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            binary_row_collector (BinaryRowCollector): int binary row collector
                for the integer part
            dec_text_row_collector (StringCollector): text row collector for the decimal part
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.binary_row_collector = binary_row_collector
        self.dec_text_row_collector = dec_text_row_collector


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (NumericContext): collection context

        Returns:
            float: collected row
        '''
        int_part = await self.binary_row_collector.run(ctx=self._make_int_ctx(ctx))

        if int_part == 0:
            # int(-0.123) and int(0.123) are both 0, so we need to check positivity
            is_positive = ctx.rows_are_positive or await self.check_row_is_positive(ctx)
        else:
            is_positive = int_part > 0

        sign = '' if is_positive else '-'
        buffer = f'{sign}{abs(int_part)}.'

        text_ctx = self._make_text_ctx(ctx, start_offset=len(buffer))
        buffer += await self.dec_text_row_collector.run(text_ctx)

        return float(buffer)


    async def check_row_is_positive(self, ctx):
        '''Checks if the current row is positive.

        Params:
            ctx (NumericContext): collection context

        Returns:
            bool: row is positive flag
        '''
        if ctx.rows_are_positive is True:
            return True

        query = self.dbms.QueryRowIsPositive(dbms=self.dbms)
        return await self.requester.run(query=query, ctx=ctx)


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        int_part, dec_part = str(value).split('.')
        int_part = int(int_part)

        sign_cost = 1.0 if int_part == 0 else 0.0
        int_cost = await self.binary_row_collector.stats.success_cost()
        dec_cost = await self.dec_text_row_collector.stats.success_cost()

        if int_cost and dec_cost:
            await self.stats.update(is_success=True, cost=sign_cost + int_cost + dec_cost)

        await self.binary_row_collector.update(ctx, value=int_part, row_guessed=row_guessed)

        sign = '' if value >= 0.0 else '-'
        buffer = f'{sign}{abs(int_part)}.'

        ctx = self._make_text_ctx(ctx, start_offset=len(buffer))
        await self.dec_text_row_collector.update(ctx, value=dec_part, row_guessed=row_guessed)


    @staticmethod
    def _make_int_ctx(ctx):
        ctx = ctx.clone()
        ctx.cast_to = 'int'
        return ctx


    @staticmethod
    def _make_text_ctx(ctx, start_offset):
        kwargs = asdict(ctx)
        kwargs.pop('rows_are_positive')
        kwargs['start_offset'] = start_offset
        kwargs['rows_are_ascii'] = True
        kwargs['row_is_ascii'] = True
        kwargs['cast_to'] = 'text'
        return TextContext(**kwargs)
