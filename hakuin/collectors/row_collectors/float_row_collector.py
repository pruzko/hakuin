from copy import deepcopy
from dataclasses import asdict

from hakuin.collectors import TextContext

from .row_collector import RowCollector



class FloatRowCollector(RowCollector):
    def __init__(self, requester, dbms, int_binary_row_collector, dec_text_row_collector, query_cls_row_is_positive=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            int_binary_row_collector (IntBinaryRowCollector): int binary row collector for the integer part
            dec_text_row_collector (StringCollector): text row collector for the decimal part
            query_cls_row_is_positive (DBMS.Query): query class (default QueryRowIsPositive)
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.query_cls_row_is_positive = query_cls_row_is_positive or self.dbms.QueryRowIsPositive

        self.int_binary_row_collector = int_binary_row_collector
        self.dec_text_row_collector = dec_text_row_collector


    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (NumericContext): collection context

        Returns:
            float: collected row
        '''
        int_ctx = deepcopy(ctx)
        int_ctx.cast_to = 'int'
        int_part = await self.int_binary_row_collector.run(int_ctx)

        if int_part == 0:
            sign = '' if ctx.rows_are_positive or await self.check_row_is_positive(ctx) else '-'
        else:
            sign = '' if int_part >= 0 else '-'

        int_part = abs(int_part)

        text_ctx = self._to_text_ctx(ctx, start_offset=len(f'{sign}{int_part}.'))
        dec_part = await self.dec_text_row_collector.run(text_ctx)

        return float(f'{sign}{int_part}.{dec_part}')


    async def check_row_is_positive(self, ctx):
        '''TODO'''
        if ctx.rows_are_positive is True:
            return True

        query = self.query_cls_row_is_positive(dbms=self.dbms, ctx=ctx)
        return await self.requester.run(query)


    async def update(self, ctx, value, row_guessed):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (int): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        sign = '' if value >= 0 else '-'
        int_part, dec_part = str(value).split('.')
        int_part = int(int_part)

        sign_cost = 1.0 if int_part == 0 else 0.0
        int_cost = await self.int_binary_row_collector.stats.success_cost()
        dec_cost = await self.dec_text_row_collector.stats.success_cost()
        await self.stats.update(is_success=True, cost=sign_cost + int_cost + dec_cost)

        await self.int_binary_row_collector.update(ctx, value=int_part, row_guessed=row_guessed)

        ctx = self._to_text_ctx(ctx, start_offset=len(f'{sign}{abs(int_part)}.'))
        await self.dec_text_row_collector.update(ctx, value=dec_part, row_guessed=row_guessed)


    @staticmethod
    def _to_text_ctx(ctx, start_offset):
        '''Helper function to convert NumericContext to TextContext for collection of float decimal parts.

        Params:
            ctx (NumericContext): context to convert
            start_offset (int): start collecting chars at this offset

        Returns:
            TextContext: converted context
        '''
        kwargs = asdict(ctx)
        kwargs.pop('rows_are_positive')
        kwargs['cast_to'] = 'text'
        kwargs['start_offset'] = start_offset
        kwargs['rows_are_ascii'] = True
        kwargs['row_is_ascii'] = True
        return TextContext(**kwargs)
