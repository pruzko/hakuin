from hakuin import Model
from hakuin.utils import CHARSET_DIGITS

from .Collector import Collector, CollectorBuilder
from .char_collectors import ListBinaryCharCollector, ModelCharCollector
from .row_collectors import IntAutoIncRowCollector, IntBinaryRowCollector, IntRowCollector, FloatRowCollector, StringRowCollector



class NumericCollector(Collector):
    '''Numeric column collector. Column collectors repeatidly run row collectors to extract rows.'''
    async def check_rows(self, ctx):
        '''Checks rows for various properties and updates the collection context.

        Params:
            ctx (IntContext): collection context
        '''
        await super().check_rows(ctx)
        if ctx.rows_are_positive is None:
            ctx.rows_are_positive = await self.check_rows_are_positive(ctx)


    async def check_rows_are_positive(self, ctx):
        '''Checks if rows are positive.

        Params:
            ctx (IntContext): collection context

        Returns:
            bool: rows are positive flag
        '''
        query = self.dbms.QueryRowsArePositive(dbms=self.dbms, ctx=ctx)
        return await self.requester.run(query)




class IntCollectorBuilder(CollectorBuilder):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.int_binary_row_collector = IntBinaryRowCollector(requester=self.requester, dbms=self.dbms)
        self.int_auto_inc_row_collector = None


    def add_auto_inc(self):
        self.int_auto_inc_row_collector = IntAutoIncRowCollector(requester=self.requester, dbms=self.dbms)


    def build(self):
        row_collector = IntRowCollector(
            requester=self.requester,
            dbms=self.dbms,
            int_binary_row_collector=self.int_binary_row_collector,
            int_auto_inc_row_collector=self.int_auto_inc_row_collector,
        )
        return NumericCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
            row_collector=row_collector,
            guessing_row_collector=self.guessing_row_collector,
        )



class FloatCollectorBuilder(CollectorBuilder):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.int_binary_row_collector = IntBinaryRowCollector(
            requester=self.requester,
            dbms=self.dbms,
            query_cls_int_lt=self.dbms.QueryFloatIntLt,
        )
        self.binary_char_collector = ListBinaryCharCollector(
            requester=self.requester,
            dbms=self.dbms,
            charset=CHARSET_DIGITS,
        )
        self.unigram_char_collector = None
        self.fivegram_char_collector = None


    def add_unigram(self):
        self.unigram_char_collector = ModelCharCollector(
            requester=self.requester,
            dbms=self.dbms,
            model=Model(1),
            adaptive=True,
        )


    def add_fivegram(self):
        self.fivegram_char_collector = ModelCharCollector(
            requester=self.requester,
            dbms=self.dbms,
            model=Model(5),
            adaptive=True,
        )


    def build(self):
        dec_text_row_collector = StringRowCollector(
            requester=self.requester,
            dbms=self.dbms,
            binary_char_collector=self.binary_char_collector,
            unigram_char_collector=self.unigram_char_collector,
            fivegram_char_collector=self.fivegram_char_collector,
        )
        row_collector = FloatRowCollector(
            requester=self.requester,
            dbms=self.dbms,
            int_binary_row_collector=self.int_binary_row_collector,
            dec_text_row_collector=dec_text_row_collector,
        )
        return NumericCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
            row_collector=row_collector,
            guessing_row_collector=self.guessing_row_collector,
        )
