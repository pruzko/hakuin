from hakuin import Model
from hakuin.utils import CHARSET_DIGITS

from .collector import Collector
from .string_collectors import TextCollector
from .row_collectors import IntAutoIncRowCollector, IntBinaryRowCollector, IntRowCollector, FloatRowCollector, TextRowCollector



class NumericCollector(Collector):
    '''Numeric column collector. Column collectors repeatidly run row collectors to extract rows.'''
    async def check_rows(self, ctx):
        '''Checks rows for various properties and sets the appropriate ctx settings.

        Params:
            ctx (Context): collection context
        '''
        await super().check_rows(ctx)
        ctx.rows_are_positive = await self.check_rows_are_positive(ctx)


    async def check_rows_are_positive(self, ctx):
        '''Checks if rows are positive.

        Params:
            ctx (NumericContext): collection context

        Returns:
            bool: rows are positive flag
        '''
        if ctx.rows_are_positive is None:
            query = self.dbms.QueryRowsArePositive(dbms=self.dbms)
            return await self.requester.run(query=query, ctx=ctx)

        return ctx.rows_are_positive



    class Builder(Collector.Builder):
        pass



class IntCollector(NumericCollector):
    class Builder(NumericCollector.Builder):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.int_binary_row_collector = None
            self.int_auto_inc_row_collector = None


        def add_binary_row_collector(self):
            self.int_binary_row_collector = IntBinaryRowCollector(
                requester=self.requester,
                dbms=self.dbms,
            )


        def add_auto_inc_row_collector(self):
            self.int_auto_inc_row_collector = IntAutoIncRowCollector(
                requester=self.requester,
                dbms=self.dbms,
            )


        def build_row_collector(self):
            if not self.int_binary_row_collector:
                self.add_binary_row_collector()

            self.row_collector = IntRowCollector(
                requester=self.requester,
                dbms=self.dbms,
                int_binary_row_collector=self.int_binary_row_collector,
                int_auto_inc_row_collector=self.int_auto_inc_row_collector,
            )


        def build(self):
            if not self.row_collector:
                self.build_row_collector()

            return IntCollector(
                requester=self.requester,
                dbms=self.dbms,
                n_tasks=self.n_tasks,
                row_collector=self.row_collector,
                guessing_row_collector=self.guessing_row_collector,
            )



class FloatCollector(NumericCollector):
    class Builder(NumericCollector.Builder):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.int_binary_row_collector = None
            self.dec_text_row_collector = None
            self.text_builder = TextCollector.Builder(**kwargs)


        def add_binary_row_collector(self):
            self.int_binary_row_collector = IntBinaryRowCollector(
                requester=self.requester,
                dbms=self.dbms,
            )


        def add_list_char_collector(self, charset=None):
            self.text_builder.add_list_char_collector(
                charset=charset or CHARSET_DIGITS,
            )


        def add_unigram_char_collector(self, model=None, adaptive=True,):
            self.text_builder.add_unigram_char_collector(
                model=model,
                adaptive=adaptive,
            )


        def add_fivegram_char_collector(self, model=None, adaptive=True,):
            self.text_builder.add_fivegram_char_collector(
                model=model,
                adaptive=adaptive,
            )


        def build_dec_text_row_collector(self, query_cls_row_is_ascii=None):
            self.dec_text_row_collector = self.text_builder.build_row_collector()
            return self.dec_text_row_collector


        def build_row_collector(self):
            if not self.int_binary_row_collector:
                self.add_binary_row_collector()

            if not self.dec_text_row_collector:
                self.build_dec_text_row_collector()

            self.row_collector = FloatRowCollector(
                requester=self.requester,
                dbms=self.dbms,
                int_binary_row_collector=self.int_binary_row_collector,
                dec_text_row_collector=self.dec_text_row_collector,
            )
            return self.row_collector


        def build(self):
            if not self.row_collector:
                self.build_row_collector()

            return FloatCollector(
                requester=self.requester,
                dbms=self.dbms,
                n_tasks=self.n_tasks,
                row_collector=self.row_collector,
                guessing_row_collector=self.guessing_row_collector,
            )
