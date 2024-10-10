from hakuin import Model

from .contexts import TextContext
from .Collector import Collector, CollectorBuilder
from .char_collectors import BinaryCharCollector, ModelCharCollector
from .row_collectors import MetaRowCollector, StringRowCollector



class StringCollector(Collector):
    '''String (text/blob) column collector. Column collectors repeatidly run row collectors to extract rows.'''
    async def check_rows(self, ctx):
        '''Checks rows for NULL and ASCII and updates the collection context.

        Params:
            ctx (StringContext): collection context
        '''
        await super().check_rows(ctx)
        if isinstance(ctx, TextContext) and ctx.rows_are_ascii is None:
            ctx.rows_are_ascii = await self.check_rows_are_ascii(ctx)


    async def check_rows_are_ascii(self, ctx):
        '''Checks if rows are ascii.

        Params:
            ctx (StringContext): collection context

        Returns:
            bool: rows are ascii flag
        '''
        query = self.dbms.QueryRowsAreAscii(dbms=self.dbms, ctx=ctx)
        return await self.requester.run(query)


    async def check_row(self, ctx):
        '''Checks the current row for various properties and updates the collection context.

        Params:
            ctx (StringContext): collection context
        '''
        await super().check_row(ctx)
        if isinstance(ctx, TextContext):
            ctx.row_is_ascii = ctx.rows_are_ascii or ctx.row_is_ascii
            if ctx.row_is_ascii is None:
                ctx.row_is_ascii = await self.check_row_is_ascii(ctx)


    async def check_row_is_ascii(self, ctx):
        '''Checks if row is ascii.

        Params:
            ctx (StringContext): collection context

        Returns:
            bool: row is ascii flag
        '''
        query = self.dbms.QueryRowIsAscii(dbms=self.dbms, ctx=ctx)
        return await self.requester.run(query)



class StringCollectorBuilder(CollectorBuilder):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.binary_char_collector = None
        self.unigram_char_collector = None
        self.fivegram_char_collector = None


    def add_binary(self, query_cls_char_lt=None):
        self.binary_char_collector = BinaryCharCollector(
            requester=self.requester,
            dbms=self.dbms,
            query_cls_char_lt=query_cls_char_lt,
        )


    def add_unigram(self, model=None, query_cls_char_in_string=None):
        model = model or Model(1)

        self.unigram_char_collector = ModelCharCollector(
            requester=self.requester,
            dbms=self.dbms,
            model=model,
            adaptive=True,
            query_cls_char_in_string=query_cls_char_in_string,
        )


    def add_fivegram(self, model=None, query_cls_char_in_string=None):
        model = model or Model(5)

        self.fivegram_char_collector = ModelCharCollector(
            requester=self.requester,
            dbms=self.dbms,
            model=model,
            adaptive=True,
            query_cls_char_in_string=query_cls_char_in_string,
        )


    def build(self, use_meta=False):
        if self.binary_char_collector is None:
            self.add_binary()

        if use_meta:
            row_collector = MetaRowCollector(
                requester=self.requester,
                dbms=self.dbms,
                binary_char_collector=self.binary_char_collector,
                fivegram_char_collector=self.fivegram_char_collector,
            )
        else:
            row_collector = StringRowCollector(
                requester=self.requester,
                dbms=self.dbms,
                binary_char_collector=self.binary_char_collector,
                unigram_char_collector=self.unigram_char_collector,
                fivegram_char_collector=self.fivegram_char_collector,
            )
        return StringCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
            row_collector=row_collector,
            guessing_row_collector=self.guessing_row_collector,
        )
