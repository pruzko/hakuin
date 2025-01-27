from abc import abstractmethod

from hakuin import Model

from .collector import Collector
from .char_collectors import BlobBinaryCharCollector, BlobListCharCollector, BlobModelCharCollector
from .char_collectors import TextBinaryCharCollector, TextListCharCollector, TextModelCharCollector
from .row_collectors import MetaTextRowCollector, BlobRowCollector, TextRowCollector



class StringCollector(Collector):
    '''String (text/blob) column collector. Column collectors repeatidly run row collectors to extract rows.'''
    class Builder(Collector.Builder):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.binary_char_collector = None
            self.list_char_collector = None
            self.unigram_char_collector = None
            self.fivegram_char_collector = None
            self.cls_collector = None
            self.cls_row_collector = None
            self.cls_binary_char_collector = None
            self.cls_list_char_collector = None
            self.cls_model_char_collector = None


        def add_binary_char_collector(self):
            self.binary_char_collector = self.cls_binary_char_collector(
                requester=self.requester,
                dbms=self.dbms,
            )


        def add_list_char_collector(self, charset):
            self.list_char_collector = self.cls_list_char_collector(
                requester=self.requester,
                dbms=self.dbms,
                charset=charset,
            )


        def add_unigram_char_collector(self, model=None, adaptive=True):
            self.unigram_char_collector = self.cls_model_char_collector(
                requester=self.requester,
                dbms=self.dbms,
                model=model or Model(1),
                adaptive=adaptive,
            )


        def add_fivegram_char_collector(self, model=None, adaptive=True):
            self.fivegram_char_collector = self.cls_model_char_collector(
                requester=self.requester,
                dbms=self.dbms,
                model=model or Model(5),
                adaptive=adaptive,
            )


        def build_row_collector(self, **kwargs):
            if not self.binary_char_collector and not self.list_char_collector:
                self.add_binary_char_collector()

            self.row_collector = self.cls_row_collector(
                requester=self.requester,
                dbms=self.dbms,
                binary_char_collector=self.binary_char_collector or self.list_char_collector,
                unigram_char_collector=self.unigram_char_collector,
                fivegram_char_collector=self.fivegram_char_collector,
                **kwargs,
            )
            return self.row_collector


        def build(self, **kwargs):
            if not self.row_collector:
                self.build_row_collector()

            return self.cls_collector(
                requester=self.requester,
                dbms=self.dbms,
                n_tasks=self.n_tasks,
                row_collector=self.row_collector,
                guessing_row_collector=self.guessing_row_collector,
                **kwargs,
            )



class TextCollector(StringCollector):
    async def check_rows(self, ctx):
        '''Checks rows for various properties and sets the appropriate ctx settings.

        Params:
            ctx (Context): collection context
        '''
        await super().check_rows(ctx)
        ctx.rows_are_ascii = await self.check_rows_are_ascii(ctx)


    async def check_rows_are_ascii(self, ctx):
        '''Checks if rows are ascii.

        Params:
            ctx (StringContext): collection context

        Returns:
            bool: rows are ascii flag
        '''
        if ctx.rows_are_ascii is None:
            query = self.dbms.QueryRowsAreAscii(dbms=self.dbms)
            return await self.requester.run(query=query, ctx=ctx)

        return ctx.rows_are_ascii



    class Builder(StringCollector.Builder):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.cls_collector = TextCollector
            self.cls_row_collector = TextRowCollector
            self.cls_binary_char_collector = TextBinaryCharCollector
            self.cls_list_char_collector = TextListCharCollector
            self.cls_model_char_collector = TextModelCharCollector


        # def build_row_collector(self, query_cls_row_is_ascii=None):
        #     return super().build_row_collector(query_cls_row_is_ascii=query_cls_row_is_ascii)


        # def build(self, query_cls_rows_are_ascii=None):
        #     return super().build(query_cls_rows_are_ascii=query_cls_rows_are_ascii)



    class MetaBuilder(Builder):
        def build_row_collector(self):
            if not self.binary_char_collector and not self.list_char_collector:
                self.add_binary_char_collector()

            self.row_collector = MetaTextRowCollector(
                requester=self.requester,
                dbms=self.dbms,
                binary_char_collector=self.binary_char_collector or self.list_char_collector,
                fivegram_char_collector=self.fivegram_char_collector,
            )
            return self.row_collector


        def build(self):
            self.guessing_row_collector = None
            return super().build()
            # return super().build(query_cls_rows_are_ascii=query_cls_rows_are_ascii)



class BlobCollector(StringCollector):
    class Builder(StringCollector.Builder):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.cls_collector = BlobCollector
            self.cls_row_collector = BlobRowCollector
            self.cls_binary_char_collector = BlobBinaryCharCollector
            self.cls_list_char_collector = BlobListCharCollector
            self.cls_model_char_collector = BlobModelCharCollector
