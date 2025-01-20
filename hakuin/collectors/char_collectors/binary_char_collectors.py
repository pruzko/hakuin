from abc import abstractmethod

from hakuin.search_algorithms import BinarySearch
from hakuin.utils import BYTE_MAX, ASCII_MAX, UNICODE_MAX, EOS

from .char_collector import CharCollector



class BinaryCharCollector(CharCollector):
    '''Char collector based on numerical binary search.'''
    def __init__(self, requester, dbms, query_cls_char_lt):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            query_cls_char_lt (DBMS.Query): query class (default QueryCharLt)
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.query_cls_char_lt = query_cls_char_lt


    @abstractmethod
    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str|bytes: collected char
        '''
        raise NotImplementedError



class TextBinaryCharCollector(BinaryCharCollector):
    def __init__(self, requester, dbms, query_cls_char_lt=None, query_cls_char_is_ascii=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            query_cls_char_lt (DBMS.Query): query class (default QueryTextCharLt)
            query_cls_char_is_ascii (DBMS.Query): query class (default QueryCharIsAscii)
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            query_cls_char_lt=query_cls_char_lt or dbms.QueryTextCharLt
        )
        self.query_cls_char_is_ascii = query_cls_char_is_ascii or dbms.QueryCharIsAscii


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str|bytes: collected char
        '''
        if ctx.row_is_ascii or await self.check_char_is_ascii(requester=requester, ctx=ctx):
            res = await BinarySearch(
                requester=requester,
                dbms=self.dbms,
                query_cls=self.query_cls_char_lt,
                lower=0,
                upper=ASCII_MAX + 2,
                find_lower=False,
                find_upper=False,
            ).run(ctx)
            return chr(res) if res <= ASCII_MAX else EOS

        res = await BinarySearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.query_cls_char_lt,
            lower=ASCII_MAX + 1,
            upper=UNICODE_MAX + 2,
            find_lower=False,
            find_upper=False,
        ).run(ctx)
        return chr(res) if res <= UNICODE_MAX else EOS


    async def check_char_is_ascii(self, requester, ctx):
        '''Checks if char is ascii.

        Params:
            ctx (Context): collection context

        Returns:
            bool: char is ascii flag
        '''
        if ctx.row_is_ascii is True:
            return True

        query = self.query_cls_char_is_ascii(dbms=self.dbms)
        return await requester.run(ctx, query=query)



class BlobBinaryCharCollector(BinaryCharCollector):
    def __init__(self, requester, dbms, query_cls_char_lt=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            query_cls_char_lt (DBMS.Query): query class (default QueryBlobCharLt)
            query_cls_char_is_ascii (DBMS.Query): query class (default QueryBlobCharIsAscii)
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            query_cls_char_lt=query_cls_char_lt or dbms.QueryBlobCharLt
        )


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str|bytes: collected char
        '''
        res = await BinarySearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.query_cls_char_lt,
            lower=0,
            upper=BYTE_MAX + 2,
            find_lower=False,
            find_upper=False,
        ).run(ctx)
        return res.to_bytes(1, byteorder='big') if res <= BYTE_MAX else EOS
