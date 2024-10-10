from hakuin.collectors import BlobContext
from hakuin.search_algorithms import BinarySearch
from hakuin.utils import BYTE_MAX, ASCII_MAX, UNICODE_MAX, EOS

from .CharCollector import CharCollector



class BinaryCharCollector(CharCollector):
    '''Char collector based on numerical binary search.'''
    def __init__(self, requester, dbms):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
        '''
        super().__init__(requester=requester, dbms=dbms)


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str|bytes: collected char
        '''
        if isinstance(ctx, BlobContext):
            res = await BinarySearch(
                requester=requester,
                dbms=self.dbms,
                query_cls=self.dbms.QueryCharLt,
                lower=0,
                upper=BYTE_MAX + 2,
                find_lower=False,
                find_upper=False,
            ).run(ctx)
            return res.to_bytes(1, byteorder='big') if res <= BYTE_MAX else EOS

        if ctx.row_is_ascii or await self.check_char_is_ascii(requester=requester, ctx=ctx):
            res = await BinarySearch(
                requester=requester,
                dbms=self.dbms,
                query_cls=self.dbms.QueryCharLt,
                lower=0,
                upper=ASCII_MAX + 2,
                find_lower=False,
                find_upper=False,
            ).run(ctx)
            return chr(res) if res <= ASCII_MAX else EOS

        res = await BinarySearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryCharLt,
            lower=0,
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
        query = self.dbms.QueryCharIsAscii(dbms=self.dbms, ctx=ctx)
        return await requester.run(query)
