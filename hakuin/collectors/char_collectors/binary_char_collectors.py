from hakuin.search_algorithms import BinarySearch
from hakuin.utils import BYTE_MAX, ASCII_MAX, UNICODE_MAX, EOS

from .char_collector import CharCollector



class TextBinaryCharCollector(CharCollector):
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
                query_cls=self.dbms.QueryTextCharLt,
                lower=0,
                upper=ASCII_MAX + 2,
                find_lower=False,
                find_upper=False,
            ).run(ctx)
            return chr(res) if res <= ASCII_MAX else EOS

        res = await BinarySearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryTextCharLt,
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

        query = self.dbms.QueryCharIsAscii(dbms=self.dbms)
        return await requester.run(query=query, ctx=ctx)



class BlobBinaryCharCollector(CharCollector):
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
            query_cls=self.dbms.QueryBlobCharLt,
            lower=0,
            upper=BYTE_MAX + 2,
            find_lower=False,
            find_upper=False,
        ).run(ctx)
        return res.to_bytes(1, byteorder='big') if res <= BYTE_MAX else EOS
