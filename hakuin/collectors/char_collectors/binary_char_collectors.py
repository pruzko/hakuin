from hakuin.collectors.checks import check_flag
from hakuin.search_algorithms import BinarySearch
from hakuin.utils import BYTE_MAX, ASCII_MAX, UNICODE_MAX, Symbol

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
        char_is_ascii = await check_flag(
            requester=requester,
            dbms=self.dbms,
            ctx=ctx,
            name='char_is_ascii',
            true_if_true='row_is_ascii',
        )

        if char_is_ascii:
            res = await BinarySearch(
                requester=requester,
                dbms=self.dbms,
                query_cls=self.dbms.QueryTextCharLt,
                lower=0,
                upper=ASCII_MAX + 2,
                find_lower=False,
                find_upper=False,
            ).run(ctx)
            return chr(res) if res <= ASCII_MAX else Symbol.EOS

        res = await BinarySearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryTextCharLt,
            lower=ASCII_MAX + 1,
            upper=UNICODE_MAX + 2,
            find_lower=False,
            find_upper=False,
        ).run(ctx)
        return chr(res) if res <= UNICODE_MAX else Symbol.EOS



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
        return res.to_bytes(1, byteorder='big') if res <= BYTE_MAX else Symbol.EOS
