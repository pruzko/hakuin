from hakuin.search_algorithms import ListBinarySearch
from hakuin.utils import EOS

from .CharCollector import CharCollector



class ListBinaryCharCollector(CharCollector):
    def __init__(self, requester, dbms, charset):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            charset (list): list of possible characters
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.charset = charset

        if EOS not in self.charset:
            self.charset.append(EOS)


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str: collected char
        '''
        return await ListBinarySearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.QueryCharInString,
            values=self.charset,
        ).run(ctx)
