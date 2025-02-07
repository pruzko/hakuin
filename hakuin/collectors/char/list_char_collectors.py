from hakuin.search_algorithms import ListSearch
from hakuin.utils import Symbol

from .char_collector import CharCollector



class ListCharCollector(CharCollector):
    '''Base class for list char collectors.'''
    QUERY_CLS_LOOKUP = None


    def __init__(self, requester, dbms, charset):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            charset (list): list of possible characters
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.charset = charset


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str: collected char
        '''
        return await ListSearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.query_cls(self.QUERY_CLS_LOOKUP),
            values=self.charset,
        ).run(ctx)


class TextListCharCollector(ListCharCollector):
    '''Text list char collector.'''
    QUERY_CLS_LOOKUP = 'text_char_in_string'



class BlobListCharCollector(ListCharCollector):
    '''Blob list char collector.'''
    QUERY_CLS_LOOKUP = 'blob_char_in_string'
