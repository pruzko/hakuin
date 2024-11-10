from hakuin.search_algorithms import ListBinarySearch
from hakuin.utils import EOS

from .char_collector import CharCollector



class ListBinaryCharCollector(CharCollector):
    def __init__(self, requester, dbms, charset, query_cls_char_in_string):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            charset (list): list of possible characters
            query_cls_char_in_string (DBMS.Query): query class
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.charset = charset
        self.query_cls_char_in_string = query_cls_char_in_string

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
            query_cls=self.query_cls_char_in_string,
            values=self.charset,
        ).run(ctx)



class TextListBinaryCharCollector(ListBinaryCharCollector):
    def __init__(self, requester, dbms, charset, query_cls_char_in_string=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            charset (list): list of possible characters
            query_cls_char_in_string (DBMS.Query): query class (default QueryCharInString)
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            charset=charset,
            query_cls_char_in_string=query_cls_char_in_string or dbms.QueryTextCharInString
        )



class BlobListBinaryCharCollector(ListBinaryCharCollector):
    def __init__(self, requester, dbms, charset, query_cls_char_in_string=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            charset (list): list of possible characters
            query_cls_char_in_string (DBMS.Query): query class (default QueryCharInString)
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            charset=charset,
            query_cls_char_in_string=query_cls_char_in_string or dbms.QueryBlobCharInString
        )