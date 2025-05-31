from hakuin.search_algorithms import TreeSearch, TernaryTreeSearch
from hakuin.tree import make_balanced_tree
from hakuin.utils import Symbol

from .char_collector import CharCollector



class ListCharCollector(CharCollector):
    '''Base class for list char collectors.'''
    QUERY_CLS_NAME = None


    def __init__(self, requester, dbms, charset, use_ternary=False):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            charset (list): list of possible characters
            use_ternary (bool): use ternary search flag
        '''
        super().__init__(requester=requester, dbms=dbms, use_ternary=use_ternary)
        self.charset = charset


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str: collected char
        '''
        SearchAlg = TernaryTreeSearch if self.use_ternary else TreeSearch
        return await SearchAlg(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.query_cls_lookup(self.QUERY_CLS_NAME),
            tree=make_balanced_tree(values=self.charset, use_ternary=self.use_ternary),
            in_tree=True,
        ).run(ctx)



class TextListCharCollector(ListCharCollector):
    '''Text list char collector.'''
    QUERY_CLS_NAME = 'QueryTextCharInString'



class BlobListCharCollector(ListCharCollector):
    '''Blob list char collector.'''
    QUERY_CLS_NAME = 'QueryBlobCharInString'
