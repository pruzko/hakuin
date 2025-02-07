from hakuin.search_algorithms import TreeSearch
from hakuin.utils.huffman import make_tree

from .char_collector import CharCollector



class ModelCharCollector(CharCollector):
    '''Base class for model char collectors.'''
    QUERY_CLS_NAME = None


    def __init__(self, requester, dbms, model, adaptive=True):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            model (Model): language model
            adaptive (bool): adaptively train model on collected characters
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.model = model
        self.adaptive = adaptive


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str|None: collected char or None on fail
        '''
        probs = await self.model.predict(buffer=ctx.buffer)

        return await TreeSearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.query_cls_lookup(self.QUERY_CLS_NAME),
            tree=make_tree(probs),
        ).run(ctx)


    async def update(self, ctx, value):
        '''Emulates the collection and updates the stats. If adaptive, also trains the model.

        Param:
            ctx (Context): collection context
            value (str): collected char
        '''
        await super().update(ctx=ctx, value=value)
        if self.adaptive:
            await self.model.fit_char(char=value, buffer=ctx.buffer)



class TextModelCharCollector(ModelCharCollector):
    '''Text model char collector.'''
    QUERY_CLS_NAME = 'QueryTextCharInString'



class BlobModelCharCollector(ModelCharCollector):
    '''Blob model char collector.'''
    QUERY_CLS_NAME = 'QueryBlobCharInString'
