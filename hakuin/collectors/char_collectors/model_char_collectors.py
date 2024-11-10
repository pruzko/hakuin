from hakuin.search_algorithms import TreeSearch
from hakuin.utils import tokenize
from hakuin.utils.huffman import make_tree

from .char_collector import CharCollector



class ModelCharCollector(CharCollector):
    def __init__(self, requester, dbms, model, adaptive, query_cls_char_in_string):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            model (Model): language model
            adaptive (bool): adaptively train model on collected characters
            query_cls_char_in_string (DBMS.Query): query class (default QueryCharInString)
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.model = model
        self.adaptive = adaptive
        self.query_cls_char_in_string = query_cls_char_in_string


    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str|None: collected char or None on fail
        '''
        model_ctx = tokenize(ctx.buffer, add_eos=False)
        scores = await self.model.scores(context=model_ctx)

        return await TreeSearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.query_cls_char_in_string,
            tree=make_tree(scores),
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
    def __init__(self, requester, dbms, model, adaptive=True, query_cls_char_in_string=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            model (Model): language model
            adaptive (bool): adaptively train model on collected characters
            query_cls_char_in_string (DBMS.Query): query class (default QueryCharInString)
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            model=model,
            adaptive=adaptive,
            query_cls_char_in_string=query_cls_char_in_string or dbms.QueryTextCharInString
        )



class BlobModelCharCollector(ModelCharCollector):
    def __init__(self, requester, dbms, model, adaptive=True, query_cls_char_in_string=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            model (Model): language model
            adaptive (bool): adaptively train model on collected characters
            query_cls_char_in_string (DBMS.Query): query class (default QueryCharInString)
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            model=model,
            adaptive=adaptive,
            query_cls_char_in_string=query_cls_char_in_string or dbms.QueryBlobCharInString
        )
