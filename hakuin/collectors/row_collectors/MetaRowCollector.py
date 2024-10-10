from .StringRowCollector import StringRowCollector



class MetaRowCollector(StringRowCollector):
    def __init__(self, requester, dbms, binary_char_collector, fivegram_char_collector=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            binary_char_collector (BinaryCharCollector|ListBinaryCharCollector): binary char collector
            fivegram_char_collector (ModelCharCollector|None): fivegram char collector
        '''
        super().__init__(
            requester=requester,
            dbms=dbms,
            binary_char_collector=binary_char_collector,
            fivegram_char_collector=fivegram_char_collector,
        )


    async def collect_char(self, ctx):
        '''Collects a single char.

        Params:
            ctx (Context): collection context

        Returns:
            str: collected char
        '''
        res = None
        if self.fivegram_char_collector:
            res = await self.fivegram_char_collector.run(ctx)
        if res is None:
            res = await self.binary_char_collector.run(ctx)

        return res


    async def update(self, ctx, value, row_guessed):
        '''Does nothing. Meta collectors are stateless.'''
        pass
