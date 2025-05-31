from abc import ABCMeta, abstractmethod

from hakuin.collectors import Stats
from hakuin.requesters import EmulationRequester



class CharCollector(metaclass=ABCMeta):
    '''Char collector base class.'''
    def __init__(self, requester, dbms, use_ternary=False):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            use_ternary (bool): use ternary search flag
        '''
        self.requester = requester
        self.dbms = dbms
        self.use_ternary = use_ternary
        self.stats = Stats()


    async def run(self, ctx):
        '''Collects a single char.

        Params:
            ctx (Context): collection context

        Returns:
            str|bytes|None: collected char or None on failure
        '''
        return await self._run(requester=self.requester, ctx=ctx)


    async def _emulate(self, ctx, correct):
        '''Runs the collection without sending requests.

        Params:
            ctx (Context): collection context
            correct (str|bytes): correct character

        Returns:
            (int, str|bytes|None): request count and the result if available
        '''
        requester = EmulationRequester(correct=correct)
        res = await self._run(requester=requester, ctx=ctx)
        n_requests = await requester.n_requests()
        return n_requests, res


    @abstractmethod
    async def _run(self, requester, ctx):
        '''Collects a single char.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context

        Returns:
            str|bytes|None: collected char or None on fail
        '''
        raise NotImplementedError


    async def update(self, ctx, value):
        '''Updates the char collector with the newly collected char. This typically involves emulating
        the collection and updating the stats.

        Param:
            ctx (Context): collection context
            value (str|bytes): collected char
        '''
        cost, res = await self._emulate(ctx, correct=value)
        await self.stats.update(is_success=res is not None, cost=cost)
