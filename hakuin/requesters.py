import asyncio
import functools
from abc import ABCMeta, abstractmethod



class Requester(metaclass=ABCMeta):
    '''Requester base class. Requesters inject queries and extract their results.'''
    def __init__(self):
        '''Constructor.'''
        self._n_requests = 0
        self._lock = asyncio.Lock()


    async def run(self, query, ctx):
        '''Runs the requester and increments the request counter.

        Params:
            ctx (Context): collection context
            query (Query): query to be injected

        Returns:
            bool: query result
        '''
        res = await self.request(query=query, ctx=ctx)
        async with self._lock:
            self._n_requests += 1
        return res


    @abstractmethod
    async def request(self, query, ctx):
        '''Injects the query and extracts its result.

        Params:
            query (Query): query to be injected
            ctx (Context): collection context

        Returns:
            bool: query result
        '''
        raise NotImplementedError()


    async def n_requests(self):
        '''Coroutine-safe getter for the request counter.

        Returns:
            int: request counter
        '''
        async with self._lock:
            return self._n_requests


    async def initialize(self):
        '''Initialization hook invoked by "hk.py".'''
        pass


    async def cleanup(self):
        '''Clean-up hook invoked by "hk.py".'''
        pass



class EmulationRequester(Requester):
    '''Requester stub for emulation.'''
    def __init__(self, correct):
        '''Constructor.

        Params:
            correct (value): correct value
        '''
        super().__init__()
        self.correct = correct


    async def request(self, query, ctx):
        '''Emulates the query without sending requests.

        Params:
            query (Query): query to be injected
            ctx (Context): collection context

        Returns:
            bool: query result
        '''
        return query.emulate(correct=self.correct)
