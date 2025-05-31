import asyncio
import functools
from abc import ABCMeta, abstractmethod

import aiohttp



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
        try:
            return await self.request(query=query, ctx=ctx)
        finally:
            async with self._lock:
                self._n_requests += 1


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


    async def __aenter__(self):
        return self


    async def __aexit__(self, *args, **kwargs):
        pass



class SessionRequester(Requester):
    '''Requester with an AioHTTP.ClientSession.'''
    def __init__(self):
        super().__init__()
        self.session = None


    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self


    async def __aexit__(self, *args, **kwargs):
        if self.session:
            await self.session.close()
            self.session = None
        



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
