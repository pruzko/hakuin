import asyncio
import functools
from abc import ABCMeta, abstractmethod



class Requester(metaclass=ABCMeta):
    '''Abstract class for requesters. Requesters craft requests with
    injected queries, send them, and infer the query's results.
    '''
    def __init__(self):
        '''Constructor.'''
        self._n_requests = 0
        self._lock = asyncio.Lock()


    async def run(self, query):
        '''Calls request() and increments the request counter.

        Params:
            query (Query): query to be injected

        Returns:
            bool: query result
        '''
        res = await self.request(query)
        async with self._lock:
            self._n_requests += 1
        return res


    @abstractmethod
    async def request(self, query):
        '''Sends a request with injected query and infers its result.

        Params:
            query (Query): query to be injected

        Returns:
            bool: query result
        '''
        raise NotImplementedError()


    async def n_requests(self):
        '''Retrieves the request counter.

        Returns:
            int: request counter
        '''
        async with self._lock:
            return self._n_requests


    async def initialize(self):
        '''Async initialization. This method is called by "hk.py"'''
        pass


    async def cleanup(self):
        '''Async clean-up. This method is called by "hk.py"'''
        pass



class EmulationRequester(Requester):
    '''Requester for emulation.'''
    def __init__(self, correct):
        '''Constructor.

        Params:
            correct (value): correct value
        '''
        super().__init__()
        self.correct = correct


    async def request(self, query):
        '''Emulates inference without sending any requests.

        Params:
            query (Query): query to be injected

        Returns:
            bool: query result
        '''
        return query.emulate(correct=self.correct)
