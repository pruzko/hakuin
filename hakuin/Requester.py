import os
from abc import ABCMeta, abstractmethod



class Requester(metaclass=ABCMeta):
    '''Abstract class for requesters. Requesters craft requests with
    injected queries, send them, and infer the query's results.
    '''
    @abstractmethod
    async def request(self, ctx, query):
        '''Sends a request with injected query and infers its result.

        Params:
            ctx (Context): inference context
            query (str): query to be injected

        Returns:
            bool: query result
        '''
        raise NotImplementedError()



class HKRequester(Requester):
    '''Abstract class for requesters that can be loaded by hk.py.'''
    def __init__(self):
        '''Constructor.'''
        self.n_requests = 0


    async def initialize(self):
        '''Async initialization. This method is called by "hk.py"'''
        pass


    async def cleanup(self):
        '''Async clean-up. This method is called by "hk.py"'''
        pass