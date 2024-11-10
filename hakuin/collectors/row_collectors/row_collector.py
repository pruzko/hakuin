from abc import ABCMeta, abstractmethod

from hakuin.collectors import Stats



class RowCollector(metaclass=ABCMeta):
    def __init__(self, requester, dbms):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
        '''
        self.requester = requester
        self.dbms = dbms
        self.stats = Stats()


    @abstractmethod
    async def run(self, ctx):
        '''Collects a single row.

        Params:
            ctx (Context): collection context

        Returns:
            value|None: collected row or None on failure
        '''
        raise NotImplementedError


    @abstractmethod
    async def update(self, ctx, value, row_guessed, **kwargs):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (value): collected row
            row_guessed (bool): row was successfully guessed flag
        '''
        raise NotImplementedError