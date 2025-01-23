import asyncio
from abc import ABCMeta, abstractmethod

from hakuin.utils import split_at



class SearchAlgorithm(metaclass=ABCMeta):
    '''Search algorithm base class.'''
    def __init__(self, requester, dbms, query_cls):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            query_cls (Type[Query]): Query class
        '''
        self.requester = requester
        self.dbms = dbms
        self.query_cls = query_cls


    @abstractmethod
    async def run(self, ctx):
        '''Runs the search algorithm.'''
        raise NotImplementedError()


class BinarySearch(SearchAlgorithm):
    '''Exponential and binary search for numeric values.'''
    def __init__(self, requester, dbms, query_cls, lower=0, upper=16, find_lower=False, find_upper=False):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            query_cls (Type[Query]): Query class
            lower (int): lower bound of search range (included)
            upper (int): upper bound of search range (excluded)
            find_lower (bool): exponentially expands the lower bound until the correct value is within
            find_upper (bool): exponentially expands the upper bound until the correct value is within
        '''
        assert lower != upper, f'Lower and uppper bounds cannot be the same: {lower}'

        super().__init__(requester, dbms, query_cls)
        self.lower = lower
        self.upper = upper
        self.find_lower = find_lower
        self.find_upper = find_upper
        self._lower = None
        self._upper = None
        self._f_lower = None
        self._f_upper = None


    async def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): collection context

        Returns:
            int|None: inferred number or None on fail
        '''
        self._lower = self.lower
        self._upper = self.upper
        self._f_lower = self.find_lower
        self._f_upper = self.find_upper
        step = self._upper - self._lower
        if self._f_lower:
            await self._find_lower(ctx, step=step)
        if self._f_upper:
            await self._find_upper(ctx, step=step)

        return await self._search(ctx, lower=self._lower, upper=self._upper)


    async def _find_lower(self, ctx, step):
        '''Exponentially expands the lower bound until the correct value is within.

        Params:
            ctx (Context): collection context
            step (int): initial step
        '''
        query = self.query_cls(dbms=self.dbms, n=self._lower)
        if not await self.requester.run(query=query, ctx=ctx):
            return

        self._upper = self._lower
        self._f_upper = False
        self._lower -= step
        return await self._find_lower(ctx, step=step * 2)


    async def _find_upper(self, ctx, step):
        '''Exponentially expands the upper bound until the correct value is within.

        Params:
            ctx (Context): collection context
            step (int): initial step
        query = self.query_cls(dbms=self.dbms, n=n)
        '''
        query = self.query_cls(dbms=self.dbms, n=self._upper)
        if await self.requester.run(query=query, ctx=ctx):
            return

        self._lower = self._upper
        self._upper += step
        return await self._find_upper(ctx, step=step * 2)


    async def _search(self, ctx, lower, upper):
        '''Numeric binary search.

        Params:
            ctx (Context): collection context
            lower (int): lower bound
            upper (int): upper bound

        Returns:
            int: inferred number
        '''
        if lower + 1 == upper:
            return lower

        middle = (lower + upper) // 2
        query = self.query_cls(dbms=self.dbms, n=middle)
        if await self.requester.run(query=query, ctx=ctx):
            return await self._search(ctx, lower=lower, upper=middle)

        return await self._search(ctx, lower=middle, upper=upper)



class ListBinarySearch(SearchAlgorithm):
    '''Binary search for lists of values.'''
    def __init__(self, requester, dbms, query_cls, values):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            query_cls (Type[Query]): Query class
            values (list): list of values to search
        '''
        super().__init__(requester, dbms, query_cls)
        self.values = values


    async def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): collection context

        Returns:
            value|None: inferred value or None on fail
        '''
        return await self._search(ctx, self.values)


    async def _search(self, ctx, values):
        if not values:
            return None

        if len(values) == 1:
            return values[0]

        left, right = split_at(values, len(values) // 2)

        query = self.query_cls(dbms=self.dbms, values=left)
        if await self.requester.run(query=query, ctx=ctx):
            return await self._search(ctx, left)

        return await self._search(ctx, right)



class TreeSearch(SearchAlgorithm):
    '''Huffman tree search.'''
    def __init__(self, requester, dbms, query_cls, tree):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
            query_cls (Type[Query]): Query class
            tree (utils.huffman.Node): Huffman tree to search
        '''
        super().__init__(requester, dbms, query_cls)        
        self.tree = tree


    async def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): collection context

        Returns:
            value|None: inferred value or None on fail
        '''
        return await self._search(ctx, tree=self.tree)


    async def _search(self, ctx, tree, in_tree=False):
        '''Tree search.
        
        Params:
            ctx (Context): collection context
            tree (utils.huffman.Node): Huffman tree to search
            in_tree (bool): True if the correct value is known to be in the tree

        Returns:
            value|None: inferred value or None on fail
        '''
        if tree is None:
            return None

        if tree.is_leaf:
            if in_tree:
                return tree.value

            query = self.query_cls(dbms=self.dbms, values=[tree.value])
            if await self.requester.run(query=query, ctx=ctx):
                return tree.value
            return None

        query = self.query_cls(dbms=self.dbms, values=tree.left.values())
        if await self.requester.run(query=query, ctx=ctx):
            return await self._search(ctx, tree=tree.left, in_tree=True)

        if tree.right is None:
            return None

        return await self._search(ctx, tree=tree.right, in_tree=in_tree)
