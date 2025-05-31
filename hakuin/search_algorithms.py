import asyncio
from abc import ABCMeta, abstractmethod

from hakuin.exceptions import ServerError



class SearchAlgorithm(metaclass=ABCMeta):
    '''Search algorithm base class.'''
    def __init__(self, requester, dbms, query_cls):
        '''Constructor.

        Params:
            requester (Requester): requester
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



class SectionSearch(SearchAlgorithm):
    '''Exponential and section search for numeric values.'''
    def __init__(
        self, requester, dbms, query_cls, lower=0, upper=16, find_lower=False, find_upper=False
    ):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            query_cls (Type[Query]): Query class
            lower (int): lower bound of search range (included)
            upper (int): upper bound of search range (excluded)
            find_lower (bool): exponentially expands the lower bound
                until the correct value is within
            find_upper (bool): exponentially expands the upper bound
                until the correct value is within
        '''
        assert lower < upper, f'Lower bound must be lower than the upper bound.'

        super().__init__(requester, dbms, query_cls)
        self.lower = lower
        self.upper = upper
        self.find_lower = find_lower
        self.find_upper = find_upper
        self._lower = None
        self._upper = None
        self._f_lower = None
        self._f_upper = None
        self._step = None


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

        if self._f_lower:
            await self._find_lower(ctx)
        if self._f_upper:
            await self._find_upper(ctx)

        return await self._search(ctx)


    async def _find_lower(self, ctx):
        '''Exponentially expands the lower bound until the correct value is within.

        Params:
            ctx (Context): collection context
        '''
        self._step = self._upper - self._lower

        while True:
            query = self.query_cls(dbms=self.dbms, n=self._lower)
            if not await self.requester.run(query=query, ctx=ctx):
                return
            self._expand_lower()


    async def _find_upper(self, ctx):
        '''Exponentially expands the upper bound until the correct value is within.

        Params:
            ctx (Context): collection context
        '''
        self._step = self._upper - self._lower

        while True:
            query = self.query_cls(dbms=self.dbms, n=self._upper)
            if await self.requester.run(query=query, ctx=ctx):
                return
            self._expand_upper()


    def _expand_lower(self, factor=2):
        '''Helper function to expand the lower bound of the search range.

        Params:
            factor (int): step multiplication factor
        '''
        self._upper = self._lower
        self._f_upper = False
        self._lower -= self._step
        self._step *= factor


    def _expand_upper(self, factor=2):
        '''Helper function to expand the upper bound of the search range.

        Params:
            factor (int): step multiplication factor
        '''
        self._lower = self._upper
        self._f_lower = False
        self._upper += self._step
        self._step *= factor


    async def _search(self, ctx):
        '''Numeric section search.

        Params:
            ctx (Context): collection context

        Returns:
            int: inferred number
        '''
        while True:
            if self._lower + 1 == self._upper:
                return self._lower

            section = (self._lower + self._upper) // 2

            query = self.query_cls(dbms=self.dbms, n=section)
            if await self.requester.run(query=query, ctx=ctx):
                self._upper = section
            else:
                self._lower = section



class TernarySectionSearch(SectionSearch):
    '''Ternary exponential and section search for numeric values.'''
    async def _find_lower(self, ctx):
        '''Exponentially expands the lower bound until the correct value is within.

        Params:
            ctx (Context): collection context
        '''
        self._step = self._upper - self._lower

        while True:
            query1 = self.query_cls(dbms=self.dbms, n=self._lower - self._step)
            query2 = self.query_cls(dbms=self.dbms, n=self._lower)
            query = self.dbms.QueryTernary(dbms=self.dbms, query1=query1, query2=query2)

            try:
                if await self.requester.run(query=query, ctx=ctx):
                    self._expand_lower(factor=3)
                    self._expand_lower(factor=3)
                else:
                    self._expand_lower(factor=3)
                    return
            except ServerError:
                return


    async def _find_upper(self, ctx):
        '''Exponentially expands the upper bound until the correct value is within.

        Params:
            ctx (Context): collection context
        '''
        self._step = self._upper - self._lower

        while True:
            query1 = self.query_cls(dbms=self.dbms, n=self._upper)
            query2 = self.query_cls(dbms=self.dbms, n=self._upper + self._step)
            query = self.dbms.QueryTernary(dbms=self.dbms, query1=query1, query2=query2)

            try:
                if await self.requester.run(query=query, ctx=ctx):
                    return
                else:
                    self._expand_upper(factor=3)
                    return
            except ServerError:
                self._expand_upper(factor=3)
                self._expand_upper(factor=3)


    async def _search(self, ctx):
        '''Numeric section search.

        Params:
            ctx (Context): collection context

        Returns:
            int: inferred number
        '''
        while True:
            if self._lower + 1 == self._upper:
                return self._lower

            diff = (self._upper - self._lower) // 3
            diff = diff or 1
            section1 = self._lower + diff
            section2 = self._lower + diff * 2

            query1 = self.query_cls(dbms=self.dbms, n=section1)
            query2 = self.query_cls(dbms=self.dbms, n=section2)
            query = self.dbms.QueryTernary(dbms=self.dbms, query1=query1, query2=query2)

            try:
                if await self.requester.run(query=query, ctx=ctx):
                    self._upper = section1
                else:
                    self._lower = section1
                    self._upper = section2
            except ServerError:
                self._lower = section2



class TreeSearch(SearchAlgorithm):
    '''Huffman tree search.'''
    def __init__(self, requester, dbms, query_cls, tree, in_tree=False):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            query_cls (Type[Query]): Query class
            tree (Node): Huffman tree to search
            in_tree (bool): value is guaranteed to be in the tree flag
            is_ternary (bool): is ternary flag
        '''
        super().__init__(requester, dbms, query_cls)        
        self.tree = tree
        self.in_tree = in_tree


    async def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): collection context

        Returns:
            value|None: inferred value or None on fail
        '''
        return await self._search(ctx)


    async def _search(self, ctx):
        '''Tree search.
        
        Params:
            ctx (Context): collection context

        Returns:
            value|None: inferred value or None on fail
        '''
        tree = self.tree
        in_tree = self.in_tree

        while True:
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
                tree = tree.left
                in_tree = True
            else:
                tree = tree.right



class TernaryTreeSearch(TreeSearch):
    '''Ternary Huffman tree search.'''
    async def _search(self, ctx):
        '''Tree search.
        
        Params:
            ctx (Context): collection context

        Returns:
            value|None: inferred value or None on fail
        '''
        tree = self.tree
        in_tree = self.in_tree

        while True:
            if tree is None:
                return None

            if tree.is_leaf:
                if in_tree:
                    return tree.value

                query = self.query_cls(dbms=self.dbms, values=[tree.value])
                if await self.requester.run(query=query, ctx=ctx):
                    return tree.value
                return None

            query1 = self.query_cls(dbms=self.dbms, values=tree.left.values())
            query2 = self.query_cls(dbms=self.dbms, values=tree.middle.values())
            query = self.dbms.QueryTernary(dbms=self.dbms, query1=query1, query2=query2)

            try:
                if await self.requester.run(query=query, ctx=ctx):
                    tree = tree.left
                    in_tree = True
                else:
                    tree = tree.middle
                    in_tree = True
            except ServerError:
                    tree = tree.right
