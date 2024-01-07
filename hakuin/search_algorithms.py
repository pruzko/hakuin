from abc import ABCMeta, abstractmethod

from hakuin.utils import split_at




class SearchAlgorithm(metaclass=ABCMeta):
    '''Abstract class for various search algorithms.'''
    def __init__(self, requester, query_cb):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
        '''
        self.requester = requester
        self.query_cb = query_cb


    @abstractmethod
    async def run(self, ctx, correct=None):
        '''Runs the search algorithm.'''
        raise NotImplementedError()



class NumericBinarySearch(SearchAlgorithm):
    '''Exponential and binary search for numeric values.'''
    def __init__(self, requester, query_cb, lower=0, upper=16, find_lower=False, find_upper=False, correct=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            lower (int): lower bound of search range
            upper (int): upper bound of search range
            find_lower (bool): exponentially expands the lower bound until the correct value is within
            find_upper (bool): exponentially expands the upper bound until the correct value is within
            correct (int|None): correct value. If provided, the search is emulated
        '''
        super().__init__(requester, query_cb)
        self.lower = lower
        self.upper = upper
        self.find_lower = find_lower
        self.find_upper = find_upper
        self.correct = correct
        self.n_queries = 0


    async def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): collection context

        Returns:
            int|None: inferred number or None on fail
        '''
        self.n_queries = 0

        if self.find_lower:
            await self._find_lower(ctx, self.upper - self.lower)
        if self.find_upper:
            await self._find_upper(ctx, self.upper - self.lower)

        return await self._search(ctx, self.lower, self.upper)


    async def _find_lower(self, ctx, step):
        '''Exponentially expands the lower bound until the correct value is within.

        Params:
            ctx (Context): collection context
            step (int): initial step
        '''
        if not await self._query(ctx, self.lower):
            return

        self.upper = self.lower
        self.lower -= step
        await self._find_lower(ctx, step * 2)


    async def _find_upper(self, ctx, step):
        '''Exponentially expands the upper bound until the correct value is within.

        Params:
            ctx (Context): collection context
            step (int): initial step
        '''
        if await self._query(ctx, self.upper):
            return

        self.lower = self.upper
        self.upper += step
        await self._find_upper(ctx, step * 2)


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
        if await self._query(ctx, middle):
            return await self._search(ctx, lower, middle)

        return await self._search(ctx, middle, upper)


    async def _query(self, ctx, n):
        self.n_queries += 1

        if self.correct is None:
            query_string = self.query_cb(ctx, n)
            return await self.requester.request(ctx, query_string)

        return self.correct < n



class BinarySearch(SearchAlgorithm):
    '''Binary search for lists of values.'''
    def __init__(self, requester, query_cb, values, correct=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            values (list): list of values to search
            correct (value|None): correct value. If provided, the search is emulated
        '''
        super().__init__(requester, query_cb)
        self.values = values
        self.correct = correct
        self.n_queries = 0


    async def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): collection context

        Returns:
            value|None: inferred value or None on fail
        '''
        self.n_queries = 0
        return await self._search(ctx, self.values)


    async def _search(self, ctx, values):
        if not values:
            return None

        if len(values) == 1:
            return values[0]

        left, right = split_at(values, len(values) // 2)

        if await self._query(ctx, left):
            return await self._search(ctx, left)

        return await self._search(ctx, right)


    async def _query(self, ctx, values):
        self.n_queries += 1

        if self.correct is None:
            query_string = self.query_cb(ctx, values)
            return await self.requester.request(ctx, query_string)

        return self.correct in values




class TreeSearch(SearchAlgorithm):
    '''Huffman tree search.'''
    def __init__(self, requester, query_cb, tree, in_tree=False, correct=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            tree (utils.huffman.Node): Huffman tree to search
            in_tree (bool): True if the correct value is known to be in the tree
            correct (value|None): correct value. If provided, the search is emulated
        '''
        super().__init__(requester, query_cb)
        self.tree = tree
        self.in_tree = in_tree
        self.correct = correct
        self.n_queries = 0


    async def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): collection context

        Returns:
            value|None: inferred value or None on fail
        '''
        self.n_queries = 0
        return await self._search(ctx, self.tree, in_tree=self.in_tree)


    async def _search(self, ctx, tree, in_tree):
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

        if tree.is_leaf():
            if in_tree:
                return tree.values()[0]
            if await self._query(ctx, tree.values()):
                return tree.values()[0]
            return None

        if await self._query(ctx, tree.left.values()):
            return await self._search(ctx, tree.left, True)

        if tree.right is None:
            return None

        return await self._search(ctx, tree.right, in_tree)


    async def _query(self, ctx, values):
        self.n_queries += 1

        if self.correct is None:
            query_string = self.query_cb(ctx, values)
            return await self.requester.request(ctx, query_string)

        return self.correct in values
