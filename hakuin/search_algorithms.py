from abc import ABCMeta, abstractmethod

from hakuin.utils import split_at



class Context:
    '''Inference state.'''
    def __init__(self, table, column, row, s):
        '''Constructor.

        Params:
            table (str|None): table name
            column (str|None): column name
            row (int|None): row index
            s (str|None): partially or completely inferred string
        '''
        self.table = table
        self.column = column
        self.row = row
        self.s = s



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
    def run(self, ctx, correct=None):
        '''Runs the search algorithm.'''
        raise NotImplementedError()



class IntExponentialBinarySearch(SearchAlgorithm):
    '''Exponential and binary search for integers.'''
    def __init__(self, requester, query_cb, lower=0, upper=16, find_range=True, correct=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            lower (int): lower bound of search range
            upper (int): upper bound of search range
            find_range (bool): exponentially expands range until the correct value is within 
            correct (int|None): correct value. If provided, the search is emulated
        '''
        super().__init__(requester, query_cb)
        self.lower = lower
        self.upper = upper
        self.find_range = find_range
        self.correct = correct
        self.n_queries = 0


    def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): extraction context

        Returns:
            int: inferred number
        '''
        self.n_queries = 0

        if self.find_range:
            lower, upper = self._find_range(ctx, lower=self.lower, upper=self.upper)
        else:
            lower, upper = self.lower, self.upper

        return self._search(ctx, lower, upper)


    def _find_range(self, ctx, lower, upper):
        '''Exponentially expands the search range until the correct value is within.

        Params:
            ctx (Context): extraction context
            lower (int): lower bound
            upper (int): upper bound

        Returns:
            int: correct upper bound
        '''
        if self._query(ctx, upper):
            return lower, upper

        return self._find_range(ctx, upper, upper * 2)


    def _search(self, ctx, lower, upper):
        '''Numeric binary search.

        Params:
            ctx (Context): extraction context
            lower (int): lower bound
            upper (int): upper bound

        Returns:
            int: inferred number
        '''
        if lower + 1 == upper:
            return lower

        middle = (lower + upper) // 2
        if self._query(ctx, middle):
            return self._search(ctx, lower, middle)

        return self._search(ctx, middle, upper)


    def _query(self, ctx, n):
        self.n_queries += 1

        if self.correct is None:
            query_string = self.query_cb(ctx, n)
            return self.requester.request(ctx, query_string)

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


    def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): extraction context

        Returns:
            value|None: inferred value or None on fail
        '''
        self.n_queries = 0
        return self._search(ctx, self.values)


    def _search(self, ctx, values):
        if not values:
            return None

        if len(values) == 1:
            return values[0]

        left, right = split_at(values, len(values) // 2)

        if self._query(ctx, left):
            return self._search(ctx, left)

        return self._search(ctx, right)


    def _query(self, ctx, values):
        self.n_queries += 1

        if self.correct is None:
            query_string = self.query_cb(ctx, values)
            return self.requester.request(ctx, query_string)

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


    def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): extraction context

        Returns:
            value|None: inferred value or None on fail
        '''
        self.n_queries = 0
        return self._search(ctx, self.tree, in_tree=self.in_tree)


    def _search(self, ctx, tree, in_tree):
        '''Tree search.
        
        Params:
            ctx (Context): extraction context
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
            if self._query(ctx, tree.values()):
                return tree.values()[0]
            return None

        if self._query(ctx, tree.left.values()):
            return self._search(ctx, tree.left, True)

        if tree.right is None:
            return None

        return self._search(ctx, tree.right, in_tree)


    def _query(self, ctx, values):
        self.n_queries += 1

        if self.correct is None:
            query_string = self.query_cb(ctx, values)
            return self.requester.request(ctx, query_string)

        return self.correct in values
