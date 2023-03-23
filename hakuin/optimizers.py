from abc import ABCMeta, abstractmethod

from hakuin.utils import split_at



class Context:
    '''The state of the inference.'''
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



class Optimizer(metaclass=ABCMeta):
    '''Abstract class for optimizers. Optimizers implement
    different search algorithms.
    '''
    def __init__(self, requester, query_cb):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
        '''
        self.requester = requester
        self.query_cb = query_cb


    @abstractmethod
    def run(self, ctx):
        '''Runs the search algorithm.'''
        raise NotImplementedError()


    @abstractmethod
    def eval(self, ctx, correct):
        '''Runs (emulates) the search algorithm without sending any requests.'''
        raise NotImplementedError()



class NumericBinarySearch(Optimizer):
    '''Binary search for numeric values.'''
    def __init__(self, requester, query_cb, upper=16):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            upper (int): the initial upper bound of search range
        '''
        super().__init__(requester, query_cb)
        self.upper = upper


    def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): inference context

        Returns:
            int: inferred number
        '''
        lower, upper, _ = self._get_range(ctx, 0, self.upper)
        return self._search(ctx, lower, upper)[0]


    def eval(self, ctx, correct):
        '''Runs (emulates) the search algorithm without sending any requests.

        Params:
            ctx (Context): inference context
            correct (int): correct value

        Returns:
            (int, int): inferred number and number of queries
        '''
        lower, upper, range_n = self._get_range(ctx, 0, self.upper, correct)
        result, search_n = self._search(ctx, lower, upper, correct)
        return result, range_n + search_n


    def _get_range(self, ctx, lower, upper, correct=None, n=0):
        '''Exponentially expands the search range until the correct values is within.

        Params:
            ctx (Context): inference context
            lower (int): lower bound
            upper (int): upper bound
            correct (int|None): correct value

        Returns:
            (int, int): correct upper bound and number of queries
        '''
        if correct is None:
            query_string = self.query_cb(ctx, upper)
            found = self.requester.request(ctx, query_string)
        else:
            found = correct < upper

        if found:
            return lower, upper, n

        return self._get_range(ctx, upper, upper * 2, correct, n + 1)


    def _search(self, ctx, lower, upper, correct=None, n=0):
        '''Numeric binary search.
        
        Params:
            ctx (Context): inference context
            lower (int): lower bound
            upper (int): upper bound
            correct (int|None): correct value

        Returns:
            (int, int): inferred number and number of queries
        '''
        if lower + 1 == upper:
            return lower, n

        middle = (lower + upper) // 2

        if correct is None:
            query_string = self.query_cb(ctx, middle)
            found = self.requester.request(ctx, query_string)
        else:
            found = correct < middle

        if found:
            return self._search(ctx, lower, middle, correct, n + 1)
        else:
            return self._search(ctx, middle, upper, correct, n + 1)



class BinarySearch(Optimizer):
    '''Binary search for lists of values.'''
    def __init__(self, requester, query_cb, values):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            values (list): list of values to search (e.g., strings or characters)
        '''
        super().__init__(requester, query_cb)
        self.values = values


    def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): inference context

        Returns:
            value|None: inferred value (e.g., string or character)
        '''
        return self._search(ctx, self.values)[0]


    def eval(self, ctx, correct):
        '''Runs (emulates) the search algorithm without sending any requests.

        Params:
            ctx (Context): inference context
            correct (value): correct value

        Returns:
            (value|None, int): inferred value and the number of queries
        '''
        return self._search(ctx, self.values, correct)
        

    def _search(self, ctx, values, correct=None, n=0):
        '''Binary search for lists of values.
        
        Params:
            ctx (Context): inference context
            values (list): list of values to search (e.g., strings or characters)
            correct (value|None): correct value (e.g., string or character)

        Returns:
            (value|None, int): inferred value and number of queries
        '''
        if not values:
            return None, n

        if len(values) == 1:
            return values[0], n

        left, right = split_at(values, len(values) // 2)

        if correct is None:
            query_string = self.query_cb(ctx, left)
            found = self.requester.request(ctx, query_string)
        else:
            found = correct in left

        if found:
            return self._search(ctx, left, correct, n + 1)
        else:
            return self._search(ctx, right, correct, n + 1)



class TreeSearch(Optimizer):
    '''Huffman tree search.'''
    def __init__(self, requester, query_cb, tree, in_tree=False):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            tree (utils.huffman.Node): Huffman tree to search
            in_tree (bool): True if the correct value is known to be in the tree
        '''
        super().__init__(requester, query_cb)
        self.tree = tree
        self.in_tree = in_tree


    def run(self, ctx):
        '''Runs the search algorithm.

        Params:
            ctx (Context): inference context

        Returns:
            value|None: inferred value (e.g., string or character)
        '''
        return self._search(ctx, self.tree, verified=self.in_tree)[0]


    def eval(self, ctx, correct):
        '''Runs (emulates) the search algorithm without sending any requests.

        Params:
            ctx (Context): inference context
            correct (value): correct value

        Returns:
            (value|None, int): inferred value and the number of queries
        '''
        return self._search(ctx, self.tree, verified=self.in_tree, correct=correct)


    def _search(self, ctx, tree, verified, correct=None, n=0):
        '''Huffman tree search.
        
        Params:
            ctx (Context): inference context
            tree (utils.huffman.Node): Huffman tree to search
            verified (bool): True when the values has been confirmed to be in the tree
            correct (value|None): correct value (e.g., string or character)

        Returns:
            (value|None, int): inferred value and number of queries
        '''
        if tree is None:
            return None, n

        if tree.left is None:
            if verified:
                return tree.values()[0], n

            if correct is None:
                query_string = self.query_cb(ctx, tree.values())
                found = self.requester.request(ctx, query_string)
            else:
                found = correct in tree.values()

            if found:
                return tree.values()[0], n + 1
            return None, n + 1

        if correct is None:
            query_string = self.query_cb(ctx, tree.left.values())
            found = self.requester.request(ctx, query_string)
        else:
            found = correct in tree.left.values()

        if found:
            return self._search(ctx, tree.left, True, correct, n + 1)
        else:
            if tree.right is None:
                return None, n
            return self._search(ctx, tree.right, verified, correct, n + 1)
