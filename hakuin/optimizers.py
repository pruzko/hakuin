from abc import ABCMeta, abstractmethod

from hakuin.utils import split_at



class Context:
    def __init__(self, table, column, row, s):
        self.table = table
        self.column = column
        self.row = row
        self.s = s



class Optimizer(metaclass=ABCMeta):
    def __init__(self, requester, query_cb):
        self.requester = requester
        self.query_cb = query_cb


    @abstractmethod
    def run(self, ctx):
        raise NotImplementedError()


    @abstractmethod
    def eval(self, ctx, correct):
        raise NotImplementedError()



class NumericBinarySearch(Optimizer):
    def __init__(self, requester, query_cb, upper=16):
        super().__init__(requester, query_cb)
        self.upper = upper


    def run(self, ctx):
        lower, upper, _ = self._get_range(ctx, 0, self.upper)
        return self._search(ctx, lower, upper)[0]


    def eval(self, ctx, correct):
        lower, upper, range_n = self._get_range(ctx, 0, self.upper, correct)
        result, search_n = self._search(ctx, lower, upper, correct)
        return result, range_n + search_n


    def _get_range(self, ctx, lower, upper, correct=None, n=0):
        if correct is None:
            query_string = self.query_cb(ctx, upper)
            found = self.requester.request(ctx, query_string)
        else:
            found = correct < upper

        if found:
            return lower, upper, n

        return self._get_range(ctx, upper, upper * 2, correct, n + 1)


    def _search(self, ctx, lower, upper, correct=None, n=0):
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
    def __init__(self, requester, query_cb, values):
        super().__init__(requester, query_cb)
        self.values = values


    def run(self, ctx):
        return self._search(ctx, self.values)[0]


    def eval(self, ctx, correct):
        return self._search(ctx, self.values, correct)
        

    def _search(self, ctx, values, correct=None, n=0):
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
    def __init__(self, requester, query_cb, tree, in_tree=False):
        super().__init__(requester, query_cb)
        self.tree = tree
        self.in_tree = in_tree


    def run(self, ctx):
        return self._search(ctx, self.tree, verified=self.in_tree)[0]


    def eval(self, ctx, correct):
        return self._search(ctx, self.tree, verified=self.in_tree, correct=correct)


    def _search(self, ctx, tree, verified, correct=None, n=0):
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
