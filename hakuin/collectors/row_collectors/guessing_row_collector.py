import asyncio
from collections import Counter

from hakuin.requesters import EmulationRequester
from hakuin.search_algorithms import TreeSearch
from hakuin.utils.huffman import make_tree

from .row_collector import RowCollector



class GuessCounter:
    '''Counter for frequently occuring values.

    Attributes:
        ITEM_PROB_TH (float): probability threshold necessary for guesses to be included
    '''
    ITEM_PROB_TH = 0.01


    def __init__(self):
        '''Constructor.'''
        self._counter = Counter()
        self._total = 0
        self._lock = asyncio.Lock()


    async def guesses(self):
        '''Retrieves potential guesses and their corresponding probabilities.

        Returns:
            list[value, float]: ordered list of guesses and probabilities
        '''
        guesses = []
        async with self._lock:
            if self._total == 0:
                return guesses

            for guess, count in self._counter.most_common():
                prob = count / self._total
                if count <= 1 or prob < self.ITEM_PROB_TH:
                    break
                guesses.append((guess, prob))

        return guesses


    async def update(self, value):
        '''Updates the counter with a new value.

        Params:
            value (value): new value
        '''
        async with self._lock:
            self._counter[value] += 1
            self._total += 1



class GuessingRowCollector(RowCollector):
    '''Guessing row collector.

    Attributes:
        TOTAL_PROB_TH (float): probability threshold necessary for guessing to take place
    '''
    TOTAL_PROB_TH = 0.5


    def __init__(self, requester, dbms):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): database engine
        '''
        super().__init__(requester=requester, dbms=dbms)
        self.counter = GuessCounter()


    async def run(self, ctx, tree):
        '''Collects a single row.

        Params:
            ctx (Context): collection context

        Returns:
            value|None: collected row or None on fail
        '''
        return await self._run(requester=self.requester, ctx=ctx, tree=tree)


    async def _emulate(self, ctx, tree, correct):
        '''Emulates collection of a single row.

        Params:
            ctx (Context): collection context
            tree (HuffmanNode): guessing tree
            correct: correct value

        Returns:
            (int, value|None): request count and the result if available
        '''
        requester = EmulationRequester(correct=correct)
        res = await self._run(requester=requester, ctx=ctx, tree=tree)
        n_requests = await requester.n_requests()
        return n_requests, res


    async def _run(self, requester, ctx, tree):
        '''Collects a single row.

        Params:
            requester (Requester): requester to be used
            ctx (Context): collection context
            tree (HuffmanNode): guessing tree

        Returns:
            value: collected row
        '''
        return await TreeSearch(
            requester=requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryValueInList,
            tree=tree,
        ).run(ctx)


    async def make_tree(self, fallback_cost):
        '''Creates an optimal guessing tree.

        Params:
            fallback_cost (float): fallback cost

        Returns:
            HuffmanNode|None: guessing tree or None if unavailable
        '''
        guesses = {}
        prob = 0.0
        best_prob = prob
        best_cost = float('inf')
        best_tree = None

        for guess, guess_prob in await self.counter.guesses():
            guesses[guess] = guess_prob
            tree = make_tree(guesses)
            prob += guess_prob
            cost = prob * tree.success_cost() + ((1 - prob) * (tree.fail_cost() + fallback_cost))

            if cost > best_cost:
                break

            best_prob = prob
            best_cost = cost
            best_tree = tree

        return best_tree if best_prob >= self.TOTAL_PROB_TH else None


    async def update(self, ctx, row_guessed, value, tree):
        '''Updates the row collector with a newly collected row.

        Param:
            ctx (Context): collection context
            value (value): collected row
            row_guessed (bool): row was successfully guessed flag
            tree (HuffmanNode|None): guessing tree or None if not available
        '''
        if tree:
            cost, res = await self._emulate(ctx, tree=tree, correct=value)
            await self.stats.update(is_success=res is not None, cost=cost)

        await self.counter.update(value)
