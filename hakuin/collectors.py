import logging
from abc import ABCMeta, abstractmethod
from collections import Counter

import hakuin
from hakuin.utils import tokenize, CHARSET_ASCII, EOS
from hakuin.utils.huffman import make_tree
from hakuin.search_algorithms import Context, BinarySearch, TreeSearch



class Collector(metaclass=ABCMeta):
    '''Abstract class for collectors. Collectors repeatidly run
    search algorithms to infer column rows.
    '''
    def __init__(self, requester, query_cb):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
        '''
        self.requester = requester
        self.query_cb = query_cb


    def run(self, ctx, n_rows):
        '''Run collection.

        Params:
            ctx (Context): inference context
            n_rows (int): number of rows in column

        Returns:
            list: column rows
        '''
        logging.info(f'Inferring "{ctx.table}.{ctx.column}"...')

        data = []
        for row in range(n_rows):
            ctx = Context(ctx.table, ctx.column, row, None)
            res = self._collect_row(ctx)
            data.append(res)

            logging.info(f'({row + 1}/{n_rows}) inferred: {res}')

        return data


    @abstractmethod
    def _collect_row(self, ctx):
        raise NotImplementedError()



class TextCollector(Collector):
    '''Collector for text columns.'''
    def _collect_row(self, ctx):
        ctx.s = ''
        while True:
            c = self._collect_char(ctx)
            if c == EOS:
                return ctx.s
            ctx.s += c


    @abstractmethod
    def _collect_char(self, ctx):
        raise NotImplementedError()



class BinaryTextCollector(TextCollector):
    '''Binary search text collector'''
    def __init__(self, requester, query_cb, charset=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            charset (list|None): list of possible characters

        Returns:
            list: column rows
        '''
        super().__init__(requester, query_cb)
        self.charset = charset if charset else CHARSET_ASCII


    def _collect_char(self, ctx):
        return BinarySearch(
            self.requester,
            self.query_cb,
            values=self.charset,
        ).run(ctx)



class ModelTextCollector(TextCollector):
    '''Language model-based text collector.'''
    def __init__(self, requester, query_cb, model, charset=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_cb (function): query construction function
            model (Model): language model
            charset (list|None): list of possible characters

        Returns:
            list: column rows
        '''
        super().__init__(requester, query_cb)
        self.model = model
        self.charset = charset if charset else CHARSET_ASCII


    def _collect_char(self, ctx):
        model_ctx = tokenize(ctx.s, add_eos=False, max_len=self.model.max_ngram - 1)
        scores = self.model.score_any_dict(model_ctx)

        c = TreeSearch(
            self.requester,
            self.query_cb,
            tree=make_tree(scores),
        ).run(ctx)

        if c is not None:
            return c

        charset = list(set(self.charset).difference(set(scores)))
        return BinarySearch(
            self.requester,
            self.query_cb,
            values=self.charset,
        ).run(ctx)



class AdaptiveTextCollector(ModelTextCollector):
    '''Same as ModelTextCollector but adapts the model.'''
    def _collect_char(self, ctx):
        c = super()._collect_char(ctx)
        self.model.fit_correct(ctx.s, c)
        return c



class DynamicTextCollector(TextCollector):
    '''Dynamic text collector. The collector keeps statistical information (RPC)
    for several strategies (binary search, unigram, and five-gram) and dynamically
    chooses the best one. In addition, it uses the statistical information to
    identify when guessing whole strings is likely to succeed and then uses
    previously inferred strings to make the guesses.

    Attributes:
        GUESS_TH (float): success probability threshold necessary to make guesses
        GUESS_SCORE_TH (float): minimal necessary probability to be included in guess tree
    '''
    GUESS_TH = 0.5
    GUESS_SCORE_TH = 0.01


    def __init__(self, requester, query_char_cb, query_string_cb, charset=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_char_cb (function): query construction function for searching characters
            query_string_cb (function): query construction function for searching strings
            charset (list|None): list of possible characters

        Other Attributes:
            model_guess: adaptive string-based model for guessing
            model_unigram: adaptive unigram model
            model_fivegram: adaptive five-gram model
        '''
        self.requester = requester
        self.query_char_cb = query_char_cb
        self.query_string_cb = query_string_cb
        self.charset = charset if charset else CHARSET_ASCII
        self.model_guess = hakuin.Model.make_clean(1)
        self.model_unigram = hakuin.Model.make_clean(1)
        self.model_fivegram = hakuin.Model.make_clean(5)
        self._stats = {
            'rpc': {
                'binary': {'avg': 0.0, 'hist': []},
                'unigram': {'avg': 0.0, 'hist': []},
                'fivegram': {'avg': 0.0, 'hist': []},
            },
            'avg_len': 0.0,
            'n_strings': 0,
        }


    def _collect_row(self, ctx):
        s = self._collect_string(ctx)
        self.model_guess.fit_correct([], s)

        self._stats['n_strings'] += 1

        total = self._stats['avg_len'] * (self._stats['n_strings'] - 1) + len(s)
        self._stats['avg_len'] = total / self._stats['n_strings']
        return s


    def _collect_string(self, ctx):
        '''Identifies if guessings strings is likely to succeed and if yes, it makes guesses.
        If guessing does not take place or fails, it proceeds with per-character inference.
        '''
        correct_str = self._try_guessing(ctx)
        
        if correct_str is not None:
            ctx.s = ''
            for c in correct_str:
                self._compute_stats(ctx, c)
                ctx.s += c

            self.model_unigram.fit([correct_str])
            self.model_fivegram.fit([correct_str])
            return correct_str

        ctx.s = ''
        while True:
            c = self._collect_char(ctx)

            self._compute_stats(ctx, c)
            self.model_unigram.fit_correct(ctx.s, c)
            self.model_fivegram.fit_correct(ctx.s, c)

            if c == EOS:
                return ctx.s

            ctx.s += c


    def _collect_char(self, ctx):
        '''Chooses the best strategy and uses it to infer a character.'''
        searched_space = set()
        c = self._get_strategy(ctx, searched_space, self._best_strategy()).run(ctx)
        if c is None:
            c = self._get_strategy(ctx, searched_space, 'binary').run(ctx)
        return c


    def _try_guessing(self, ctx):
        '''Tries to construct a guessing Huffman tree and searches it in case of success.'''
        tree = self._get_guess_tree(ctx)
        return TreeSearch(
            self.requester,
            self.query_string_cb,
            tree=tree,
        ).run(ctx)


    def _get_guess_tree(self, ctx):
        '''Identifies, whether string guessing is likely to succeed and if so,
        it constructs a Huffman tree from previously inferred strings.

        Returns:
            utils.huffman.Node|None: Huffman tree constructed from previously inferred
            strings that are likely to succeed or None if no such strings were found
        '''

        # Expectation for per-character inference:
        # exp_c = avg_len * best_strategy_rpc
        exp_c = self._stats['avg_len'] * self._stats['rpc'][self._best_strategy()]['avg']

        # Iteratively compute the best expectation "best_exp_g" by progressively inserting guess
        # strings into a candidate guess set "guesses" and computing their expectation "exp_g".
        # The iteration stops when the minimal "exp_g" is found.
        # exp(G) = p(s in G) * exp_huff(G) + (1 - p(c in G)) * (exp_huff(G) + exp_c)
        guesses = {}
        prob_g = 0.0
        best_prob_g = 0.0
        best_exp_g = float('inf')
        best_tree = None

        scores = self.model_guess.score_dict([])
        scores = {k: v for k, v in scores.items() if v >= self.GUESS_SCORE_TH and self.model_guess.count(k, []) > 1}
        for guess, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
            guesses[guess] = score

            tree = make_tree(guesses)
            tree_cost = tree.search_cost()
            prob_g += score
            exp_g = prob_g * tree_cost + (1 - prob_g) * (tree_cost + exp_c)

            if exp_g > best_exp_g:
                break

            best_prob_g = prob_g
            best_exp_g = exp_g
            best_tree = tree

        if best_exp_g > exp_c or best_prob_g < self.GUESS_TH:
            return None

        return best_tree


    def _best_strategy(self):
        '''Returns the name of the best strategy.'''
        return min(self._stats['rpc'], key=lambda strategy: self._stats['rpc'][strategy]['avg'])


    def _compute_stats(self, ctx, correct):
        '''Emulates all strategies without sending any requests and updates the
        statistical information.
        '''
        for strategy in self._stats['rpc']:
            searched_space = set()
            search_alg = self._get_strategy(ctx, searched_space, strategy, correct)
            res = search_alg.run(ctx)
            n_queries = search_alg.n_queries
            if res is None:
                binary_search = self._get_strategy(ctx, searched_space, 'binary', correct)
                binary_search.run(ctx)
                n_queries += binary_search.n_queries

            m = self._stats['rpc'][strategy]
            m['hist'].append(n_queries)
            m['hist'] = m['hist'][-100:]
            m['avg'] = sum(m['hist']) / len(m['hist'])


    def _get_strategy(self, ctx, searched_space, strategy, correct=None):
        '''Builds search algorithm configured to search appropriate space.

        Params:
            ctx (Context): inference context
            searched_space (list): list of values that have already been searched
            strategy (str): strategy ('binary', 'unigram', 'fivegram')
            correct (str|None): correct character

        Returns:
            SearchAlgorithm: configured search algorithm
        '''
        if strategy == 'binary':
            charset = list(set(self.charset).difference(searched_space))
            return BinarySearch(
                self.requester,
                self.query_char_cb,
                values=self.charset,
                correct=correct,
            )
        elif strategy == 'unigram':
            scores = self.model_unigram.score_dict([])
            searched_space.union(set(scores))
            return TreeSearch(
                self.requester,
                self.query_char_cb,
                tree=make_tree(scores),
                correct=correct,
            )
        else:
            model_ctx = tokenize(ctx.s, add_eos=False)
            model_ctx = model_ctx[-(self.model_fivegram.max_ngram - 1):]
            scores = self.model_fivegram.score_any_dict(model_ctx)

            searched_space.union(set(scores))
            return TreeSearch(
                self.requester,
                self.query_char_cb,
                tree=make_tree(scores),
                correct=correct,
            )
