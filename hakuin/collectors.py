import logging
from abc import ABCMeta, abstractmethod
from collections import Counter

import hakuin
from hakuin.utils import tokenize, CHARSET_ASCII, EOS, ASCII_MAX, UNICODE_MAX
from hakuin.utils.huffman import make_tree
from hakuin.search_algorithms import Context, BinarySearch, TreeSearch, IntExponentialBinarySearch



class Collector(metaclass=ABCMeta):
    '''Abstract class for collectors. Collectors repeatidly run
    search algorithms to extract column rows.
    '''
    def __init__(self, requester, queries):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            queries (UniformQueries): injection queries
        '''
        self.requester = requester
        self.queries = queries


    def run(self, ctx, n_rows, *args, **kwargs):
        '''Collects the whole column.

        Params:
            ctx (Context): extraction context
            n_rows (int): number of rows in column

        Returns:
            list: column rows
        '''
        logging.info(f'Inferring "{ctx.table}.{ctx.column}"')

        data = []
        for row in range(n_rows):
            ctx = Context(ctx.table, ctx.column, row, None)
            res = self.collect_row(ctx, *args, **kwargs)
            data.append(res)

            logging.info(f'({row + 1}/{n_rows}) "{ctx.table}.{ctx.column}": {res}')

        return data


    @abstractmethod
    def collect_row(self, ctx, *args, **kwargs):
        '''Collects a row.

        Params:
            ctx (Context): extraction context

        Returns:
            value: single row
        '''
        raise NotImplementedError()


class IntCollector(Collector):
    '''Collector for integer columns'''
    def __init__(self, requester, queries):
        super().__init__(requester, queries)


    def collect_row(self, ctx):
        return IntExponentialBinarySearch(
            requester=self.requester,
            query_cb=self.queries.int,
            lower=0,
            upper=128,
            find_lower=True,
            find_upper=True,
        ).run(ctx)


class TextCollector(Collector):
    '''Collector for text columns.'''
    def __init__(self, requester, queries, charset=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            queries (UniformQueries): injection queries
            charset (list|None): list of possible characters, None for default ASCII
        '''
        super().__init__(requester, queries)
        self.charset = charset if charset is not None else CHARSET_ASCII
        if EOS not in self.charset:
            self.charset.append(EOS)


    def run(self, ctx, n_rows):
        '''Collects the whole column.

        Params:
            ctx (Context): extraction context
            n_rows (int): number of rows in column

        Returns:
            list: column rows
        '''
        rows_are_ascii = self.check_rows_are_ascii(ctx)
        return super().run(ctx, n_rows, rows_are_ascii)


    def collect_row(self, ctx, rows_are_ascii):
        '''Collects a row.

        Params:
            ctx (Context): extraction context
            rows_are_ascii (bool): ASCII flag for all rows in column

        Returns:
            string: single row
        '''
        row_is_ascii = True if rows_are_ascii else self.check_row_is_ascii(ctx)

        ctx.s = ''
        while True:
            c = self.collect_char(ctx, row_is_ascii)
            if c == EOS:
                return ctx.s
            ctx.s += c


    @abstractmethod
    def collect_char(self, ctx, row_is_ascii):
        '''Collects a character.

        Params:
            ctx (Context): extraction context
            row_is_ascii (bool): row ASCII flag

        Returns:
            string: single character
        '''
        raise NotImplementedError()


    def check_rows_are_ascii(self, ctx):
        '''Finds out whether all rows in column are ASCII.

        Params:
            ctx (Context): extraction context

        Returns:
            bool: ASCII flag
        '''
        query = self.queries.rows_are_ascii(ctx)
        return self.requester.request(ctx, query)


    def check_row_is_ascii(self, ctx):
        '''Finds out whether current row is ASCII.

        Params:
            ctx (Context): extraction context

        Returns:
            bool: ASCII flag
        '''
        query = self.queries.row_is_ascii(ctx)
        return self.requester.request(ctx, query)


    def check_char_is_ascii(self, ctx):
        '''Finds out whether current character is ASCII.

        Params:
            ctx (Context): extraction context

        Returns:
            bool: ASCII flag
        '''
        query = self.queries.char_is_ascii(ctx)
        return self.requester.request(ctx, query)



class BinaryTextCollector(TextCollector):
    '''Binary search text collector'''
    def collect_char(self, ctx, row_is_ascii):
        '''Collects a character.

        Params:
            ctx (Context): extraction context
            row_is_ascii (bool): row ASCII flag

        Returns:
            string: single character
        '''
        return self._collect_or_emulate_char(ctx, row_is_ascii)[0]


    def emulate_char(self, ctx, row_is_ascii, correct):
        '''Emulates character collection without sending requests.

        Params:
            ctx (Context): extraction context
            row_is_ascii (bool): row ASCII flag
            correct (str): correct character

        Returns:
            int: number of requests necessary
        '''
        return self._collect_or_emulate_char(ctx, row_is_ascii, correct)[1]


    def _collect_or_emulate_char(self, ctx, row_is_ascii, correct=None):
        total_queries = 0

        # custom charset or ASCII
        if self.charset is not CHARSET_ASCII or row_is_ascii or self._check_or_emulate_char_is_ascii(ctx, correct):
            search_alg = BinarySearch(
                requester=self.requester,
                query_cb=self.queries.char,
                values=self.charset,
                correct=correct,
            )
            res = search_alg.run(ctx)
            total_queries += search_alg.n_queries

            if res is not None:
                return res, total_queries

        # Unicode
        correct_ord = ord(correct) if correct is not None else correct
        search_alg = IntExponentialBinarySearch(
            requester=self.requester,
            query_cb=self.queries.char_unicode,
            lower=ASCII_MAX + 1,
            upper=UNICODE_MAX + 1,
            find_lower=False,
            find_upper=False,
            correct=correct_ord,
        )
        res = search_alg.run(ctx)
        total_queries += search_alg.n_queries

        return chr(res), total_queries


    def _check_or_emulate_char_is_ascii(self, ctx, correct):
        if correct is None:
            return self.check_char_is_ascii(ctx)
        return correct.isascii()



class ModelTextCollector(TextCollector):
    '''Language model-based text collector.'''
    def __init__(self, requester, queries, model, charset=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            queries (UniformQueries): injection queries
            model (Model): language model
            charset (list|None): list of possible characters

        Returns:
            list: column rows
        '''
        super().__init__(requester, queries, charset)
        self.model = model
        self.binary_collector = BinaryTextCollector(
            requester=self.requester,
            queries=self.queries,
            charset=self.charset,
        )


    def collect_char(self, ctx, row_is_ascii):
        '''Collects a character.

        Params:
            ctx (Context): extraction context
            row_is_ascii (bool): row ASCII flag

        Returns:
            string: single character
        '''
        return self._collect_or_emulate_char(ctx, row_is_ascii)[0]


    def emulate_char(self, ctx, row_is_ascii, correct):
        '''Emulates character collection without sending requests.

        Params:
            ctx (Context): extraction context
            row_is_ascii (bool): row ASCII flag
            correct (str): correct character

        Returns:
            int: number of requests necessary
        '''
        return self._collect_or_emulate_char(ctx, row_is_ascii, correct)[1]


    def _collect_or_emulate_char(self, ctx, row_is_ascii, correct=None):
        n_queries_model = 0

        model_ctx = tokenize(ctx.s, add_eos=False)
        scores = self.model.scores(context=model_ctx)

        search_alg = TreeSearch(
            requester=self.requester,
            query_cb=self.queries.char,
            tree=make_tree(scores),
            correct=correct,
        )
        res = search_alg.run(ctx)
        n_queries_model = search_alg.n_queries

        if res is not None:
            return res, n_queries_model

        res, n_queries_binary = self.binary_collector._collect_or_emulate_char(ctx, row_is_ascii, correct)
        return res, n_queries_model + n_queries_binary



class AdaptiveTextCollector(ModelTextCollector):
    '''Same as ModelTextCollector but adapts the model.'''
    def collect_char(self, ctx):
        c = super().collect_char(ctx, correct)
        self.model.fit_correct_char(c, partial_str=ctx.s)
        return c



class DynamicTextStats:
    '''Helper class of DynamicTextCollector to keep track of statistical information.'''
    def __init__(self):
        self.str_len_mean = 0.0
        self.n_strings = 0
        self._rpc = {
            'binary': {'mean': 0.0, 'hist': []},
            'unigram': {'mean': 0.0, 'hist': []},
            'fivegram': {'mean': 0.0, 'hist': []},
        }


    def update_str(self, s):
        self.n_strings += 1
        self.str_len_mean = (self.str_len_mean * (self.n_strings - 1) + len(s)) / self.n_strings


    def update_rpc(self, strategy, n_queries):
        rpc = self._rpc[strategy]
        rpc['hist'].append(n_queries)
        rpc['hist'] = rpc['hist'][-100:]
        rpc['mean'] = sum(rpc['hist']) / len(rpc['hist'])


    def rpc(self, strategy):
        return self._rpc[strategy]['mean']


    def best_strategy(self):
        return min(self._rpc, key=lambda strategy: self.rpc(strategy))



class DynamicTextCollector(TextCollector):
    '''Dynamic text collector. The collector keeps statistical information (RPC)
    for several strategies (binary search, unigram, and five-gram) and dynamically
    chooses the best one. In addition, it uses the statistical information to
    identify when guessing whole strings is likely to succeed and then uses
    previously inferred strings to make the guesses.
    '''
    def __init__(self, requester, queries, charset=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            queries (UniformQueries): injection queries
            charset (list|None): list of possible characters

        Other Attributes:
            model_unigram (Model): adaptive unigram model
            model_fivegram (Model): adaptive five-gram model
            guess_collector (StringGuessCollector): collector for guessing
        '''
        super().__init__(requester, queries, charset)
        self.binary_collector = BinaryTextCollector(
            requester=self.requester,
            queries=self.queries,
            charset=self.charset,
        )
        self.unigram_collector = ModelTextCollector(
            requester=self.requester,
            queries=self.queries,
            model=hakuin.Model(1),
            charset=self.charset,
        )
        self.fivegram_collector = ModelTextCollector(
            requester=self.requester,
            queries=self.queries,
            model=hakuin.Model(5),
            charset=self.charset,
        )
        self.guess_collector = StringGuessingCollector(
            requester=self.requester,
            queries=self.queries,
        )
        self.stats = DynamicTextStats()


    def collect_row(self, ctx, rows_are_ascii):
        row_is_ascii = True if rows_are_ascii else self.check_row_is_ascii(ctx)

        s = self._collect_string(ctx, row_is_ascii)
        self.guess_collector.model.fit_single(s, context=[])
        self.stats.update_str(s)

        return s


    def _collect_string(self, ctx, row_is_ascii):
        '''Tries to guess strings or extracts them on per-character basis if guessing fails'''
        exp_c = self.stats.str_len_mean * self.stats.rpc(self.stats.best_strategy())
        correct_str = self.guess_collector.collect_row(ctx, exp_c)

        if correct_str is not None:
            self._update_stats_str(ctx, row_is_ascii, correct_str)
            self.unigram_collector.model.fit_data([correct_str])
            self.fivegram_collector.model.fit_data([correct_str])
            return correct_str

        return self._collect_string_per_char(ctx, row_is_ascii)


    def _collect_string_per_char(self, ctx, row_is_ascii):
        ctx.s = ''
        while True:
            c = self.collect_char(ctx, row_is_ascii)
            self._update_stats(ctx, row_is_ascii, c)
            self.unigram_collector.model.fit_correct_char(c, partial_str=ctx.s)
            self.fivegram_collector.model.fit_correct_char(c, partial_str=ctx.s)

            if c == EOS:
                return ctx.s
            ctx.s += c

        return ctx.s


    def collect_char(self, ctx, row_is_ascii):
        '''Chooses the best strategy and uses it to infer a character.'''
        best = self.stats.best_strategy()
        # print(f'b: {self.stats.rpc("binary")}, u: {self.stats.rpc("unigram")}, f: {self.stats.rpc("fivegram")}')
        if best == 'binary':
            return self.binary_collector.collect_char(ctx, row_is_ascii)
        elif best == 'unigram':
            return self.unigram_collector.collect_char(ctx, row_is_ascii)
        else:
            return self.fivegram_collector.collect_char(ctx, row_is_ascii)


    def _update_stats(self, ctx, row_is_ascii, correct):
        '''Emulates all strategies without sending requests and updates the statistical information.'''
        collectors = (
            ('binary', self.binary_collector),
            ('unigram', self.unigram_collector),
            ('fivegram', self.fivegram_collector),
        )

        for strategy, collector in collectors:
            n_queries = collector.emulate_char(ctx, row_is_ascii, correct)
            self.stats.update_rpc(strategy, n_queries)


    def _update_stats_str(self, ctx, row_is_ascii, correct_str):
        '''Like _update_stats but for whole strings.'''
        ctx.s = ''
        for c in correct_str:
            self._update_stats(ctx, row_is_ascii, c)
            ctx.s += c



class StringGuessingCollector(Collector):
    '''String guessing collector. The collector keeps track of previously extracted
    strings and opportunistically tries to guess new strings.
    '''
    GUESS_TH = 0.5
    GUESS_SCORE_TH = 0.01


    def __init__(self, requester, queries):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            queries (UniformQueries): injection queries

        Other Attributes:
            GUESS_TH (float): minimal threshold necessary to start guessing
            GUESS_SCORE_TH (float): minimal threshold for strings to be eligible for guessing
            model (Model): adaptive string-based model for guessing
        '''
        super().__init__(requester, queries)
        self.model = hakuin.Model(1)


    def collect_row(self, ctx, exp_alt=None):
        '''Tries to construct a guessing Huffman tree and searches it in case of success.

        Params:
            ctx (Context): extraction context
            exp_alt (float|None): expectation for alternative extraction method or None if it does not exist

        Returns:
            string|None: guessed string or None if skipped or failed
        '''
        exp_alt = exp_alt if exp_alt is not None else float('inf')
        tree = self._get_guess_tree(ctx, exp_alt)
        return TreeSearch(
            requester=self.requester,
            query_cb=self.queries.string,
            tree=tree,
        ).run(ctx)


    def _get_guess_tree(self, ctx, exp_alt):
        '''Identifies, whether string guessing is likely to succeed and if so,
        it constructs a Huffman tree from previously inferred strings.

        Params:
            ctx (Context): extraction context
            exp_alt (float): expectation for alternative extraction method

        Returns:
            utils.huffman.Node|None: Huffman tree constructed from previously inferred strings that are
                                     likely to succeed or None if no such strings were found
        '''
        # Iteratively compute the best expectation "best_exp_g" by progressively inserting guess
        # strings into a candidate guess set "guesses" and computing their expectation "exp_g".
        # The iteration stops when the minimal "exp_g" is found.
        # exp(G) = p(s in G) * exp_huff(G) + (1 - p(c in G)) * (exp_huff(G) + exp_alt)
        guesses = {}
        prob_g = 0.0
        best_prob_g = 0.0
        best_exp_g = float('inf')
        best_tree = None

        scores = self.model.scores(context=[])
        scores = {k: v for k, v in scores.items() if v >= self.GUESS_SCORE_TH and self.model.count(k, []) > 1}
        for guess, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
            guesses[guess] = score

            tree = make_tree(guesses)
            tree_cost = tree.search_cost()
            prob_g += score
            exp_g = prob_g * tree_cost + (1 - prob_g) * (tree_cost + exp_alt)

            if exp_g > best_exp_g:
                break

            best_prob_g = prob_g
            best_exp_g = exp_g
            best_tree = tree

        if best_exp_g <= exp_alt and best_prob_g > self.GUESS_TH:
            return best_tree

        return None
