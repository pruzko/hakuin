import asyncio
import logging
from abc import ABCMeta, abstractmethod
from copy import deepcopy

import hakuin
from hakuin.utils import tokenize, EOS, ASCII_MAX, UNICODE_MAX, BYTE_MAX, CHARSET_DIGITS
from hakuin.utils.huffman import make_tree
from hakuin.search_algorithms import BinarySearch, TreeSearch, NumericBinarySearch



class Context:
    '''Collection state.'''
    def __init__(
        self, target, table=None, column=None, schema=None, n_rows=None, row_idx=None, rows_have_null=None,
        buffer=None, rows_are_ascii=None, row_is_ascii=None
    ):
        '''Constructor.

        Params:
            target (str): extraction target
            table (str|None): table name
            column (str|None): column name
            schema (str|None): schema name
            n_rows (int|None): number of rows
            row_idx (int|None): row index
            rows_have_null (bool|None): flag for columns with NULL values
            buffer (sequencial|None): buffer for sequential data
            rows_are_ascii (bool|None): flag for ASCII columns
            row_is_ascii (bool|None): flag for a single ASCII row
        '''
        self.target = target
        self.schema = schema
        self.table = table
        self.column = column
        self.n_rows = n_rows
        self.row_idx = row_idx
        self.rows_have_null = rows_have_null
        self.buffer = buffer
        self.rows_are_ascii = rows_are_ascii
        self.row_is_ascii = row_is_ascii



class Collector(metaclass=ABCMeta):
    '''Abstract class for collectors. Collectors repeatidly run
    search algorithms to extract column rows.
    '''
    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): Database engine
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        self.requester = requester
        self.dbms = dbms
        self.n_tasks = n_tasks

        self._row_idx_ctr = 0
        self._row_idx_ctr_lock = asyncio.Lock()
        self._data_lock = asyncio.Lock()


    async def run(self, ctx):
        '''Collects the whole column.

        Params:
            ctx (Context): collection context

        Returns:
            list: column rows
        '''
        logging.info(f'Inferring "{ctx.table}.{ctx.column}"')

        if ctx.n_rows is None:
            ctx.n_rows = await NumericBinarySearch(
                requester=self.requester,
                query_cb=self.dbms.q_rows_count_lt,
                lower=0,
                upper=128,
                find_lower=False,
                find_upper=True,
            ).run(ctx)

        if ctx.rows_have_null is None:
            ctx.rows_have_null = await self.check_rows_have_null(ctx)

        data = [None] * ctx.n_rows
        await asyncio.gather(
            *[self.task_collect_row(deepcopy(ctx), data) for _ in range(self.n_tasks)]
        )

        return data


    async def task_collect_row(self, ctx, data):
        while True:
            async with self._row_idx_ctr_lock:
                if self._row_idx_ctr >= ctx.n_rows:
                    return
                ctx.row_idx = self._row_idx_ctr
                self._row_idx_ctr += 1

            if ctx.rows_have_null and await self.check_row_is_null(ctx):
                res = None
            else:
                res = await self.collect_row(ctx)

            async with self._data_lock:
                data[ctx.row_idx] = res

            logging.info(f'({ctx.row_idx + 1}/{ctx.n_rows}) "{ctx.table}.{ctx.column}": {res}')


    @abstractmethod
    async def collect_row(self, ctx, *args, **kwargs):
        '''Collects a row.

        Params:
            ctx (Context): collection context

        Returns:
            value: single row
        '''
        raise NotImplementedError()


    async def check_rows_have_null(self, ctx):
        query = self.dbms.q_rows_have_null(ctx)
        return await self.requester.request(ctx, query)


    async def check_row_is_null(self, ctx):
        query = self.dbms.q_row_is_null(ctx)
        return await self.requester.request(ctx, query)



class IntCollector(Collector):
    '''Collector for integer columns'''
    async def collect_row(self, ctx):
        return await NumericBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_int_lt,
            lower=0,
            upper=128,
            find_lower=True,
            find_upper=True,
        ).run(ctx)



class FloatCollector(Collector):
    '''Collector for integer columns'''
    async def collect_row(self, ctx):
        ctx.buffer = ''
        while True:
            c = await self.collect_char(ctx)
            if c == EOS:
                return float(ctx.buffer)
            ctx.buffer += c


    async def collect_char(self, ctx):
        return await BinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_float_char_in_set,
            values=CHARSET_DIGITS,
        ).run(ctx)



class BlobCollector(Collector):
    '''Collector for blob columns'''
    async def collect_row(self, ctx):
        ctx.buffer = b''
        while True:
            b = await self.collect_byte(ctx)
            if b == EOS:
                return ctx.buffer
            ctx.buffer += b


    async def collect_byte(self, ctx):
        res = await NumericBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_byte_lt,
            lower=0,
            upper=BYTE_MAX + 2,
            find_lower=False,
            find_upper=False,
        ).run(ctx)
        return EOS if res == BYTE_MAX + 1 else res.to_bytes(1, 'big')



class TextCollector(Collector):
    '''Collector for text columns.'''
    def __init__(self, requester, dbms, charset=None, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): Database engine
            charset (list|None): list of possible characters, None for default ASCII
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        super().__init__(requester, dbms, n_tasks)
        self.charset = charset
        if self.charset and EOS not in self.charset:
            self.charset.append(EOS)


    async def run(self, ctx):
        '''Collects the whole column.

        Params:
            ctx (Context): collection context

        Returns:
            list: column rows
        '''
        if ctx.rows_are_ascii is None:
            ctx.rows_are_ascii = await self.check_rows_are_ascii(ctx)

        return await super().run(ctx)


    async def collect_row(self, ctx):
        '''Collects a row.

        Params:
            ctx (Context): collection context

        Returns:
            string: single row
        '''
        ctx.row_is_ascii = True if ctx.rows_are_ascii else await self.check_row_is_ascii(ctx)

        ctx.buffer = ''
        while True:
            c = await self.collect_char(ctx)
            if c == EOS:
                return ctx.buffer
            ctx.buffer += c


    @abstractmethod
    async def collect_char(self, ctx):
        '''Collects a character.

        Params:
            ctx (Context): collection context

        Returns:
            string: single character
        '''
        raise NotImplementedError()


    async def check_rows_are_ascii(self, ctx):
        '''Finds out whether all rows in column are ASCII.

        Params:
            ctx (Context): collection context

        Returns:
            bool: ASCII flag
        '''
        query = self.dbms.q_rows_are_ascii(ctx)
        return await self.requester.request(ctx, query)


    async def check_row_is_ascii(self, ctx):
        '''Finds out whether current row is ASCII.

        Params:
            ctx (Context): collection context

        Returns:
            bool: ASCII flag
        '''
        query = self.dbms.q_row_is_ascii(ctx)
        return await self.requester.request(ctx, query)


    async def check_char_is_ascii(self, ctx):
        '''Finds out whether current character is ASCII.

        Params:
            ctx (Context): collection context

        Returns:
            bool: ASCII flag
        '''
        query = self.dbms.q_char_is_ascii(ctx)
        return await self.requester.request(ctx, query)



class BinaryTextCollector(TextCollector):
    '''Binary search text collector'''
    async def collect_char(self, ctx):
        '''Collects a character.

        Params:
            ctx (Context): collection context

        Returns:
            string: single character
        '''
        res = await self._collect_or_emulate_char(ctx)
        return res[0]


    async def emulate_char(self, ctx, correct):
        '''Emulates character collection without sending requests.

        Params:
            ctx (Context): collection context
            correct (str): correct character

        Returns:
            int: number of requests necessary
        '''
        res = await self._collect_or_emulate_char(ctx, correct)
        return res[1]


    async def _collect_or_emulate_char(self, ctx, correct=None):
        total_queries = 0

        # custom charset
        if self.charset:
            search_alg = BinarySearch(
                requester=self.requester,
                query_cb=self.dbms.q_char_in_set,
                values=self.charset,
                correct=correct,
            )
            res = await search_alg.run(ctx)
            total_queries += search_alg.n_queries

            if res is not None:
                return res, total_queries

        # ASCII
        if correct is not None:
            correct_ord = ASCII_MAX + 1 if correct == EOS else ord(correct)
        else:
            correct_ord = None

        if ctx.row_is_ascii or await self._check_or_emulate_char_is_ascii(ctx, correct):
            search_alg = NumericBinarySearch(
                requester=self.requester,
                query_cb=self.dbms.q_char_lt,
                lower=0,
                upper=ASCII_MAX + 2,
                find_lower=False,
                find_upper=False,
                correct=correct_ord,
            )
            res = await search_alg.run(ctx)
            res = EOS if res == ASCII_MAX + 1 else chr(res)

            total_queries += search_alg.n_queries
            return res, total_queries

        # Unicode
        search_alg = NumericBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_char_lt,
            lower=ASCII_MAX + 1,
            upper=UNICODE_MAX + 1,
            find_lower=False,
            find_upper=False,
            correct=correct_ord,
        )
        res = await search_alg.run(ctx)
        res = chr(res)

        total_queries += search_alg.n_queries
        return res, total_queries


    async def _check_or_emulate_char_is_ascii(self, ctx, correct):
        if correct is None:
            return await self.check_char_is_ascii(ctx)
        return correct.isascii()



class ModelTextCollector(TextCollector):
    '''Language model-based text collector.'''
    def __init__(self, requester, dbms, model, charset=None, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): Database engine
            model (Model): language model
            charset (list|None): list of possible characters
            n_tasks (int): number of extraction tasks to run in parallel

        Returns:
            list: column rows
        '''
        super().__init__(requester, dbms, charset, n_tasks)
        self.model = model
        self.binary_collector = BinaryTextCollector(
            requester=self.requester,
            dbms=self.dbms,
            charset=self.charset,
            n_tasks=self.n_tasks,
        )


    async def collect_char(self, ctx):
        '''Collects a character.

        Params:
            ctx (Context): collection context

        Returns:
            string: single character
        '''
        res = await self._collect_or_emulate_char(ctx)
        return res[0]


    async def emulate_char(self, ctx, correct):
        '''Emulates character collection without sending requests.

        Params:
            ctx (Context): collection context
            correct (str): correct character

        Returns:
            int: number of requests necessary
        '''
        res = await self._collect_or_emulate_char(ctx, correct)
        return res[1]


    async def _collect_or_emulate_char(self, ctx, correct=None):
        n_queries_model = 0

        model_ctx = tokenize(ctx.buffer, add_eos=False)

        scores = await self.model.scores(context=model_ctx)

        search_alg = TreeSearch(
            requester=self.requester,
            query_cb=self.dbms.q_char_in_set,
            tree=make_tree(scores),
            correct=correct,
        )
        res = await search_alg.run(ctx)
        n_queries_model = search_alg.n_queries

        if res is not None:
            return res, n_queries_model

        res, n_queries_binary = await self.binary_collector._collect_or_emulate_char(ctx, correct)
        return res, n_queries_model + n_queries_binary



class AdaptiveTextCollector(ModelTextCollector):
    '''Same as ModelTextCollector but adapts the model.'''
    async def collect_char(self, ctx):
        c = await super().collect_char(ctx, correct)
        await self.model.fit_correct_char(c, partial_str=ctx.buffer)
        return c



class DynamicTextStats:
    '''Helper class of DynamicTextCollector to keep track of statistical information.'''
    def __init__(self):
        self._str_len_mean = 0.0
        self._n_strings = 0
        self._rpc = {
            'binary': {'mean': 0.0, 'hist': []},
            'unigram': {'mean': 0.0, 'hist': []},
            'fivegram': {'mean': 0.0, 'hist': []},
        }
        self._lock = asyncio.Lock()


    async def update_str(self, s):
        async with self._lock:
            self._n_strings += 1
            self._str_len_mean = (self._str_len_mean * (self._n_strings - 1) + len(s)) / self._n_strings


    async def update_rpc(self, strategy, n_queries):
        async with self._lock:
            rpc = self._rpc[strategy]
            rpc['hist'].append(n_queries)
            rpc['hist'] = rpc['hist'][-100:]
            rpc['mean'] = sum(rpc['hist']) / len(rpc['hist'])


    async def rpc(self, strategy):
        async with self._lock:
            return self._rpc[strategy]['mean']


    async def best_strategy(self):
        async with self._lock:
            return min(self._rpc, key=lambda strategy: self._rpc[strategy]['mean'])


    async def str_len_mean(self):
        async with self._lock:
            return self._str_len_mean



class DynamicTextCollector(TextCollector):
    '''Dynamic text collector. The collector keeps statistical information (RPC)
    for several strategies (binary search, unigram, and five-gram) and dynamically
    chooses the best one. In addition, it uses the statistical information to
    identify when guessing whole strings is likely to succeed and then uses
    previously inferred strings to make the guesses.
    '''
    def __init__(self, requester, dbms, charset=None, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): Database engine
            charset (list|None): list of possible characters
            n_tasks (int): number of extraction tasks to run in parallel

        Other Attributes:
            model_unigram (Model): adaptive unigram model
            model_fivegram (Model): adaptive five-gram model
            guess_collector (StringGuessCollector): collector for guessing
        '''
        super().__init__(requester, dbms, charset, n_tasks)
        self.binary_collector = BinaryTextCollector(
            requester=self.requester,
            dbms=self.dbms,
            charset=self.charset,
            n_tasks=self.n_tasks,
        )
        self.unigram_collector = ModelTextCollector(
            requester=self.requester,
            dbms=self.dbms,
            model=hakuin.Model(1),
            charset=self.charset,
            n_tasks=self.n_tasks,
        )
        self.fivegram_collector = ModelTextCollector(
            requester=self.requester,
            dbms=self.dbms,
            model=hakuin.Model(5),
            charset=self.charset,
            n_tasks=self.n_tasks,
        )
        self.guess_collector = StringGuessingCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        self.stats = DynamicTextStats()


    async def collect_row(self, ctx):
        ctx.row_is_ascii = True if ctx.rows_are_ascii else await self.check_row_is_ascii(ctx)

        s = await self._collect_string(ctx)
        await self.guess_collector.model.fit_single(s, context=[])
        await self.stats.update_str(s)

        return s


    async def _collect_string(self, ctx):
        '''Tries to guess strings or extracts them on per-character basis if guessing fails'''
        best_strategy = await self.stats.best_strategy()
        best_rpc = await self.stats.rpc(best_strategy)
        exp_c = await self.stats.str_len_mean() * best_rpc
        correct_str = await self.guess_collector.collect_row(ctx, exp_c)

        if correct_str is not None:
            await self._update_stats_str(ctx, correct_str)
            await self.unigram_collector.model.fit_data([correct_str])
            await self.fivegram_collector.model.fit_data([correct_str])
            return correct_str

        return await self._collect_string_per_char(ctx)


    async def _collect_string_per_char(self, ctx):
        ctx.buffer = ''
        while True:
            c = await self.collect_char(ctx)
            await self._update_stats(ctx, c)
            await self.unigram_collector.model.fit_correct_char(c, partial_str=ctx.buffer)
            await self.fivegram_collector.model.fit_correct_char(c, partial_str=ctx.buffer)

            if c == EOS:
                return ctx.buffer
            ctx.buffer += c

        return ctx.buffer


    async def collect_char(self, ctx):
        '''Chooses the best strategy and uses it to infer a character.'''
        best = await self.stats.best_strategy()
        if best == 'binary':
            return await self.binary_collector.collect_char(ctx)
        elif best == 'unigram':
            return await self.unigram_collector.collect_char(ctx)
        else:
            return await self.fivegram_collector.collect_char(ctx)


    async def _update_stats(self, ctx, correct):
        '''Emulates all strategies without sending requests and updates the statistical information.'''
        collectors = (
            ('binary', self.binary_collector),
            ('unigram', self.unigram_collector),
            ('fivegram', self.fivegram_collector),
        )

        for strategy, collector in collectors:
            n_queries = await collector.emulate_char(ctx, correct)
            await self.stats.update_rpc(strategy, n_queries)


    async def _update_stats_str(self, ctx, correct_str):
        '''Like _update_stats but for whole strings.'''
        ctx.buffer = ''
        for c in correct_str:
            await self._update_stats(ctx, c)
            ctx.buffer += c



class StringGuessingCollector(Collector):
    '''String guessing collector. The collector keeps track of previously extracted
    strings and opportunistically tries to guess new strings.
    '''
    GUESS_TH = 0.5
    GUESS_SCORE_TH = 0.01


    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            dbms (DBMS): Database engine
            n_tasks (int): number of extraction tasks to run in parallel

        Other Attributes:
            GUESS_TH (float): minimal threshold necessary to start guessing
            GUESS_SCORE_TH (float): minimal threshold for strings to be eligible for guessing
            model (Model): adaptive string-based model for guessing
        '''
        super().__init__(requester, dbms, n_tasks)
        self.model = hakuin.Model(1)


    async def collect_row(self, ctx, exp_alt=None):
        '''Tries to construct a guessing Huffman tree and searches it in case of success.

        Params:
            ctx (Context): collection context
            exp_alt (float|None): expectation for alternative extraction method or None if it does not exist

        Returns:
            string|None: guessed string or None if skipped or failed
        '''
        exp_alt = exp_alt if exp_alt is not None else float('inf')
        tree = await self._get_guess_tree(ctx, exp_alt)
        return await TreeSearch(
            requester=self.requester,
            query_cb=self.dbms.q_string_in_set,
            tree=tree,
        ).run(ctx)


    async def _get_guess_tree(self, ctx, exp_alt):
        '''Identifies, whether string guessing is likely to succeed and if so,
        it constructs a Huffman tree from previously inferred strings.

        Params:
            ctx (Context): collection context
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

        scores = await self.model.scores(context=[])
        scores = {k: v for k, v in scores.items() if v >= self.GUESS_SCORE_TH and await self.model.count(k, []) > 1}
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
