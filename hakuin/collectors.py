import logging
from abc import ABCMeta, abstractmethod
from collections import Counter

import hakuin
from hakuin.utils import tokenize, CHARSET_ASCII, EOS
from hakuin.utils.huffman import make_tree
from hakuin.optimizers import Context, BinarySearch, TreeSearch



class Collector(metaclass=ABCMeta):
    '''Abstract class for collectors. Collectors repeatidly run
    Optimizers (i.e., search algorithms) to infer column rows.
    '''
    def run(self, ctx, n_rows):
        '''Run the collection.

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
        '''Collect single row.

        Params:
            ctx (Context): inference context

        Returns:
            value: single column row
        '''
        raise NotImplementedError()



class TextCollector(Collector):
    '''Collector for text columns.'''
    def _collect_row(self, ctx):
        '''Collects column row.

        Params:
            ctx (Context): inference context

        Returns:
            str: single column row
        '''
        ctx.s = ''
        while True:
            c = self._search_char(ctx)
            if c == EOS:
                return ctx.s
            ctx.s += c


    @abstractmethod
    def _search_char(self, ctx):
        '''Collect single character.

        Params:
            ctx (Context): inference context

        Returns:
            str: single character
        '''
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
        self.requester = requester
        self.query_cb = query_cb
        self.charset = charset if charset else CHARSET_ASCII


    def _search_char(self, ctx):
        '''Finds a character with binary search.

        Params:
            ctx (Context): inference context

        Returns:
            str: single character
        '''
        return BinarySearch(
            self.requester,
            self.query_cb,
            values=self.charset,
        ).run(ctx)



class ModelTextCollector(TextCollector):
    '''Text collector that uses model and Huffman trees.'''
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
        self.requester = requester
        self.query_cb = query_cb
        self.model = model
        self.charset = charset if charset else CHARSET_ASCII


    def _search_char(self, ctx):
        '''Finds a character with Huffman trees or
        binary search in case the former fails.

        Params:
            ctx (Context): inference context

        Returns:
            str: single character
        '''
        model_context = tokenize(ctx.s, add_eos=False)
        model_context = model_context[-(self.model.max_ngram - 1):]
        scores = self.model.score_any_dict(model_context)

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
    '''Same as ModelTextCollector but adapts the models throughout inference.'''
    def _search_char(self, ctx):
        '''Same as ModelTextCollector._search_char but adapts the model
        with newly inferred characters.

        Params:
            ctx (Context): inference context

        Returns:
            str: single character
        '''
        c = super()._search_char(ctx)
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
    '''
    GUESS_TH = 0.5


    def __init__(self, requester, query_char_cb, query_string_cb, charset=None):
        '''Constructor.

        Params:
            requester (Requester): Requester instance
            query_char_cb (function): query construction function for searching characters
            query_string_cb (function): query construction function for searching strings
            charset (list|None): list of possible characters

        Other Attributes:
            model_guess: adaptive model that keeps track of previously inferred characters and
                         their probabilities
            model_unigram: adaptive unigram model
            model_adaptive: adaptive five-gram model
            _stats: statistical information
        '''
        self.requester = requester
        self.query_char_cb = query_char_cb
        self.query_string_cb = query_string_cb
        self.charset = charset if charset else CHARSET_ASCII
        self.model_guess = hakuin.Model.make_clean(1)
        self.model_unigram = hakuin.Model.make_clean(1)
        self.model_adaptive = hakuin.Model.make_clean(5)
        self._stats = {
            'rpc': {
                'binary': {'avg': 0.0, 'hist': []},
                'adaptive': {'avg': 0.0, 'hist': []},
                'unigram': {'avg': 0.0, 'hist': []},
            },
            'avg_len': 0.0,
            'n_strings': 0,
        }


    def _collect_row(self, ctx):
        '''Collects column row but also update the 'model_guess' with the newly
        inferred string and updates the statistical information.

        Params:
            ctx (Context): inference context

        Returns:
            str: single column row
        '''
        s = self._get_string(ctx)
        self.model_guess.fit_correct([], s)

        st = self._stats
        st['n_strings'] += 1
        st['avg_len'] = (st['avg_len'] * (st['n_strings'] - 1) + len(s)) / st['n_strings']
        return s


    def _get_string(self, ctx):
        '''Identifies if guessings strings is likely to succeed and if yes, it makes guesses.
        If guessing does not take place or fails, it proceeds with per-character inference.
        Also, the 'model_unigram' and 'model_adaptive' are adapted with every newly inferred
        character.
        '''
        correct_str = self._guess_value(ctx)
        if correct_str is not None:
            self.model_unigram.fit([correct_str])
            self.model_adaptive.fit([correct_str])

            ctx.s = ''
            for c in correct_str:
                self._eval_modes(ctx, c)
                ctx.s += c

            return correct_str

        ctx.s = ''
        while True:
            c = self._search_char(ctx)

            self._eval_modes(ctx, c)
            self.model_unigram.fit_correct(ctx.s, c)
            self.model_adaptive.fit_correct(ctx.s, c)

            if c == EOS:
                return ctx.s
            ctx.s += c


    def _guess_value(self, ctx):
        '''Tries to construct a guessing Huffman tree and searches it in case of success.'''
        tree = self._get_guess_tree(ctx)
        return TreeSearch(
            self.requester,
            self.query_string_cb,
            tree=tree,
        ).run(ctx)


    def _get_guess_tree(self, ctx):
        '''Identifies, whether string guessing is likely to succed and if so,
        also constructs a Huffman tree from previously inferred strings.

        Returns:
            utils.huffman.Node|None: Huffman tree constructed from previously inferred
            strings that are likely to succeed or None if no such strings were found
        '''

        # Compute the expectation "exp_c" for per-character inference.
        # exp_c = avg_len * best_strategy_rpc
        exp_c = self._stats['avg_len'] * self._stats['rpc'][self._best_mode()]['avg']

        # Compute the best expectation "best_exp_g" by iteratively inserting previously
        # inferred strings into a candidate guess set "guesses" and computing their
        # expectation "exp_g" for guessing. The iteration stops when the minimal "exp_g"
        # is found.
        # exp(G) = p(s in G) * exp_huff(G) + (1 - p(c in G)) * (exp_huff(G) + exp_c)
        guesses = {}
        prob_g = 0.0
        best_prob_g = 0.0
        best_exp_g = float('inf')
        best_tree = None

        scores = self.model_guess.score_dict([])
        scores = {k: v for k, v in scores.items() if self.model_guess.count(k, []) > 1}
        for guess, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
            guesses[guess] = score

            tree = make_tree(guesses)
            exp_tree = tree.expected_height()
            prob_g += score
            exp_g = prob_g * exp_tree + (1 - prob_g) * (exp_tree + exp_c)

            if exp_g > best_exp_g:
                break

            best_prob_g = prob_g
            best_exp_g = exp_g
            best_tree = tree

        if best_exp_g > exp_c or best_prob_g < self.GUESS_TH:
            return None

        return best_tree


    def _search_char(self, ctx):
        '''Chooses the best strategy and uses it to infer a character.'''
        searched_space = set()
        c = self._get_optim(ctx, searched_space, self._best_mode()).run(ctx)
        if c is None:
            c = self._get_optim(ctx, searched_space, 'binary').run(ctx)
        return c


    def _best_mode(self):
        '''Returns the name of the best strategy.'''
        return min(self._stats['rpc'], key=lambda mode: self._stats['rpc'][mode]['avg'])


    def _eval_modes(self, ctx, correct):
        '''Runs (emulates) all strategies without sending any requests and updates the
        statistical information.
        '''
        for mode in self._stats['rpc']:
            result, n_queries = self._get_optim(ctx, set(), mode).eval(ctx, correct)
            if result is None:
                n_queries += self._get_optim(ctx, set(), 'binary').eval(ctx, correct)[1]

            m = self._stats['rpc'][mode]
            m['hist'].append(n_queries)
            m['hist'] = m['hist'][-100:]
            m['avg'] = sum(m['hist']) / len(m['hist'])


    def _get_optim(self, ctx, searched_space, mode):
        '''Returns optimization (search algorithm) and configures it to search
        appropriate space.

        Params:
            ctx (Context): inference context
            searched_space (list): list of values that have already been searched
            mode (str): the name of optimization ('binary_search', 'unigram', 'adaptive')

        Returns:
            Optimization: configured search algorithm
        '''
        if mode == 'adaptive':
            model_context = tokenize(ctx.s, add_eos=False)
            model_context = model_context[-(self.model_adaptive.max_ngram - 1):]
            scores = self.model_adaptive.score_any_dict(model_context)

            searched_space.union(set(scores))
            return TreeSearch(
                self.requester,
                self.query_char_cb,
                tree=make_tree(scores),
            )
        elif mode == 'unigram':
            scores = self.model_unigram.score_dict([])
            searched_space.union(set(scores))
            return TreeSearch(
                self.requester,
                self.query_char_cb,
                tree=make_tree(scores),
            )
        else:
            charset = list(set(self.charset).difference(searched_space))
            return BinarySearch(
                self.requester,
                self.query_char_cb,
                values=self.charset,
            )

