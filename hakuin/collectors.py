from abc import ABCMeta, abstractmethod
from collections import Counter

import hakuin
from hakuin.utils import tokenize, CHARSET_ASCII, EOS
from hakuin.utils.huffman import make_tree
from hakuin.optimizers import Context, BinarySearch, TreeSearch



class Collector(metaclass=ABCMeta):
    def run(self, ctx, n_rows):
        data = []
        for row in range(n_rows):
            ctx = Context(ctx.table, ctx.column, row, None)
            data.append(self._collect_row(ctx))

        return data


    @abstractmethod
    def _collect_row(self, ctx):
        raise NotImplementedError()



class TextCollector(Collector):
    def _collect_row(self, ctx):
        ctx.s = ''
        while True:
            c = self._search_char(ctx)
            if c == EOS:
                return ctx.s
            ctx.s += c


    @abstractmethod
    def _search_char(self, ctx):
        raise NotImplementedError()



class BinaryTextCollector(TextCollector):
    def __init__(self, requester, query_cb, charset=None):
        self.requester = requester
        self.query_cb = query_cb
        self.charset = charset if charset else CHARSET_ASCII


    def _search_char(self, ctx):
        return BinarySearch(
            self.requester,
            self.query_cb,
            values=self.charset,
        ).run(ctx)



class ModelTextCollector(TextCollector):
    def __init__(self, requester, query_cb, model, charset=None):
        self.requester = requester
        self.query_cb = query_cb
        self.model = model
        self.charset = charset if charset else CHARSET_ASCII


    def _search_char(self, ctx):
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
    def _search_char(self, ctx):
        c = super()._search_char(ctx)
        self.model.fit_correct(ctx.s, c)
        return c



class DynamicTextCollector(TextCollector):
    GUESS_TH = 0.5


    def __init__(self, requester, query_char_cb, query_string_cb, charset=None):
        self.requester = requester
        self.query_char_cb = query_char_cb
        self.query_string_cb = query_string_cb
        self.charset = charset if charset else CHARSET_ASCII
        self.model_guess = hakuin.Model.make_clean(1)
        self.model_unigram = hakuin.Model.make_clean(1)
        self.model_adaptive = hakuin.Model.make_clean(5)
        self._stats = {
            'qpc': {
                'binary': {'avg': 0.0, 'hist': []},
                'adaptive': {'avg': 0.0, 'hist': []},
                'unigram': {'avg': 0.0, 'hist': []},
            },
            'avg_len': 0.0,
            'n_strings': 0,
        }


    def _collect_row(self, ctx):
        s = self._get_string(ctx)
        self.model_guess.fit_correct([], s)

        st = self._stats
        st['n_strings'] += 1
        st['avg_len'] = (st['avg_len'] * (st['n_strings'] - 1) + len(s)) / st['n_strings']
        return s


    def _get_string(self, ctx):
        v = self._guess_value(ctx)
        if v is not None:
            return v

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
        tree = self._get_guess_tree(ctx)
        return TreeSearch(
            self.requester,
            self.query_string_cb,
            tree=tree,
        ).run(ctx)


    def _get_guess_tree(self, ctx):
        # select only those values where you expect the qpc less than when you
        # infer the string char by char.
        # expectation for char by char: exp_c = avg_len * best_mode_qpc
        # expectation for guesses: exp(G) = p(s in G) * exp_huff(G) + (1 - p(c in G)) * exp_c
        exp_c = self._stats['avg_len'] * self._stats['qpc'][self._best_mode()]['avg']

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
            prob_g += score
            exp_g = prob_g * tree.expected_height() + (1 - prob_g) * exp_c

            if exp_g > best_exp_g:
                break

            best_prob_g = prob_g
            best_exp_g = exp_g
            best_tree = tree

        if best_exp_g > exp_c or best_prob_g < self.GUESS_TH:
            return None

        return best_tree


    def _search_char(self, ctx):
        searched_space = set()
        c = self._get_optim(ctx, searched_space, self._best_mode()).run(ctx)
        if c is None:
            c = self._get_optim(ctx, searched_space, 'binary').run(ctx)
        return c


    def _best_mode(self):
        return min(self._stats['qpc'], key=lambda mode: self._stats['qpc'][mode]['avg'])


    def _eval_modes(self, ctx, correct):
        for mode in self._stats['qpc']:
            result, n_queries = self._get_optim(ctx, set(), mode).eval(ctx, correct)
            if result is None:
                n_queries += self._get_optim(ctx, set(), 'binary').eval(ctx, correct)[1]

            m = self._stats['qpc'][mode]
            m['hist'].append(n_queries)
            m['hist'] = m['hist'][-100:]
            m['avg'] = sum(m['hist']) / len(m['hist'])


    def _get_optim(self, ctx, searched_space, mode):
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

