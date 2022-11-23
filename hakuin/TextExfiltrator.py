from abc import ABCMeta, abstractmethod

from hakuin.huffman import make_tree
from hakuin.utils import CHARSET_ASCII, CHARSET_LOWER
from hakuin.utils import split_at



class TextExfiltrator(metaclass=ABCMeta):
    def __init__(self):
        pass


    @abstractmethod
    def try_query(self, table, column, row, s, values):
        raise NotImplementedError()


    def _get_scores(self, model, s):
        s = ['<s>'] + list(s)
        s = s[-(model.max_ngram - 1):]

        while s:
            scores = model.score_dict(s)
            if scores:
                return scores
            s.pop(0)

        return dict()


    def search_binary(self, charset, verified=False, **kwargs):
        if not charset:
            return None

        if len(charset) == 1:
            if verified or self.try_query(values=charset, **kwargs):
                return charset[0]
            return None

        left, right = split_at(charset, (len(charset) + 1) // 2)
        if self.try_query(values=left, **kwargs):
            return self.search_binary(left, True, **kwargs)
        else:
            return self.search_binary(right, verified, **kwargs)


    def search_tree(self, tree, verified=False, **kwargs):
        if tree is None:
            return None

        if tree.left is None:
            if verified or self.try_query(values=tree.values(), **kwargs):
                return tree.values()[0]
            return None

        if self.try_query(values=tree.left.values(), **kwargs):
            return self.search_tree(tree.left, True, **kwargs)
        else:
            if tree.right is None:
                return None
            return self.search_tree(tree.right, verified, **kwargs)


    def exfiltrate_string_binary(self, charset, verified, table, column, row):
        s = ''
        while True:
            c = self.search_binary(charset, verified, table=table, column=column, row=row, s=s)
            if c == '</s>':
                return s
            s += c


    def exfiltrate_string(self, model, charset, verified, table, column, row):
        s = ''
        while True:
            scores = self._get_scores(model, s)
            tree = make_tree(scores)
            c = self.search_tree(tree=tree, table=table, column=column, row=row, s=s)

            if c is None:
                charset = list(set(charset).difference(set(scores)))
                c = self.search_binary(charset, verified, table=table, column=column, row=row, s=s)

            if c == '</s>':
                return s
            s += c


    def exfiltrate_data_binary(self, charset, verified, table, column, n):
        data = []
        for row in range(n):
            data.append(self.exfiltrate_string_binary(charset, verified, table, column, row))
        return data


    def exfiltrate_data(self, model, charset, verified, table, column, n):
        data = []
        for row in range(n):
            data.append(self.exfiltrate_string(model, charset, verified, table, column, row))
        return data


    def exfiltrate_data_adaptive(self, model, table, column, n):
        data = []
        for row in range(n):
            s = ''
            while True:
                scores = self._get_scores(model, s)
                tree = make_tree(scores)
                c = self.search_tree(tree=tree, table=table, column=column, row=row, s=s)

                if c is None:
                    charset = list(set(CHARSET_ASCII).difference(set(scores)))
                    c = self.search_binary(charset, verified=True, table=table, column=column, row=row, s=s)

                model.fit_correct(s, c)

                if c == '</s>':
                    break
                s += c

            data.append(s)
        return data
