class Node:
    def __init__(self, value, prob, left=None, right=None):
        self.value = value
        self.prob = prob
        self.left = left
        self.right = right


    def is_leaf(self):
        return self.left is None


    def values(self):
        if self.is_leaf():
            return [self.value] if self.value is not None else []

        res = self.left.values()
        if self.right:
            res += self.right.values()
        return res


    def probabilities(self):
        if self.is_leaf():
            return [self.prob] if self.value is not None else []

        res = self.left.probabilities()
        if self.right:
            res += self.right.probabilities()
        return res


    def search_cost(self):
        return self._search_cost(0)


    def _search_cost(self, n=0):
        if self.is_leaf():
            return None if self.value is None else n * self.prob

        exp_h = self.left._search_cost(n + 1)
        if self.right:
            exp_h += self.right._search_cost(n + 1)
        return exp_h



def make_tree(probabilities):
    if not probabilities:
        return None

    nodes = [Node(c, s) for c, s in probabilities.items()]

    if len(nodes) == 1:
        return Node(None, nodes[0].prob, nodes[0], None)

    while len(nodes) > 1:
        nodes.sort(key=lambda n: n.prob)
        l = nodes.pop(0)
        r = nodes.pop(0)

        if max(l.probabilities()) >= max(r.probabilities()):
            nodes.append(Node(None, l.prob + r.prob, l, r))
        else:
            nodes.append(Node(None, l.prob + r.prob, r, l))

    return nodes[0]
