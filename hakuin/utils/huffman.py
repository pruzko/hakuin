# TODO make a tree class that has nodes to make sure the functions are always called properly
class Node:
    def __init__(self, value, freq, left=None, right=None):
        self.value = value
        self.freq = freq
        self.left = left
        self.right = right


    def values(self):
        if self.left is None:
            return [self.value] if self.value is not None else []

        res = self.left.values()
        if self.right:
            res += self.right.values()
        return res


    def scores(self):
        if self.left is None:
            return [self.freq] if self.value is not None else []

        res = self.left.scores()
        if self.right:
            res += self.right.scores()
        return res


    def expected_height(self):
        return self._expected_height(0)


    def _expected_height(self, n=0):
        if not self.left:
            return None if self.value is None else n * self.freq

        exp_h = self.left._expected_height(n + 1)
        if self.right:
            exp_h += self.right._expected_height(n + 1)
        return exp_h



def make_tree(scores):
    if not scores:
        return None

    nodes = [Node(c, s) for c, s in scores.items()]

    if len(nodes) == 1:
        return Node(None, nodes[0].freq, nodes[0], None)

    while len(nodes) > 1:
        nodes.sort(key=lambda n: n.freq)
        l = nodes.pop(0)
        r = nodes.pop(0)

        if max(l.scores()) >= max(r.scores()):
            nodes.append(Node(None, l.freq + r.freq, l, r))
        else:
            nodes.append(Node(None, l.freq + r.freq, r, l))

    return nodes[0]
