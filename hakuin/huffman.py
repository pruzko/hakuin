class Node:
    def __init__(self, char, score, left=None, right=None):
        self.char = char
        self.score = score
        self.left = left
        self.right = right


    def values(self):
        if self.left is None:
            return [self.char] if self.char is not None else []

        res = self.left.values()
        if self.right:
            res += self.right.values()
        return res


    def scores(self):
        if self.left is None:
            return [self.score] if self.char is not None else []

        res = self.left.scores()
        if self.right:
            res += self.right.scores()
        return res


def make_tree(scores):
    if not scores:
        return None

    nodes = [Node(c, s) for c, s in scores.items()]

    if len(nodes) == 1:
        return Node(None, nodes[0].score, nodes[0], None)

    while len(nodes) > 1:
        nodes.sort(key=lambda n: n.score)
        l = nodes.pop(0)
        r = nodes.pop(0)

        if max(l.scores()) >= max(r.scores()):
            nodes.append(Node(None, l.score + r.score, l, r))
        else:
            nodes.append(Node(None, l.score + r.score, r, l))

    return nodes[0]
