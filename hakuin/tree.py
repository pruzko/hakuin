class Node:
    def __init__(self, value, prob, left=None, middle=None, right=None):
        self.value = value
        self.prob = prob
        self.left = left
        self.middle = middle
        self.right = right


    @property
    def is_leaf(self):
        return self.left is None and self.middle is None and self.right is None


    @property
    def leaves(self):
        if self.is_leaf:
            return [self]
        
        leaves = []
        if self.left:
            leaves.extend(self.left.leaves)
        if self.middle:
            leaves.extend(self.middle.leaves)
        if self.right:
            leaves.extend(self.right.leaves)
        return leaves


    @property
    def height(self):
        l = self.left.height if self.left else 0
        m = self.middle.height if self.middle else 0
        r = self.right.height if self.right else 0
        return max([l, m, r]) + 1


    def values(self):
        return [node.value for node in self.leaves]


    def probs(self):
        return [node.prob for node in self.leaves]


    def success_prob(self):
        return sum(self.probs())


    def success_cost(self, n=0):
        if self.is_leaf:
            return n * self.prob

        cost = self.left.success_cost(n + 1)
        if self.middle:
            cost += self.middle.success_cost(n + 1)
        if self.right:
            cost += self.right.success_cost(n + 1)
        return cost


    def fail_cost(self):
        if self.right is None:
            return 1.0
        return self.right.fail_cost() + 1.0


    def dump(self, last=True, header=''):
        elbow = '└──'
        pipe = '│  '
        tee = '├──'
        blank = '   '

        prob = round(self.prob, 2)
        node_str = f' {prob} [{self.value}]' if self.value is not None else f' {prob}'
        print(f'{header}{elbow if last else tee}{node_str}')

        header += blank if last else pipe

        if self.left:
            last = self.middle is None and self.right is None
            self.left.dump(header=header, last=last)
        if self.middle:
            last = self.right is None
            self.middle.dump(header=header, last=last)
        if self.right:
            self.right.dump(header=header, last=True)



def make_huffman_binary_tree(probs):
    if not probs:
        return None

    heap = [Node(value=key, prob=prob) for key, prob in probs.items()]
    while len(heap) > 1:
        heap.sort(key=lambda node: node.prob)
        left = heap.pop(0)
        right = heap.pop(0)

        if max(left.probs()) >=  max(right.probs()):
            node = Node(value=None, prob=left.prob + right.prob, left=left, right=right)
        else:
            node = Node(value=None, prob=left.prob + right.prob, left=right, right=left)
        heap.append(node)

    return heap[0]


def make_huffman_ternary_tree(probs):
    if not probs:
        return None

    heap = [Node(value=key, prob=prob) for key, prob in probs.items()]
    while len(heap) > 1:
        heap.sort(key=lambda node: node.prob)

        nodes = [heap.pop(0), heap.pop(0)]
        nodes.append(heap.pop(0) if len(heap) % 2 else None)

        nodes.sort(key=lambda n: max(n.probs()) if n else float('-inf'), reverse=True)
        prob = sum([n.prob for n in nodes if n])
        node = Node(value=None, prob=prob, left=nodes[0], middle=nodes[1], right=nodes[2])
        heap.append(node)

    return heap[0]


def make_huffman_tree(probs, use_ternary=False):
    if use_ternary:
        return make_huffman_ternary_tree(probs)
    return make_huffman_binary_tree(probs)


def make_balanced_tree(values, use_ternary=False):
    if not values:
        return None

    probs = {v:1.0 / len(values) for v in values}
    return make_huffman_tree(probs=probs, use_ternary=use_ternary)
