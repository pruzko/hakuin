class HuffmanNode:
    def __init__(self, value, prob, left=None, right=None):
        self.value = value
        self.prob = prob
        self.left = left
        self.right = right


    @property
    def is_leaf(self):
        return self.left is None and self.right is None


    @property
    def leaves(self):
        if self.is_leaf:
            return [self]
        
        leaves = []
        if self.left:
            leaves.extend(self.left.leaves)
        if self.right:
            leaves.extend(self.right.leaves)
        return leaves


    @property
    def height(self):
        l = self.left.height if self.left else 0
        r = self.right.height if self.right else 0
        return max([l, r]) + 1


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
        if self.right:
            cost += self.right.success_cost(n + 1)
        return cost


    def fail_cost(self):
        if self.right is None:
            return 1.0
        return self.right.fail_cost() + 1.0



def make_tree(probabilities):
    if not probabilities:
        return None

    heap = [HuffmanNode(value=key, prob=prob) for key, prob in probabilities.items()]
    while len(heap) > 1:
        heap.sort(key=lambda node: node.prob)
        left = heap.pop(0)
        right = heap.pop(0)

        if max(left.probs()) >=  max(right.probs()):
            heap.append(HuffmanNode(value=None, prob=left.prob + right.prob, left=left, right=right))
        else:
            heap.append(HuffmanNode(value=None, prob=left.prob + right.prob, left=right, right=left))

    return heap[0]
