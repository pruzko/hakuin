class Node:
    def __init__(self, char, freq, left=None, right=None):
        self.char = char
        self.freq = freq
        self.left = left
        self.right = right


def _get_node_heights(node_heights, node, height=0):
    if node is None:
        return

    if node.char is not None:
        node_heights[node.char] = height
        return

    _get_node_heights(node_heights, node.left, height + 1)
    _get_node_heights(node_heights, node.right, height + 1)


def get_node_heights(node):
    node_heights = {}
    _get_node_heights(node_heights, node)
    return node_heights


def huffman(char_freq):
    if not char_freq:
        return dict()

    nodes = [Node(c, f) for c, f in char_freq.items()]

    if len(nodes) == 1:
        return {nodes[0].char: 1.0}

    while len(nodes) > 1:
        nodes = sorted(nodes, key=lambda x: x.freq)
        l = nodes.pop(0)
        r = nodes.pop(0)
        nodes.append(Node(None, l.freq + r.freq, l, r))

    return get_node_heights(nodes[0])
