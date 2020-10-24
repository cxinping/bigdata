# -*- coding: UTF-8 -*-
class TreeNode:
    def __init__(self, name):
        self._name = name
        self._left_node = None
        self._right_node = None

class Tree:
    root = None
    nodes = []

    def add_node(self, name, index):
        node = TreeNode(name)

        if Tree.root is None:
            Tree.root = node
            Tree.nodes.append(node)
        else:
            if index % 2 == 1:
                tmp_node = Tree.nodes[index - 1]
            elif index % 2 == 0:
                tmp_node = Tree.nodes[index - 2]

            if tmp_node._left_node is None:
                tmp_node._left_node = node
                Tree.nodes.append(tmp_node._left_node)
            elif tmp_node._right_node is None:
                tmp_node._right_node = node
                Tree.nodes.append(tmp_node._right_node)


tree = Tree()
all_node = ["Root", "Node1", "Node2", "Node3",
            "Node4", "Node5", "Node6", "Node7",
            "Node8", "Node9", "Node10"]
total = len(all_node)
for i, val in enumerate(all_node):
    tree.add_node(val, i)

	