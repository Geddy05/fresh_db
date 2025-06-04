import json
import math

class Node:

    def __init__(self, order, leaf=False):
        self.order = order
        self.leaf = leaf
        self.keys = []
        self.values = []       # Only used for leaves
        self.children = []  # Only used for internal nodes
        self.next = None       # For leaf-level linked list (optional)

class BplusTree:
    def __init__(self, order=4):
        self.root = Node(order, leaf=True)
        self.order = order

    def _find_leaf(self, key):
        node = self.root
        while not node.leaf:
            idx = self._find_index(node.keys, key)
            node = node.children[idx]
        return node
    
    def _find_index(self, keys, key):
        for i, k in enumerate(keys):
            if key < k:
                return i
        return len(keys)
    
    def insert(self, key, value):
        root = self.root
        if len(root.keys) == self.order - 1:
            new_root = Node(self.order)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node, key, value):
        idx = self._find_index(node.keys, key)
        if node.leaf:
            if key in node.keys:
                raise ValueError("Duplicate key")
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
        else:
            child = node.children[idx]
            if len(child.keys) == self.order - 1:
                self._split_child(node, idx)
                if key > node.keys[idx]:
                    idx += 1
            self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent, idx):
        order = self.order
        node = parent.children[idx]
        mid = order // 2
        split_key = node.keys[mid]

        # Split keys/values
        left = Node(order, leaf=node.leaf)
        right = Node(order, leaf=node.leaf)

        left.keys = node.keys[:mid]
        right.keys = node.keys[mid + (0 if node.leaf else 1):]

        if node.leaf:
            left.values = node.values[:mid]
            right.values = node.values[mid:]
            # Maintain leaf node links for fast scan (optional, advanced)
            right.next = node.next
            left.next = right # type: ignore
        else:
            left.children = node.children[:mid + 1]
            right.children = node.children[mid + 1:]

        # Insert in parent
        parent.keys.insert(idx, split_key)
        parent.children[idx] = left
        parent.children.insert(idx + 1, right)

    def search(self, key):
        node = self.root
        while not node.leaf:
            idx = self._find_index(node.keys, key)
            node = node.children[idx]
        if key in node.keys:
            i = node.keys.index(key)
            return node.values[i]
        return None
    
    def scan(self, start_key=None):
        """Yield (key, value) pairs in sorted order, optionally from start_key."""
        node = self.root
        # Go to leftmost leaf
        while not node.leaf:
            node = node.children[0]
        idx = 0
        if start_key is not None:
            idx = self._find_index(node.keys, start_key)
        while node:
            for i in range(idx, len(node.keys)):
                yield node.keys[i], node.values[i]
            node = getattr(node, "next", None)
            idx = 0

    def to_dict(self, node=None):
        """Recursively convert the tree to a serializable dict."""
        node = node or self.root
        d = {
            "keys": node.keys,
            "leaf": node.leaf
        }
        if node.leaf:
            d["values"] = node.values
        else:
            d["children"] = [self.to_dict(child) for child in node.children]
        return d

    def save(self, filepath):
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f)

    @classmethod
    def from_dict(cls, d, order=32):
        """Reconstruct tree from dict."""
        def _build(node_dict):
            node = Node(order, leaf=node_dict["leaf"])
            node.keys = node_dict["keys"]
            if node.leaf:
                node.values = node_dict["values"]
            else:
                node.children = [_build(child) for child in node_dict["children"]]
            return node
        tree = cls(order)
        tree.root = _build(d)
        return tree

    @classmethod
    def load(cls, filepath, order=32):
        with open(filepath, "r") as f:
            d = json.load(f)
        return cls.from_dict(d, order=order)

    def __repr__(self):
        # Simple text view of the tree (for debugging)
        levels = []
        def visit(node, depth=0):
            if len(levels) <= depth:
                levels.append([])
            levels[depth].append(node.keys)
            if not node.leaf:
                for child in node.children:
                    visit(child, depth+1)
        visit(self.root)
        return "\n".join(f"Level {i}: {l}" for i, l in enumerate(levels))
