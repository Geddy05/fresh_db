import pickle
import uuid
import os

from storage.block_manager import BlockManager

class Node:
    def __init__(self, order, leaf=False, node_id=None):
        self.order = order
        self.leaf = leaf
        self.keys = []
        self.values = []       # Only used for leaves
        self.children = []     # List of CHILD NODE IDS, not objects!
        self.next = None       # node_id of next leaf (not object)
        self.node_id = node_id or str(uuid.uuid4())
        self.dirty = True

class BplusTree:
    def __init__(self, order=4, block_manager=None, root_node_id=None):
        self.order = order
        self.block_manager = block_manager
        if root_node_id:
            self.root_node_id = root_node_id
            self.root = self.load_node(root_node_id)
        else:
            root = Node(order, leaf=True)
            self.save_node(root)
            self.root = root
            self.root_node_id = root.node_id

    def _find_index(self, keys, key):
        for i, k in enumerate(keys):
            if key < k:
                return i
        return len(keys)

    def _find_leaf(self, key):
        node = self.root
        while not node.leaf:
            idx = self._find_index(node.keys, key)
            child_id = node.children[idx]
            node = self.load_node(child_id)
        return node

    def save_node(self, node):
        data = pickle.dumps({
            "order": node.order,
            "leaf": node.leaf,
            "keys": node.keys,
            "values": node.values,
            "children": node.children,
            "next": node.next,
            "node_id": node.node_id
        })
        block_id = int(uuid.UUID(node.node_id).int % 1_000_000)
        self.block_manager.write_block(block_id, data[:8192])
        node.dirty = False

    def load_node(self, node_id):
        block_id = int(uuid.UUID(node_id).int % 1_000_000)
        data = self.block_manager.read_block(block_id)
        d = pickle.loads(data)
        node = Node(d["order"], d["leaf"], node_id=d["node_id"])
        node.keys = d["keys"]
        node.values = d["values"]
        node.children = d["children"]
        node.next = d["next"]
        return node

    def insert(self, key, value):
        root = self.root
        if len(root.keys) == self.order - 1:
            # Create new root
            new_root = Node(self.order, leaf=False)
            new_root.children = [root.node_id]
            self._split_child(new_root, 0)
            self.root = new_root
            self.root_node_id = new_root.node_id
            self.save_node(new_root)
        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node, key, value):
        idx = self._find_index(node.keys, key)
        if node.leaf:
            if key in node.keys:
                raise ValueError("Duplicate key")
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
            node.dirty = True
            self.save_node(node)
        else:
            child_id = node.children[idx]
            child = self.load_node(child_id)
            if len(child.keys) == self.order - 1:
                self._split_child(node, idx)
                self.save_node(node)
                # Must reload node and child pointers after split
                node = self.load_node(node.node_id)
                if key > node.keys[idx]:
                    idx += 1
            child = self.load_node(node.children[idx])
            self._insert_non_full(child, key, value)

    def _split_child(self, parent, idx):
        order = self.order
        node = self.load_node(parent.children[idx])

        mid = order // 2
        split_key = node.keys[mid]

        left = Node(order, leaf=node.leaf)
        right = Node(order, leaf=node.leaf)

        left.keys = node.keys[:mid]
        right.keys = node.keys[mid + (0 if node.leaf else 1):]

        if node.leaf:
            left.values = node.values[:mid]
            right.values = node.values[mid:]
            left.next = right.node_id
            right.next = node.next
        else:
            left.children = node.children[:mid + 1]
            right.children = node.children[mid + 1:]

        # Save new nodes before referencing them by ID
        self.save_node(left)
        self.save_node(right)

        parent.keys.insert(idx, split_key)
        parent.children[idx] = left.node_id
        parent.children.insert(idx + 1, right.node_id)
        parent.dirty = True

    def search(self, key):
        node = self.root
        while not node.leaf:
            idx = self._find_index(node.keys, key)
            child_id = node.children[idx]
            node = self.load_node(child_id)
        if key in node.keys:
            i = node.keys.index(key)
            return node.values[i]
        return None

    def scan(self, start_key=None):
        # Find leftmost leaf (optionally start at key)
        node = self.root
        while not node.leaf:
            child_id = node.children[0]
            node = self.load_node(child_id)
        idx = 0
        if start_key is not None:
            idx = self._find_index(node.keys, start_key)
        while node:
            for i in range(idx, len(node.keys)):
                yield node.keys[i], node.values[i]
            if node.next:
                node = self.load_node(node.next)
            else:
                node = None
            idx = 0

    def dirty_nodes(self):
        """
        Traverse the B+Tree and return a list of all nodes with dirty=True.
        """
        dirty = []
        visited = set()
        
        def collect(node):
            if node.node_id in visited:
                return
            visited.add(node.node_id)
            if getattr(node, "dirty", False):
                dirty.append(node)
            if not node.leaf:
                for child_id in node.children:
                    child = self.load_node(child_id)
                    collect(child)
        collect(self.root)
        return dirty

    @classmethod
    def bulk_load(cls, items, order=32, block_manager=None):
        """
        Bulk load the tree from a sorted list of (key, value) pairs.
        - items: list of (key, value) tuples (MUST BE SORTED and unique)
        - order: maximum number of keys per node
        - block_manager: BlockManager instance for disk writes
        Returns: new BplusTree instance
        """
        assert block_manager is not None, "Bulk load requires a BlockManager"

        # 1. Create leaves
        leaf_nodes = []
        node_size = order - 1  # max keys per node
        n = len(items)
        for i in range(0, n, node_size):
            chunk = items[i:i + node_size]
            keys = [k for k, v in chunk]
            values = [v for k, v in chunk]
            node = Node(order, leaf=True)
            node.keys = keys
            node.values = values
            leaf_nodes.append(node)

        # Set up leaf 'next' pointers
        for i in range(len(leaf_nodes) - 1):
            leaf_nodes[i].next = leaf_nodes[i + 1].node_id
        leaf_nodes[-1].next = None

        # 2. Write leaves to disk
        for node in leaf_nodes:
            node.dirty = False
            block_id = int(uuid.UUID(node.node_id).int % 1_000_000)
            data = pickle.dumps({
                "order": node.order,
                "leaf": node.leaf,
                "keys": node.keys,
                "values": node.values,
                "children": node.children,
                "next": node.next,
                "node_id": node.node_id
            })
            block_manager.write_block(block_id, data[:8192])

        # 3. Build internal levels upward
        current_level = leaf_nodes
        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), node_size + 1):
                chunk = current_level[i:i + node_size + 1]
                keys = [node.keys[0] for node in chunk[1:]]  # separator keys
                children = [node.node_id for node in chunk]
                parent = Node(order, leaf=False)
                parent.keys = keys
                parent.children = children

                # Write parent to disk
                parent.dirty = False
                block_id = int(uuid.UUID(parent.node_id).int % 1_000_000)
                data = pickle.dumps({
                    "order": parent.order,
                    "leaf": parent.leaf,
                    "keys": parent.keys,
                    "values": parent.values,
                    "children": parent.children,
                    "next": parent.next,
                    "node_id": parent.node_id
                })
                block_manager.write_block(block_id, data[:8192])
                next_level.append(parent)
            current_level = next_level

        # 4. The only node left is the root
        root = current_level[0]
        tree = cls(order=order, block_manager=block_manager)
        tree.root = root
        tree.root_node_id = root.node_id

        # Write root to disk
        block_id = int(uuid.UUID(root.node_id).int % 1_000_000)
        data = pickle.dumps({
            "order": root.order,
            "leaf": root.leaf,
            "keys": root.keys,
            "values": root.values,
            "children": root.children,
            "next": root.next,
            "node_id": root.node_id
        })
        block_manager.write_block(block_id, data[:8192])

        return tree


    def __repr__(self):
        levels = []
        def visit(node, depth=0):
            if len(levels) <= depth:
                levels.append([])
            levels[depth].append(node.keys)
            if not node.leaf:
                for child_id in node.children:
                    child = self.load_node(child_id)
                    visit(child, depth + 1)
        visit(self.root)
        return "\n".join(f"Level {i}: {l}" for i, l in enumerate(levels))
