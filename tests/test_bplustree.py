from indexing.bplustree import BplusTree

def test_insert_and_search():
    tree = BplusTree(order=4)
    tree.insert(10, "a")
    tree.insert(20, "b")
    assert tree.search(10) == "a"
    assert tree.search(20) == "b"
    assert tree.search(99) is None
