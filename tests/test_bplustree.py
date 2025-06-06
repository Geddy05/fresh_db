import pytest

from indexing.bplustree import BplusTree

def test_basic_insert_and_search():
    t = BplusTree(order=4)
    t.insert(10, "a")
    t.insert(20, "b")
    t.insert(5, "c")
    assert t.search(10) == "a"
    assert t.search(20) == "b"
    assert t.search(5) == "c"
    assert t.search(99) is None

def test_duplicate_key_raises():
    t = BplusTree(order=4)
    t.insert(1, "foo")
    with pytest.raises(ValueError):
        t.insert(1, "bar")

def test_split_leaf_and_internal():
    t = BplusTree(order=4)
    for i in range(10):  # This should cause root and at least one child split
        t.insert(i, str(i))
    # All keys should be there
    for i in range(10):
        assert t.search(i) == str(i)
    # Tree should have more than one level
    assert not t.root.leaf
    assert len(t.root.children) > 1

def test_scan_sorted_and_start_key():
    t = BplusTree(order=4)
    for i in [10, 5, 3, 20, 15]:
        t.insert(i, str(i))
    result = list(t.scan())
    assert result == sorted(result)  # Should be sorted by key

    # Start from key >= 10
    result2 = list(t.scan(start_key=10))
    assert all(k >= 10 for k, v in result2)

def test_serialize_and_deserialize():
    t = BplusTree(order=4)
    for i in range(5):
        t.insert(i, str(i))
    d = t.to_dict()
    t2 = BplusTree.from_dict(d, order=4)
    for i in range(5):
        assert t2.search(i) == str(i)
    # Structure should match
    assert t2.to_dict() == t.to_dict()

def test_save_and_load(tmp_path):
    t = BplusTree(order=4)
    for i in range(3):
        t.insert(i, str(i))
    f = tmp_path / "tree.json"
    t.save(str(f))
    t2 = BplusTree.load(str(f), order=4)
    for i in range(3):
        assert t2.search(i) == str(i)

def test_empty_tree_search_and_scan():
    t = BplusTree(order=4)
    assert t.search(123) is None
    assert list(t.scan()) == []

def test_repr_smoke():
    t = BplusTree(order=4)
    t.insert(1, "foo")
    r = repr(t)
    assert "Level" in r

def test_large_order():
    t = BplusTree(order=8)
    for i in range(50):
        t.insert(i, str(i))
    for i in range(50):
        assert t.search(i) == str(i)

def test_chained_splits_deep_tree():
    t = BplusTree(order=3)  # Low order for more levels
    for i in range(100):
        t.insert(i, str(i))
    for i in range(100):
        assert t.search(i) == str(i)

def test_repr_multiple_levels():
    t = BplusTree(order=3)
    for i in range(10):
        t.insert(i, str(i))
    r = repr(t)
    assert "Level 0:" in r
    assert "Level 1:" in r

def test__find_leaf_all_paths():
    t = BplusTree(order=3)
    for k in [1, 2, 3, 4, 5, 6, 7, 8]:
        t.insert(k, str(k))
    # Now the tree should have multiple levels and leaves
    # Test for smallest, middle, and largest
    for key in [1, 4, 8]:
        leaf = t._find_leaf(key)
        assert leaf.leaf
        assert key in leaf.keys

