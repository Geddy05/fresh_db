import pytest
from unittest.mock import patch
import storage.row_store

# ----------------- DUMMY CLASSES -----------------

class DummyBlockManager:
    def __init__(self, *a, **kw):
        self._counter = 0
        self._written = set()
        self._rows_per_block = {}
    def allocate_block(self):
        self._counter += 1
        return self._counter - 1
    def num_blocks(self):
        return self._counter
    def write_block(self, block_num, data):
        self._written.add(block_num)
        # Not required, but let's simulate a write for get_rows
        self._rows_per_block[block_num] = data
    def read_block(self, block_num):
        return b''

class DummyWALManager:
    def __init__(self, *a, **kw):
        self.did_replay = False
        self.inserts = []
        self.deletes = []
        self.did_clear = False
        self.insert_many_called = False
    def replay(self, apply_row, apply_delete):
        self.did_replay = True
    def log_insert(self, row):
        self.inserts.append(row)
    def log_delete(self, key):
        self.deletes.append(key)
    def clear(self):
        self.did_clear = True
    def log_insert_many(self, rows):
        self.insert_many_called = True
        self.inserts.extend(rows)

# ----------------- PATCHED TESTS -----------------

@pytest.fixture(autouse=True)
def patch_row_store_deps(monkeypatch):
    # Always patch encoding to bytes, decoding to empty list
    monkeypatch.setattr('storage.row_packer.encode_rows_block', lambda rows: b'enc')
    monkeypatch.setattr('storage.row_packer.decode_rows_block', lambda b: [])
    # No yield, just setup for each test

def test_insert_row():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        row_store = RowStore('test_table', pk='id', base_path='/tmp/')
        row = {"id": 1, "name": "foo"}
        row_store.insert_row(row)
        # Check that WAL received the insert
        assert row_store.wal_manager.inserts == [row]
        # The row must be present in block_rows
        all_rows = [r for rows in row_store.block_rows.values() for r in rows]
        assert {"id": 1, "name": "foo"} in all_rows
        # At least one block written
        assert len(row_store.bm._written) == 1

def test_bulk_insert_rows():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        row_store = RowStore('test_table', pk='id', base_path='/tmp/')
        rows = [{"id": i, "name": f"row{i}"} for i in range(60)]
        row_store.bulk_insert_rows(rows)
        # Should write at least two blocks for 60 rows (since limit is 50/block)
        assert len(row_store.bm._written) >= 2
        # Check all rows are in block_rows
        all_rows = [r for rows in row_store.block_rows.values() for r in rows]
        for row in rows:
            assert row in all_rows
        # WAL should have received all inserts (either via many or singles)
        assert row_store.wal_manager.insert_many_called or len(row_store.wal_manager.inserts) == 60

def test_recover_from_wal_calls_replay(monkeypatch):
    called = {}
    class WAL(DummyWALManager):
        def replay(self, apply_row, apply_delete):
            called['replay'] = True
            apply_row({'id': 42})
            apply_delete(77)
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', WAL):
        from storage.row_store import RowStore
        RowStore('test_table', pk='id', base_path='/tmp/')
        assert 'replay' in called

def test_bulk_insert_rows_uses_existing_block(monkeypatch):
    class ExistingBlockBM(DummyBlockManager):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self._counter = 1  # Pretend 1 block exists
            self.block_rows = {0: [{'id': 100}]}
        def num_blocks(self):
            return 1
    with patch('storage.row_store.BlockManager', ExistingBlockBM), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        store = RowStore('test_table', pk='id', base_path='/tmp/')
        # Pre-populate last block
        store.block_rows[0] = [{'id': 100}]
        store.bulk_insert_rows([{'id': 101}])
        all_ids = [r['id'] for rows in store.block_rows.values() for r in rows]
        assert 100 in all_ids and 101 in all_ids

def test_insert_without_wal_into_existing_block():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        store = RowStore('test_table', pk='id', base_path='/tmp/')
        # Simulate a block with <50 rows (so it reuses)
        store.block_rows[0] = [{'id': i} for i in range(10)]
        # Monkeypatch num_blocks to return 1
        store.bm._counter = 1
        # Should append to block 0, not create a new block
        store._insert_without_wal({'id': 11})
        assert {'id': 11} in store.block_rows[0]

def test_delete_without_wal_returns_false_when_not_found():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        store = RowStore('test_table', pk='id', base_path='/tmp/')
        store.block_rows = {0: [{'id': 1}, {'id': 2}]}
        assert store._delete_without_wal(999) is False

def test_delete_row():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        row_store = RowStore('test_table', pk='id', base_path='/tmp/')
        # Pre-populate
        row_store.block_rows[0] = [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]
        row_store.delete_row(1)
        # Check that WAL got the delete
        assert row_store.wal_manager.deletes == [1]
        # Only id 2 remains
        assert all(r['id'] != 1 for r in row_store.block_rows[0])
        # Block written
        assert 0 in row_store.bm._written

def test_clear():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        row_store = RowStore('test_table', pk='id', base_path='/tmp/')
        row_store.block_rows = {0: [{"id": 1}]}
        row_store.clear()
        assert row_store.block_rows == {}
        assert row_store.wal_manager.did_clear

def test_get_rows():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        row_store = RowStore('test_table', pk='id', base_path='/tmp/')
        row_store.block_rows = {0: [{"id": 1}, {"id": 2}], 1: [{"id": 3}]}
        all_rows = row_store.get_rows()
        assert len(all_rows) == 3
        ids = {r['id'] for r in all_rows}
        assert ids == {1, 2, 3}

def test_insert_and_delete_without_wal():
    with patch('storage.row_store.BlockManager', DummyBlockManager), \
         patch('storage.row_store.WALManager', DummyWALManager):
        from storage.row_store import RowStore
        row_store = RowStore('test_table', pk='id', base_path='/tmp/')
        row = {"id": 42}
        row_store._insert_without_wal(row)
        assert 42 in [r['id'] for r in row_store.block_rows[0]]
        row_store._delete_without_wal(42)
        assert 42 not in [r['id'] for r in row_store.block_rows[0]]
