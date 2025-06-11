import pytest
from unittest.mock import MagicMock, patch

from core.table import Table

# DummyColumn definition for flexible mocking and serialization
class DummyColumn:
    def __init__(self, name, constraints=None, auto_increment=False, dtype="int"):
        self.name = name
        self.constraints = constraints or []
        self.auto_increment = auto_increment
        self.dtype = dtype
    def is_not_null(self):
        return "NOT NULL" in self.constraints
    def is_unique(self):
        return "PK" in self.constraints or "UNIQUE" in self.constraints
    def to_dict(self):
        return {
            "name": self.name,
            "constraints": self.constraints,
            "dtype": self.dtype,
            "auto_increment": self.auto_increment
        }
    @staticmethod
    def from_dict(d):
        return DummyColumn(
            d['name'],
            d.get('constraints', []),
            d.get('auto_increment', False),
            d.get("dtype", "int")
        )

@pytest.fixture(autouse=True)
def patch_bplustree():
    with patch('core.table.BplusTree') as BP:
        def make_bptree(*args, **kwargs):
            mock_bptree = MagicMock()
            mock_bptree.search.return_value = None
            mock_bptree.insert = MagicMock()
            return mock_bptree
        BP.side_effect = make_bptree
        yield

@pytest.fixture
def mock_storage():
    storage = MagicMock()
    # Default for get_row_store/get_column_store
    storage.get_row_store.return_value.get_rows.return_value = []
    storage.get_column_store.return_value.load_segments.return_value = []
    storage.write_row = MagicMock()
    storage.bulk_write = MagicMock()
    storage.flush_table = MagicMock()
    return storage

@pytest.fixture
def unique_col():
    return DummyColumn('id', constraints=['PK'], auto_increment=True)

@pytest.fixture
def table(mock_storage, unique_col):
    t = Table('test_table', mock_storage, columns=[unique_col, DummyColumn('val')])
    return t

def test_init_sets_pk_and_auto_increment(mock_storage):
    col = DummyColumn('id', constraints=['PK'], auto_increment=True)
    t = Table('t', mock_storage, columns=[col])
    assert t.pk_column is col
    assert t.auto_increment_col is col

def test_load_next_increment_with_rows(mock_storage):
    col = DummyColumn('id', constraints=['PK'], auto_increment=True)
    # Simulate existing rows with id values
    mock_storage.get_row_store.return_value.get_rows.return_value = [{'id': 1}, {'id': 5}, {'id': 3}]
    t = Table('t', mock_storage, columns=[col])
    assert t.next_increment == 6

def test_add_column(table):
    col = DummyColumn('extra')
    table.add_column(col)
    assert col in table.columns

def test_insert_enforces_not_null(table):
    col = DummyColumn('notnull', constraints=['NOT NULL'])
    table.columns.append(col)
    with pytest.raises(ValueError):
        table.insert({'id': 1, 'val': 1})  # missing 'notnull'

def test_insert_auto_increment(table):
    table.next_increment = 101
    row = {'val': 2}
    table.insert(row)
    assert row['id'] == 101

def test_insert_duplicate_pk_raises(table):
    for bpt in table.indexes.values():
        bpt.search.return_value = 0  # Simulate PK already exists
    with pytest.raises(ValueError):
        table.insert({'id': 1, 'val': 2})

def test_insert_success(table):
    row = {'id': 10, 'val': 77}
    table.insert(row)
    for bpt in table.indexes.values():
        bpt.insert.assert_called_with(10, 0)
    table.storage.write_row.assert_called_with('test_table', row)

def test_bulk_insert_success(table):
    rows = [{'id': 1, 'val': 2}, {'id': 2, 'val': 3}]
    table.bulk_insert(rows)
    table.storage.bulk_write.assert_called_with('test_table', rows)
    for row in rows:
        for bpt in table.indexes.values():
            bpt.insert.assert_any_call(row['id'], 0)
    assert all(row in table.rows for row in rows)

def test_bulk_insert_duplicate_in_batch(table):
    rows = [{'id': 2, 'val': 3}, {'id': 2, 'val': 7}]
    with pytest.raises(ValueError):
        table.bulk_insert(rows)

def test_bulk_insert_duplicate_vs_existing(table):
    for bpt in table.indexes.values():
        bpt.search.return_value = 123
    rows = [{'id': 5, 'val': 2}]
    with pytest.raises(ValueError):
        table.bulk_insert(rows)

def test_bulk_insert_not_null(table):
    col = DummyColumn('x', constraints=['NOT NULL'])
    table.columns.append(col)
    with pytest.raises(ValueError):
        table.bulk_insert([{'id': 1, 'val': 2}])

def test_flush_calls_storage(table):
    table.flush()
    table.storage.flush_table.assert_called_with('test_table')

def test_select_all_merges_oltp_olap(table):
    table.storage.get_row_store.return_value.get_rows.return_value = [{'id': 1}]
    table.storage.get_column_store.return_value.load_segments.return_value = [{'id': 2}]
    result = table.select_all()
    assert {'id': 1} in result and {'id': 2} in result

def test_to_dict_and_from_dict(mock_storage):
    cols = [DummyColumn('id', ['PK']), DummyColumn('val')]
    t = Table('t', mock_storage, columns=cols)
    d = t.to_dict()
    from core.table import Table as RealTable
    t2 = RealTable.from_dict(d, mock_storage)
    assert t2.name == 't'
    assert t2.rows == []
    assert all(isinstance(c, DummyColumn) or hasattr(c, "name") for c in t2.columns)

def test_bulk_insert_assigns_auto_increment(table):
    table.next_increment = 123
    row = {'id': None, 'val': 1}
    table.bulk_insert([row])
    assert row['id'] == 123


def test_bulk_insert_assigns_auto_increment_multiple(table):
    table.next_increment = 123
    row1 = {'id': None, 'val': 1}
    row2 = {'id': None, 'val': 2}
    table.bulk_insert([row1])
    table.bulk_insert([row2])
    assert row1['id'] == 123
    assert row2['id'] == 124

def test_repr(table):
    s = repr(table)
    assert f"<Table {table.name}" in s
