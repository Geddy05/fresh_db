import os
import glob
from storage.row_store import RowStore
from storage.column_store import ColumnStore

class StorageManager:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        self.row_stores = {}
        self.column_stores = {}

        self._ensure_dirs()
    
    def _ensure_dirs(self):
        os.makedirs(os.path.join(self.base_path, "wal"), exist_ok=True)
        os.makedirs(os.path.join(self.base_path, "segments"), exist_ok=True)
        os.makedirs(os.path.join(self.base_path, "indexes"), exist_ok=True)

    def get_row_store(self, table_name) -> RowStore:
        if table_name not in self.row_stores:
            self.row_stores[table_name] = RowStore(table_name, self.base_path)
        return self.row_stores[table_name]
    
    def get_column_store(self, table_name) -> ColumnStore:
        if table_name not in self.column_stores:
            self.column_stores[table_name] = ColumnStore(table_name, self.base_path)
        return self.column_stores[table_name]
    
    def write_row(self, table_name, row: dict):
        self.get_row_store(table_name).insert_row(row)

    def bulk_write(self, table_name, rows:  list[dict]):
        self.get_row_store(table_name).bulk_insert_rows(rows)

    def drop_table(self, table_name):
        # Drop RowStore files and remove from manager
        if table_name in self.row_stores:
            self.row_stores[table_name].drop()
            del self.row_stores[table_name]
        if table_name in self.column_stores:
            self.column_stores[table_name].drop()
            del self.column_stores[table_name]
        # Remove index files (block and meta)
        index_pattern = os.path.join(self.base_path, "indexes", f"{table_name}_*")
        for file_path in glob.glob(index_pattern):
            try:
                os.remove(file_path)
            except Exception:
                pass

    def flush_table(self, table_name):
        row_store = self.get_row_store(table_name)
        col_store = self.get_column_store(table_name)
        rows = row_store.get_rows()
        if not rows:
            return
        col_store.flush(rows)
        row_store.clear()

    def load_all_tables(self):
        for file in os.listdir(os.path.join(self.base_path, "wal")):
            if file.endswith(".wal"):
                table_name = file.replace(".wal", "")
                store = self.get_row_store(table_name)
