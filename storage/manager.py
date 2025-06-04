import json
import os
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
    
    def get_row_store(self, table_name) -> RowStore:
        if table_name not in self.row_stores:
            self.row_stores[table_name] = RowStore(table_name, self.base_path)
        return self.row_stores[table_name]
    
    def get_column_store(self, table_name) -> ColumnStore:
        if table_name not in self.column_stores:
            self.column_stores[table_name] = ColumnStore(table_name, self.base_path)
        return self.column_stores[table_name]
    
    def write_row(self, table_name, row: dict):
        """Insert into OLTP row store and log to WAL."""
        self.get_row_store(table_name).insert_row(row)

    def bulk_write(self, table_name, rows:  list[dict]):
        """Insert into OLTP row store and log to WAL."""
        self.get_row_store(table_name).bulk_insert_rows(rows)

    def flush_table(self, table_name):
        """Flush rows to OLAP (segment) and clear WAL."""
        row_store = self.get_row_store(table_name)
        col_store = self.get_column_store(table_name)

        rows = row_store.get_rows()
        if not rows:
            return

        col_store.flush(rows)
        row_store.clear()  # Clears in-memory rows and WAL

    def load_all_tables(self):
        """Replay WALs at startup to restore in-memory state."""
        for file in os.listdir(os.path.join(self.base_path, "wal")):
            if file.endswith(".wal"):
                table_name = file.replace(".wal", "")
                store = self.get_row_store(table_name)
                # store.replay_wal()


    # def save_table(self, table_name, table_obj):
    #     path = os.path.join(DATA_DIR, f"{table_name}.tbl.json")
    #     with open(path, "w") as f:
    #         json.dump(table_obj.to_dict(), f)

    # def load_table(self, table_name):
    #     path = os.path.join(DATA_DIR, f"{table_name}.tbl.json")
    #     if not os.path.exists(path):
    #         raise FileNotFoundError(f"Table {table_name} does not exist.")
    #     with open(path, "r") as f:
    #         data = json.load(f)
    #     return Table.from_dict(data)