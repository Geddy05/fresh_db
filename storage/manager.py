import json
import os

from core.table import Table

DATA_DIR = "data"

class StorageManager:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)

    def save_table(self, table_name, table_obj):
        path = os.path.join(DATA_DIR, f"{table_name}.tbl.json")
        with open(path, "w") as f:
            json.dump(table_obj.to_dict(), f)

    def load_table(self, table_name):
        path = os.path.join(DATA_DIR, f"{table_name}.tbl.json")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Table {table_name} does not exist.")
        with open(path, "r") as f:
            data = json.load(f)
        return Table.from_dict(data)