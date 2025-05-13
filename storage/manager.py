import json
import os

DATA_DIR = "data"

class StorageManager:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)

    def save_table(self, table_name, table_obj):
        path = os.path.join(DATA_DIR, f"{table_name}.tbl.json")
        with open(path, "w") as f:
            json.dump(table_obj.to_dict(), f)

    def load_table(self, table_name, table_class):
        path = os.path.join(DATA_DIR, f"{table_name}.tbl.json")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Table {table_name} does not exist.")
        with open(path, "r") as f:
            data = json.load(f)
        return table_class.from_dict(data)