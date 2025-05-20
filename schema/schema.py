import json
import os
from core.column import Column
from core.table import Table

class Schema:
    def __init__(self, schema_path="data/schema.json"):
        self.schema_path = schema_path
        self.tables = {}

    def create_table(self, table_name, columns, storage_manager):
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")
        table = Table(table_name, storage_manager)
        for col in columns:
            table.add_column(col)
        self.tables[table_name] = table
        self._save_schema()

    def drop_table(self, table_name):
        # Not implemented Yet
        self._save_schema()

    def _save_schema(self):
        meta = {name: {"columns": [col.to_dict() for col in tbl.columns]} for name, tbl in self.tables.items()}
        with open(self.schema_path, "w") as f:
            json.dump(meta, f)

    def load_schema(self, storage_manager):
        if os.path.exists(self.schema_path):
            with open(self.schema_path, "r") as f:
                meta = json.load(f)
            for name, info in meta.items():
                # Restore Table objects with columns...
                self.tables[name] = Table(name, storage_manager, columns=[Column.from_dict(c) for c in info["columns"]])

    def get_table(self, table_name):
        return self.tables.get(table_name)

    def __repr__(self):
        return f"Schema({list(self.tables.keys())})"
