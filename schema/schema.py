import os
from core.table import Table


class Schema:
    def __init__(self):
        self.tables = {}

    def create_table(self, table_name, columns: list):
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")
        table = Table(table_name)
        for col in columns:
            table.add_column(col)
        self.tables[table_name] = table

    def get_table(self, table_name):
        return self.tables.get(table_name)
    
    def save(self, storage_manager):
        for name, table in self.tables.items():
            storage_manager.save_table(name, table)

    def load(self, table_class, storage_manager):
        for name in os.listdir("data"):
            if name.endswith(".tbl.json"):
                tname = name.replace(".tbl.json", "")
                self.tables[tname] = storage_manager.load_table(tname, table_class)

    def __repr__(self):
        return f"Schema({list(self.tables.keys())})"