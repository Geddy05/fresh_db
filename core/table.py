from core.column import Column
from storage.manager import StorageManager


class Table:
    def __init__(self, name: str, storage: StorageManager,columns: list[Column] = []):
        self.name = name
        self.storage = storage
        self.columns = columns
        self.rows = []  # We'll keep rows in memory for now

    def add_column(self, column: Column):
        self.columns.append(column)

    def insert(self, row_dict: dict):
        # Basic validation
        self.storage.write_row(self.name, row_dict)

    def flush(self):
        self.storage.flush_table(self.name)

    def select_all(self):
        # Merge results from OLTP (row store) and OLAP (column store)
        oltp_rows = self.storage.get_row_store(self.name).get_rows() # type: ignore
        olap_rows = self.storage.get_column_store(self.name).load_segments()
        return oltp_rows + olap_rows

    def to_dict(self):
        return {
            "name": self.name,
            "columns": [col.to_dict() for col in self.columns],
            "rows": self.rows
        }

    @classmethod
    def from_dict(cls, data: dict, storage_manager: StorageManager):
        table = cls(data['name'], storage_manager)

        table.columns = [Column.from_dict(col) for col in data.get('columns', [])]
        table.rows = data["rows"]
        return table

    def __repr__(self):
        return f"<Table {self.name} Columns={self.columns}>"