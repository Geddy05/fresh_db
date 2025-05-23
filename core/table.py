from core.column import Column
from storage.manager import StorageManager


class Table:
    def __init__(self, name: str, storage: StorageManager,columns: list[Column] = []):
        self.name = name
        self.storage = storage
        self.columns = columns
        self.pk_column = next((col for col in self.columns if "PK" in col.constraints), None)
        self.rows = []  # We'll keep rows in memory for now
        self.auto_increment_col = next((col for col in self.columns if col.auto_increment), None)
        self._load_next_increment()


    def _load_next_increment(self):
        # Scan all rows to determine next auto_increment value
        self.next_increment = 1
        if self.auto_increment_col:
            rows = self.storage.get_row_store(self.name).get_rows()
            if rows:
                self.next_increment = max(row[self.auto_increment_col.name] for row in rows) + 1


    def add_column(self, column: Column):
        self.columns.append(column)

    def insert(self, row_dict: dict):
        # Handle auto-increment
        if self.auto_increment_col:
            if self.auto_increment_col.name not in row_dict or row_dict[self.auto_increment_col.name] is None:
                row_dict[self.auto_increment_col.name] = self.next_increment
                self.next_increment += 1

        # Check PK constraint (must be unique)
        if self.pk_column:
            pk_value = row_dict[self.pk_column.name]
            all_rows = self.storage.get_row_store(self.name).get_rows()
            if any(r[self.pk_column.name] == pk_value for r in all_rows):
                raise ValueError(f"Duplicate PK value: {pk_value}")

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