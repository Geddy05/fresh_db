from core.column import Column
from indexing.bplustree import BplusTree
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
        self.indexes = {}
        for col in columns:
            if col.is_unique():
                self.indexes[col.name] = BplusTree(order=32)


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
        # # Check if column has a name
        for col in self.columns:
            val = row_dict.get(col.name)
            if col.is_not_null() and val is None:
                raise ValueError(f"{col.name} cannot be NULL")

        # Handle auto-increment
        if self.auto_increment_col:
            if self.auto_increment_col.name not in row_dict or row_dict[self.auto_increment_col.name] is None:
                row_dict[self.auto_increment_col.name] = self.next_increment
                self.next_increment += 1

        # Check PK/UNIQUE via B+ Tree
        for col_name, bptree in self.indexes.items():
            key = row_dict[col_name]
            if bptree.search(key) is not None:
                raise ValueError(f"Duplicate value for UNIQUE column '{col_name}'")
        
        row_idx = len(self.rows)
        self.storage.write_row(self.name, row_dict)

        # Update indexes
        for col_name, bptree in self.indexes.items():
            bptree.insert(row_dict[col_name], row_idx)

    def bulk_insert(self, rows: list[dict]):
        """ Insert a list of row dicts in bulk.
        - Fails the whole batch if any duplicate is detected (in the batch or vs. existing).
        - Otherwise, writes all rows, then updates indexes.
        """
        
        # Step 1: Prepare sets for batch PK/UNIQUE check
        for col_name, bptree in self.indexes.items():
            # Check for duplicates in existing index
            batch_keys = [row[col_name] for row in rows]
            existing = set()
            for key in batch_keys:
                if bptree.search(key) is not None:
                    raise ValueError(f"Duplicate value for UNIQUE column '{col_name}' (already exists): {key}")
                if key in existing:
                    raise ValueError(f"Duplicate value for UNIQUE column '{col_name}' (within batch): {key}")
                existing.add(key)

        # Step 2: Validate NOT NULL and auto-increment
        for row_dict in rows:
            for col in self.columns:
                val = row_dict.get(col.name)
                if col.is_not_null() and val is None:
                    raise ValueError(f"{col.name} cannot be NULL")
            # Handle auto-increment
            if self.auto_increment_col:
                if self.auto_increment_col.name not in row_dict or row_dict[self.auto_increment_col.name] is None:
                    row_dict[self.auto_increment_col.name] = self.next_increment
                    self.next_increment += 1

        # Step 3: Write all rows
        self.storage.bulk_write(self.name, rows)

        for row_dict in rows:
            row_idx = len(self.rows)
            for col_name, bptree in self.indexes.items():
                bptree.insert(row_dict[col_name], row_idx)
        self.rows.extend(rows)


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