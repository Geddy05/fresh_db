from core.column import Column
from core.constraints import Constraint


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []
        self.rows = []  # We'll keep rows in memory for now

    def __init__(self, name:str, columns: list[Column] ):
        self.name = name
        self.columns = columns
        self.rows = []

    def add_column(self, column: Column):
        self.columns.append(column)

    def insert(self, row_dict: dict):
        # Basic validation
        row = []
        for col in self.columns:
            value = row_dict.get(col.name)
            if Constraint.NOT_NULL in col.constraints and value is None:
                raise ValueError(f"Column '{col.name}' cannot be NULL")
            row.append(value)
        self.rows.append(row)

    def to_dict(self):
        return {
            "name": self.name,
            "columns": [col.to_dict() for col in self.columns],
            "rows": self.rows
        }

    @classmethod
    def from_dict(cls, data):
        columns = [Column.from_dict(col_data) for col_data in data["columns"]]
        table = cls(data["name"], columns)
        table.rows = data["rows"]
        return table

    def __repr__(self):
        return f"<Table {self.name} Columns={self.columns}>"