from core.datatypes import DataType


class Column:
    def __init__(self, name, dtype: DataType, constraints=None, auto_increment=False):
        self.name = name
        self.dtype = dtype
        self.constraints = constraints or []
        self.auto_increment = auto_increment

    def is_primary(self):
        return any(c.upper() == "PRIMARY KEY" for c in self.constraints)

    def is_unique(self):
        # Primary Key columns are always unique, UNIQUE constraint is also valid
        return self.is_primary() or any(c.upper() == "UNIQUE" for c in self.constraints)

    def is_not_null(self):
        # Primary Key columns are always NOT NULL, or explicit NOT NULL constraint
        return self.is_primary() or any(c.upper() == "NOT NULL" for c in self.constraints)


    def to_dict(self):
        return {
            "name": self.name,
            "dtype": self.dtype,
            "constraints": self.constraints,
            "auto_increment": self.auto_increment

        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data["name"],
            dtype=data["dtype"],
            constraints=data.get("constraints", []),
            auto_increment=data.get("auto_increment", False)
        )

    def __repr__(self):
        return f"Column(name='{self.name}', type={self.dtype}, constraints={self.constraints})"