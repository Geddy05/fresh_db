from core.datatypes import DataType


class Column:
    def __init__(self, name, dtype: DataType, constraints=None):
        self.name = name
        self.dtype = dtype
        self.constraints = constraints or []

    def to_dict(self):
        return {
            "name": self.name,
            "dtype": self.dtype,
            "constraints": self.constraints
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data["name"],
            dtype=data["dtype"],
            constraints=data.get("constraints", [])
        )

    def __repr__(self):
        return f"Column(name='{self.name}', type={self.dtype}, constraints={self.constraints})"