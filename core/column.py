from core.datatypes import DataType


class Column:
    def __init__(self, name, dtype: DataType, constraints=None, auto_increment=False):
        self.name = name
        self.dtype = dtype
        self.constraints = constraints or []
        self.auto_increment = auto_increment


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