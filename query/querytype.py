from enum import Enum

class QueryTypes(Enum):
    EMPTY = "EMPTY"
    CREATE = "CREATE"
    INSERT = "INSERT"
    SELECT = "SELECT"
    DELETE = "DELETE"
    DROP = "DROP"
    UNKNOWN = "UNKNOWN"


class QueryType:

    def __init__(self,type:QueryTypes, table, values=[], columns=[],conditions=[], database=None):
        self.type = type
        self.table = table
        self.database = database
        self.values = values
        self.columns = columns
        self.conditions = conditions

    def is_valid(self):
        return self.type is not None and self.type != QueryTypes.UNKNOWN

    def __repr__(self):
        return (
            f"<QueryType type={self.type} table={self.table} "
            f"columns={self.columns} values={self.values} conditions={self.conditions}>"
        )