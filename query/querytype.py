from enum import Enum

class QueryTypes(Enum):
    EMPTY = "EMPTY"
    CREATE = "CREATE"
    INSERT = "INSERT"
    SELECT = "SELECT"
    UNKNOWN = "UNKNOWN"


class QueryType:
    def __init__(self):
        self.type = QueryTypes.EMPTY

    def __init__(self,type:QueryTypes, table, values=[], columns=[],conditions=[], database=None):
        self.type = type
        self.table = table
        self.database = database
        self.values = values
        self.columns = columns
        self.conditions = conditions

        