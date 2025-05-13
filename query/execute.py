from core.column import Column
from core.table import Table
from query.querytype import QueryType, QueryTypes


def execute_query(parsed:QueryType, schema):
    cmd_type = parsed.type

    if cmd_type == QueryTypes.CREATE:
        columns = [Column(name=col[0], dtype=col[1]) for col in parsed.columns]
        table = Table(name=parsed.table, columns=columns)
        schema.tables[parsed.table] = table
        print(f"Table '{parsed.table}' created.")