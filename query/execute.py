from core.column import Column
from core.table import Table
from query.querytype import QueryType, QueryTypes


def execute_query(parsed:QueryType, schema, storage_manager):
    cmd_type = parsed.type

    if cmd_type == QueryTypes.CREATE:
        columns = [Column(name=col[0], dtype=col[1]) for col in parsed.columns]
        table = schema.create_table(parsed.table, columns=columns, storage_manager=storage_manager)
        print(f"Table '{parsed.table}' created.")

    elif cmd_type == QueryTypes.INSERT:
        table = schema.tables.get(parsed.table)
        if not table:
            print("Table does not exist.")
            return
        row = dict(zip([col.name for col in table.columns], parsed.values))
        table.insert(row)
        print(f"Inserted into '{parsed.table}': {row}")

    elif cmd_type == QueryTypes.SELECT:
        table = schema.tables.get(parsed.table)
        if not table:
            print("Table does not exist.")
            return
        if parsed.conditions:
            col, val = parsed.conditions
            results = [row for row in table.select_all() if str(row.get(col)) == val]
        else:
            results = table.select_all()
        for r in results:
            print(r)

    elif cmd_type == "UNKNOWN":
        print("Unknown command.")

    elif cmd_type == "EMPTY":
        pass

    # schema.save()