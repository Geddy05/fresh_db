from core.column import Column
from core.table import Table
from query.execute import execute_query
from query.parser import parse_command
from schema.schema import Schema
from storage.manager import StorageManager

import shlex

if __name__ == '__main__':
    schema = Schema()
    storageManager = StorageManager()
    schema.load(storage_manager=storageManager)
    command = "CREATE TABLE google"
    tokens = parse_command(command)
    execute_query(tokens,schema=schema)
    schema.save(storageManager)

    print(tokens)

    # storageManager = StorageManager()

    # id_col = Column(name="id", dtype="INT", constraints=["NOT NULL"])
    # name_col = Column(name="name", dtype="TEXT", constraints=[])

    # users_table = Table(name="users", columns=[id_col, name_col])

    # users_table.insert({"id": 1, "name": "Alice"})
    # users_table.insert({"id": 2, "name": "Bob"})
    # users_table.insert({"id": 3, "name": "Charlie"})    

    # schema = Schema()
    # schema.tables["users"] = users_table

    # schema.save(storage_manager=storageManager)

    # for row in users_table.rows:
    #     print(row)