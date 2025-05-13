import shlex

from query.querytype import QueryType, QueryTypes

def parse_command(command: str) -> dict:
    tokens = shlex.split(command)
    if not tokens:
        return QueryType()
    
    cmd =  tokens[0].upper()

    if cmd == QueryTypes.CREATE.value:
        item = tokens[1].upper
        name = tokens[2]
        columns = command[command.find("(")+1:command.find(")")].split(",")
        columns = [col.strip().split() for col in columns]
        return QueryType(type=QueryTypes.CREATE,table=name, columns=columns)
