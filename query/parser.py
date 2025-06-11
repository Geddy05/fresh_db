import shlex

from query.querytype import QueryType, QueryTypes

def parse_command(command: str):
    command= command.strip(";")
    tokens = shlex.split(command)
    if not tokens:
        return QueryType(type=QueryTypes.UNKNOWN, table=None)
    
    cmd =  tokens[0].upper()

    try:
        if cmd == QueryTypes.CREATE.value:
            item = tokens[1].upper()
            if item == "TABLE":
                name = tokens[2]
                columns = command[command.find("(")+1:command.find(")")].split(",")
                columns = [col.strip().split() for col in columns]
                return QueryType(type=QueryTypes.CREATE,table=name, columns=columns)
            else : 
                name = tokens[2]
                columns = command[command.find("(")+1:command.find(")")].split(",")
                columns = [col.strip().split() for col in columns]
                return QueryType(type=QueryTypes.CREATE,table=name, columns=columns)
        
        elif cmd == QueryTypes.INSERT.value:
            table_name = tokens[2]
            values = command[command.find("(")+1:command.find(")")].split(",")
            values = [eval(v.strip()) for v in values]  # careful in prod!
            return QueryType(type=QueryTypes.INSERT, table=table_name, values=values)
        
        elif cmd == QueryTypes.SELECT.value:
            table_name = tokens[tokens.index("FROM") + 1]
            condition = None
            if "WHERE" in tokens:
                col = tokens[tokens.index("WHERE") + 1]
                val = tokens[-1].strip(";")
                condition = (col, val.strip("'\""))
            return QueryType(type=QueryTypes.SELECT, table=table_name, conditions=condition)

        
        else:
            print("Couldn't parse query")
            return QueryType(type=QueryTypes.UNKNOWN, table=None)
        
    except Exception as e:
        print("Parse error:", e)
        return QueryType(type=QueryTypes.UNKNOWN, table=None)
