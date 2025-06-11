import csv
import time
from core.column import Column
from core.table import Table
from schema.schema import Schema           # <-- Add this import!
from storage.manager import StorageManager

IMDB_COLUMNS = [
    Column("tconst", "TEXT", constraints=["PRIMARY KEY"]),  # Primary Key
    Column("titleType", "TEXT"),
    Column("primaryTitle", "TEXT"),
    Column("originalTitle", "TEXT"),
    Column("isAdult", "INT"),
    Column("startYear", "INT"),
    Column("endYear", "INT"),
    Column("runtimeMinutes", "INT"),
    Column("genres", "TEXT")
]

def imdb_value(val):
    if val == '\\N':
        return None
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val

def import_imdb_titles(filename, table_name="imdb_titles"):
    storage_manager = StorageManager()
    schema = Schema()   # <-- Create a schema instance
    table = Table(table_name, storage_manager, columns=IMDB_COLUMNS)

    # Register table in schema
    schema.tables[table_name] = table
    schema._save_schema()  # Optionally save before, too

    print(f"Created table '{table_name}'.")

    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        n = 0
        start = time.time()
        BATCH_SIZE = 10000
        batch = []
        for row in reader:
            db_row = {
                "tconst": row["tconst"],
                "titleType": row["titleType"],
                "primaryTitle": row["primaryTitle"],
                "originalTitle": row["originalTitle"],
                "isAdult": imdb_value(row["isAdult"]),
                "startYear": imdb_value(row["startYear"]),
                "endYear": imdb_value(row["endYear"]),
                "runtimeMinutes": imdb_value(row["runtimeMinutes"]),
                "genres": row["genres"]
            }
            batch.append(db_row)

            try:
                if len(batch) == BATCH_SIZE:
                    table.bulk_insert(batch)
                    batch.clear()
            except Exception as e:
                print(f"Error at row {n}: {e}")
            n += 1
            if n % BATCH_SIZE == 0:
                end = time.time()
                elapsed = end - start
                print(f"Imported {n} rows. Time taken last 10000 rows: {elapsed:.6f} seconds")
                start = end
        if batch:
            table.bulk_insert(batch)
        print(f"Import complete. Total rows: {n}")

    # Save schema at the end (table definition is persisted)
    schema._save_schema()
    print("Schema saved.")

if __name__ == "__main__":
    filename= "data/title.basics.tsv"
    import_imdb_titles(filename)
