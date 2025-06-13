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

    # ---- DROP EXISTING TABLE AND DATA ----
    if table_name in schema.tables:
        print(f"Table '{table_name}' exists, dropping old data and indexes.")
        storage_manager.drop_table(table_name)
        # Optionally remove old index files if you store them elsewhere
        import glob
        import os
        for idx_file in glob.glob(f"data/indexes/{table_name}_*.json"):
            os.remove(idx_file)
        schema.tables.pop(table_name)
        schema._save_schema()

    # ---- CREATE FRESH TABLE ----
    table = Table(table_name, storage_manager, columns=IMDB_COLUMNS)
    schema.tables[table_name] = table
    schema._save_schema()

    print(f"Created table '{table_name}'.")

    bulk_mode = True

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
                    table.bulk_insert(batch, bulk_mode=bulk_mode)
                    batch.clear()
            except Exception as e:
                print(f"Error at row {n}: {e}")
                batch.clear()
            n += 1
            if n % BATCH_SIZE == 0:
                end = time.time()
                elapsed = end - start
                print(f"Imported {n} rows. Time taken last 10000 rows: {elapsed:.6f} seconds")
                start = end
        if batch:
            table.bulk_insert(batch,bulk_mode=bulk_mode)
        print(f"Import complete. Total rows: {n}")

    start = time.time()
    table.rebuild_index()
    end = time.time()
    elapsed = end - start
    print(f"Duration rebuild index: {elapsed:.6f} seconds")

    schema._save_schema()
    print("Schema saved.")


if __name__ == "__main__":
    filename= "data/title.basics.tsv"
    import_imdb_titles(filename)
