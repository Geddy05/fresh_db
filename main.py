import time
import threading

from query.execute import execute_query
from query.parser import parse_command
from schema.schema import Schema
from storage.manager import StorageManager
from jobs.queue import Job, JobQueue
from storage.column_store import ColumnStore

colstore = ColumnStore("users", pk="id")
job_queue = JobQueue()
job_queue.start()

COMPACTION_INTERVAL = 3600  # seconds

def schedule_periodic_compaction(job_queue, schema):
    """Schedules periodic compaction jobs for all tables."""
    while True:
        for table_name, table in schema.tables.items():
            # If your Table class knows its PK, use table.pk
            pk = table.pk if hasattr(table, 'pk') else "id"
            ""
            colstore = ColumnStore(table_name, pk=pk)
            job_queue.enqueue(Job(colstore.compact, description=f"Periodic compaction for {table_name}"))
        time.sleep(COMPACTION_INTERVAL)

def main():
    print("FreshDB > Type your SQL-like commands.")
    storage_manager = StorageManager()
    schema = Schema()
    schema.load_schema(storage_manager)  # Discover tables on startup

    job_queue = JobQueue()
    job_queue.start()

    # 3. Schedule periodic compaction for all tables (in a background thread)
    threading.Thread(target=schedule_periodic_compaction, args=(job_queue, schema), daemon=True).start()

    while True:
        try:
            command = input("FreshDB> ").strip()
            if command.lower() in ("exit", "quit"):
                break
            if not command:
                continue

            parsed = parse_command(command)
            execute_query(parsed, schema, storage_manager)  # Pass storage manager too
            # If you want to persist schema structure, implement schema.save_schema(storage_manager)
        except Exception as e:
            print("Error:", e)

if __name__ == '__main__':
    main()

# CREATE TABLE users (id INT, name TEXT);
# INSERT INTO users VALUES (1, 'Alice');
# INSERT INTO users VALUES (2, 'Bob');
# SELECT * FROM users;
# SELECT * FROM users WHERE name = 'Bob';