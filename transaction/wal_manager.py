import os
import json

class WALManager:
    def __init__(self, table_name, base_path="data/wal"):
        self.table_name = table_name
        self.wal_path = os.path.join(base_path, f"{table_name}.wal")
        os.makedirs(base_path, exist_ok=True)

    def log_insert(self, row):
        entry = {"op": "INSERT", "row": row}
        with open(self.wal_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def log_delete(self, key):
        entry = {"op": "DELETE", "key": key}
        with open(self.wal_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def replay(self, apply_fn,delete_fn):
        """Replay WAL and call apply_fn(row) for each INSERT row."""
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, "r") as f:
            for line in f:
                entry = json.loads(line.strip())
                if entry["op"] == "INSERT":
                    apply_fn(entry["row"])
                if entry["op"] == "INSERT":
                    delete_fn(entry["row"])
                

    def clear(self):
        """Truncate WAL after successful block flush."""
        open(self.wal_path, "w").close()
