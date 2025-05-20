import glob
import os
import json
import zstandard as zstd

class ColumnStore:
    def __init__(self, table_name, pk="id", segment_path='data/segments/'):
        self.table_name = table_name
        self.pk = pk
        self.segment_path = os.path.join(segment_path, table_name)
        os.makedirs(self.segment_path, exist_ok=True)
        self.deletes_path = os.path.join(self.segment_path, "deletes.json")
        self.deleted_keys = set()
        self._load_delete_tombstones()

    def _load_delete_tombstones(self):
        if os.path.exists(self.deletes_path):
            with open(self.deletes_path, "r") as f:
                self.deleted_keys = set(json.load(f))

    def flush(self, rows, segment_id=None):
        if not rows:
            return
        cols = {}
        for key in rows[0]:
            cols[key] = [row[key] for row in rows]

        segment_id = segment_id or f"seg_{len(os.listdir(self.segment_path))}.json.zst"
        path = os.path.join(self.segment_path, segment_id)

        compressor = zstd.ZstdCompressor()
        with open(path, 'wb') as f:
            f.write(compressor.compress(json.dumps(cols).encode('utf-8')))

    def log_delete(self, key_value):
        self.deleted_keys.add(key_value)
        with open(self.deletes_path, "w") as f:
            json.dump(list(self.deleted_keys), f)

    def compact(self):
        print(f"Compacting table {self.table_name}...")

        # 1. Load all rows from all segments (as in load_segments)
        decompressor = zstd.ZstdDecompressor()
        all_rows = []
        for fname in glob.glob(os.path.join(self.segment_path, "*.json.zst")):
            with open(fname, 'rb') as f:
                raw = decompressor.decompress(f.read())
                col_data = json.loads(raw.decode('utf-8'))
                rows = [dict(zip(col_data, t)) for t in zip(*col_data.values())]
                all_rows.extend(rows)

        # 2. Filter out deleted rows
        live_rows = [row for row in all_rows if row[self.pk] not in self.deleted_keys]
        print(f"Live rows after filtering tombstones: {len(live_rows)}")

        # 3. Delete all existing segment files
        for fname in glob.glob(os.path.join(self.segment_path, "*.json.zst")):
            os.remove(fname)

        # 4. (Optionally) split live_rows into multiple new segments if too many
        chunk_size = 1000  # You can tune this value
        compressor = zstd.ZstdCompressor()
        for i in range(0, len(live_rows), chunk_size):
            chunk = live_rows[i:i+chunk_size]
            if not chunk:
                continue
            cols = {key: [row[key] for row in chunk] for key in chunk[0]}
            seg_path = os.path.join(self.segment_path, f"seg_{i // chunk_size}.json.zst")
            with open(seg_path, 'wb') as f:
                f.write(compressor.compress(json.dumps(cols).encode('utf-8')))

        # 5. Delete tombstone file
        if os.path.exists(self.deletes_path):
            os.remove(self.deletes_path)
        self.deleted_keys.clear()

        print(f"Compaction complete for table {self.table_name}.")

    def load_segments(self):
        import glob
        decompressor = zstd.ZstdDecompressor()
        all_data = []
        for fname in glob.glob(os.path.join(self.segment_path, "*.json.zst")):
            with open(fname, 'rb') as f:
                raw = decompressor.decompress(f.read())
                col_data = json.loads(raw.decode('utf-8'))
                rows = [dict(zip(col_data, t)) for t in zip(*col_data.values())]
                # Filter out deleted rows
                rows = [r for r in rows if r[self.pk] not in self.deleted_keys]
                all_data.extend(rows)
        return all_data
