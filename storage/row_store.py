import json
import os
from storage.row_packer import encode_rows_block, decode_rows_block
from storage.block_manager import BlockManager
from transaction.wal_manager import WALManager

class RowStore:
    def __init__(self, table_name,pk="id", base_path='data/wal/'):
        self.table_name = table_name
        self.pk = pk
        self.block_path = os.path.join(base_path, f"{table_name}.tbl")
        self.wal_manager = WALManager(table_name)
        self.bm = BlockManager(self.block_path)
        self.block_rows = {}  # block_num -> [row, ...]
        self._load_blocks()
        self._recover_from_wal()


    def _load_blocks(self):
        for block_num in range(self.bm.num_blocks()):
            raw = self.bm.read_block(block_num)
            rows = decode_rows_block(raw)
            self.block_rows[block_num] = rows

    def _recover_from_wal(self):
        # Replay WAL and insert each row as if it was new
        def apply_row(row):
            self._insert_without_wal(row)
        def apply_delete(key):
            self._delete_without_wal(key)
        self.wal_manager.replay(apply_row,apply_delete)

    # def _load_wal(self):
    #     if os.path.exists(self.wal_path):
    #         with open(self.wal_path, 'r') as f:
    #             for line in f:
    #                 row = json.loads(line.strip())
    #                 self.rows.append(row)

    def insert_row(self, row):
        self.wal_manager.log_insert(row)
        self._insert_without_wal(row)

    def bulk_insert_rows(self, rows: list[dict]):
        """
        Insert a batch of rows with minimal WAL and block writes.
        """
        # 1. WAL: log all rows at once (if your WALManager supports batch logging, otherwise loop)
        self.wal_manager.log_insert_many(rows) if hasattr(self.wal_manager, "log_insert_many") else [self.wal_manager.log_insert(r) for r in rows]
        
        # 2. Buffer rows for block writing
        buffer = []
        last_block = self.bm.num_blocks() - 1
        if last_block >= 0:
            buffer = self.block_rows.get(last_block, [])
        
        for row in rows:
            buffer.append(row)
            # If buffer full, write and start new block
            if len(buffer) >= 50:
                block_num = last_block if last_block >= 0 else self.bm.allocate_block()
                self.block_rows[block_num] = buffer
                self.bm.write_block(block_num, encode_rows_block(buffer))
                buffer = []
                last_block = self.bm.allocate_block()
        # Write any remaining rows in buffer
        if buffer:
            block_num = last_block if last_block >= 0 else self.bm.allocate_block()
            self.block_rows[block_num] = buffer
            self.bm.write_block(block_num, encode_rows_block(buffer))

    def _insert_without_wal(self, row):
        # Try to fit in the last block
        last_block = self.bm.num_blocks() - 1
        if last_block < 0 or len(self.block_rows.get(last_block, [])) >= 50:  # limit: 50 rows/block
            block_num = self.bm.allocate_block()
            rows = []
        else:
            block_num = last_block
            rows = self.block_rows.get(block_num, [])
        rows.append(row)
        self.block_rows[block_num] = rows
        self.bm.write_block(block_num, encode_rows_block(rows))

    def delete_row(self, key_value):
        self.wal_manager.log_delete(key_value)
        self._delete_without_wal(key_value)

    def _delete_without_wal(self, key_value):
        for block_num, rows in self.block_rows.items():
            for i, row in enumerate(rows):
                if row.get(self.pk) == key_value:
                    del rows[i]
                    self.bm.write_block(block_num, encode_rows_block(rows))
                    return True
        return False

    def get_rows(self):
        all_rows = []
        for rows in self.block_rows.values():
            all_rows.extend(rows)
        return all_rows
    
    def clear(self):
        self.block_rows = {}
        self.wal_manager.clear()
    
    # def clear(self):
    #     """Clear rows and WAL after flushing."""
    #     self.rows = []
    #     open(self.wal_path, "w").close()

    # def replay_wal(self):
    #     """Restore rows from WAL at startup."""
    #     if os.path.exists(self.wal_path):
    #         with open(self.wal_path, "r") as f:
    #             for line in f:
    #                 row = json.loads(line.strip())
    #                 self.rows.append(row)