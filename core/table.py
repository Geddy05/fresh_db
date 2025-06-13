import os
from core.column import Column
from indexing.bplustree import BplusTree
from storage.block_manager import BlockManager
from storage.manager import StorageManager

class Table:
    def __init__(self, name: str, storage: StorageManager, columns: list[Column] = []):
        self.name = name
        self.storage = storage
        self.columns = columns
        self.pk_column = next((col for col in self.columns if "PK" in col.constraints), None)
        self.rows = []
        self.auto_increment_col = next((col for col in self.columns if col.auto_increment), None)
        self._load_next_increment()
        self.indexes = {}
        
        # Use block-based index files (.idx)
        os.makedirs('data/indexes', exist_ok=True)
        for col in columns:
            if col.is_unique():
                idx_path = f"data/indexes/{self.name}_{col.name}.idx"
                block_manager = BlockManager(idx_path)
                # Root node id persistence (optional: could be in a metadata file)
                root_meta_path = f"{idx_path}.meta"
                root_node_id = None
                if os.path.exists(root_meta_path):
                    with open(root_meta_path, 'r') as f:
                        root_node_id = f.read().strip()
                bptree = BplusTree(order=32, block_manager=block_manager, root_node_id=root_node_id)
                self.indexes[col.name] = bptree
                # Always persist root node id for recovery
                with open(root_meta_path, 'w') as f:
                    f.write(bptree.root_node_id)

    def _load_next_increment(self):
        self.next_increment = 1
        if self.auto_increment_col:
            rows = self.storage.get_row_store(self.name).get_rows()
            if rows:
                self.next_increment = max(row[self.auto_increment_col.name] for row in rows) + 1

    def add_column(self, column: Column):
        self.columns.append(column)

    def _persist_index_root_ids(self):
        for col_name, bptree in self.indexes.items():
            idx_path = f"data/indexes/{self.name}_{col_name}.idx"
            meta_path = f"{idx_path}.meta"
            with open(meta_path, 'w') as f:
                f.write(bptree.root_node_id)

    def insert(self, row_dict: dict):
        for col in self.columns:
            val = row_dict.get(col.name)
            if col.is_not_null() and val is None:
                raise ValueError(f"{col.name} cannot be NULL")

        if self.auto_increment_col:
            if self.auto_increment_col.name not in row_dict or row_dict[self.auto_increment_col.name] is None:
                row_dict[self.auto_increment_col.name] = self.next_increment
                self.next_increment += 1

        for col_name, bptree in self.indexes.items():
            key = row_dict[col_name]
            if bptree.search(key) is not None:
                raise ValueError(f"Duplicate value for UNIQUE column '{col_name}'")
        
        row_idx = len(self.rows)
        self.storage.write_row(self.name, row_dict)

        # Update indexes (persist dirty nodes only)
        for col_name, bptree in self.indexes.items():
            bptree.insert(row_dict[col_name], row_idx)
        self._persist_index_root_ids()

    def bulk_insert(self, rows: list[dict], bulk_mode=False):
        # 1. Check for duplicates *in the batch only* if bulk_mode, otherwise check both in tree and batch
        for col_name, bptree in self.indexes.items():
            batch_keys = [row[col_name] for row in rows]
            if bulk_mode:
                # Only check for duplicates *within the batch*
                if len(batch_keys) != len(set(batch_keys)):
                    raise ValueError(f"Duplicate values in batch for UNIQUE column '{col_name}'")
            else:
                existing_keys = set(k for k, _ in bptree.scan())
                for key in batch_keys:
                    if key in existing_keys:
                        raise ValueError(f"Duplicate value for UNIQUE column '{col_name}' (already exists): {key}")
                    if batch_keys.count(key) > 1:
                        raise ValueError(f"Duplicate value for UNIQUE column '{col_name}' (within batch): {key}")

        # 2. Validate NOT NULL and auto-increment
        for row_dict in rows:
            for col in self.columns:
                val = row_dict.get(col.name)
                if col.is_not_null() and val is None:
                    raise ValueError(f"{col.name} cannot be NULL")
            if self.auto_increment_col:
                if self.auto_increment_col.name not in row_dict or row_dict[self.auto_increment_col.name] is None:
                    row_dict[self.auto_increment_col.name] = self.next_increment
                    self.next_increment += 1

        # 3. Write all rows in bulk to storage
        self.storage.bulk_write(self.name, rows)

        # 4. Add to in-memory rows list (for index rebuild)
        for row_dict in rows:
            self.rows.append(row_dict)

        # 5. If not bulk mode, update indexes as you go
        if not bulk_mode:
            for row_dict in rows:
                row_idx = len(self.rows)
                for col_name, bptree in self.indexes.items():
                    bptree.insert(row_dict[col_name], row_idx)
            # Save dirty nodes
            for col_name, bptree in self.indexes.items():
                for node in bptree.dirty_nodes():
                    bptree.save_node(node)

    def rebuild_index(self):
        """Bulk rebuild all B+Tree indexes from self.rows."""
        for col_name, bptree in self.indexes.items():
            print(f"[BulkLoad] Rebuilding index for column {col_name}...")
            # Gather all key -> rowid pairs
            pairs = [(row[col_name], idx) for idx, row in enumerate(self.rows)]
            pairs.sort()  # Required by B+Tree bulk load (by key)
            # Build a new block_manager for this index
            idx_path = f"data/indexes/{self.name}_{col_name}_bptree.json"
            block_manager = BlockManager(idx_path)
            # Build the new B+Tree
            new_bptree = BplusTree.bulk_load(pairs, order=32, block_manager=block_manager)
            self.indexes[col_name] = new_bptree  # Swap in-place!
        print("[BulkLoad] All indexes rebuilt.")

    def flush(self):
        self.storage.flush_table(self.name)

    def delete_rows(self, column, value):
        row_store = self.storage.get_row_store(self.name)
        rows = row_store.get_rows()
        initial_len = len(rows)
        rows_to_keep = [row for row in rows if str(row.get(column)) != value]
        n_deleted = initial_len - len(rows_to_keep)
        row_store.clear()
        for row in rows_to_keep:
            row_store.insert_row(row)
        self._persist_index_root_ids()
        return n_deleted

    def select_all(self):
        oltp_rows = self.storage.get_row_store(self.name).get_rows()
        olap_rows = self.storage.get_column_store(self.name).load_segments()
        return oltp_rows + olap_rows

    def __repr__(self):
        return f"<Table {self.name} Columns={self.columns}>"
