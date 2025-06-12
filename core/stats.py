import os

def get_basic_stats(schema):
    stats = {}
    stats["total_tables"] = len(schema.tables)
    stats["tables"] = {}

    for table_name, table in schema.tables.items():
        # Get disk usage if stored on disk (optional)
        try:
            data_file = f"data/{table_name}.tbl"
            disk_bytes = os.path.getsize(data_file) if os.path.exists(data_file) else 0
        except Exception:
            disk_bytes = 0

        # Get number of rows (live, OLTP)
        try:
            num_rows = len(table.storage.get_row_store(table.name).get_rows())
        except Exception:
            num_rows = None  # or 0

        stats["tables"][table_name] = {
            "name" : table_name,
            "columns": len(table.columns),
            "rows": num_rows,
            "disk_usage_bytes": disk_bytes,
            "indexes": list(table.indexes.keys()),
            "has_pk": bool(table.pk_column),
        }

    return stats
