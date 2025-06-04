import json

def encode_rows_block(rows):
    """
    Header: 2 bytes = number of rows
    Payload: JSON serialization of list of rows
    """
    payload = json.dumps(rows).encode("utf-8")
    row_count = len(rows)
    header = row_count.to_bytes(2, "big")
    return header + payload

def decode_rows_block(data):
    if len(data) < 2:
        return []
    row_count = int.from_bytes(data[:2], "big")
    if row_count == 0:
        return []
    payload = data[2:]
     # Remove trailing nulls (padding), then decode to string, then load JSON
    payload_str = payload.rstrip(b"\x00").decode("utf-8")
    return json.loads(payload_str)