import os

BLOCK_SIZE = 8192

class BlockManager:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            with open(path, "wb") as f:
                pass  # create file

    def num_blocks(self):
        return os.path.getsize(self.path) // BLOCK_SIZE

    def read_block(self, block_num):
        with open(self.path, "rb") as f:
            f.seek(block_num * BLOCK_SIZE)
            return f.read(BLOCK_SIZE)

    def write_block(self, block_num, data):
        # Ensure the file exists before opening in 'r+b'
        if not os.path.exists(self.path):
            with open(self.path, "wb") as f:
                pass
        with open(self.path, "r+b") as f:
            f.seek(block_num * BLOCK_SIZE)
            f.write(data)

    def allocate_block(self):
        block_num = self.num_blocks()
        with open(self.path, "ab") as f:
            f.write(b"\x00" * BLOCK_SIZE)
        return block_num
