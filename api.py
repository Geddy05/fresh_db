from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import time

from core.stats import get_basic_stats
from schema.schema import Schema
from storage.manager import StorageManager

# Suppose you have a main DB instance somewhere accessible:

storage_manager = StorageManager()
schema = Schema()
schema.load_schema(storage_manager)  # Discover tables on startup


app = FastAPI()

# Allow React dev server access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

start_time = time.time()

@app.get("/stats")
def get_stats():
    # Implement this in your Database class!
    stats = get_basic_stats(schema)
    stats["uptime"] = time.time() - start_time
    return stats
