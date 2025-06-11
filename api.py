from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import time

# Suppose you have a main DB instance somewhere accessible:
from core.db import Database  # Adjust this import to your main DB class

db = Database()  # Or import your running DB instance

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
    stats = db.get_stats()
    stats["uptime"] = time.time() - start_time
    return stats
