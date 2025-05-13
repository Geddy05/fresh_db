# FreshDB

**FreshDB** is a Python-based educational SQL database engine built from scratch to help understand how core components of a database work — including schema management, disk-based storage, simple queries, and optional indexing.

---

## 📁 Folder Structure Overview

```bash
minidb/
├── data/              # Folder for storing the db files
├── config/            # Settings & environment config
├── core/              # Data types, column, row, and in-memory table logic
├── schema/            # Manages table definitions and metadata
├── storage/           # File, page, and record management (disk I/O)
├── indexing/          # B+ tree and index manager (optional)
├── query/             # SQL parsing, planning, and execution
├── transaction/       # Logging, transaction lifecycle, and recovery
├── cli/               # REPL for interacting with the database
├── tests/             # Unit tests for all modules
├── utils/             # Exceptions, helper functions
├── main.py            # Entry point
└── README.md          # Project documentation
