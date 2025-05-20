# 🚀 FreshDB

FreshDB is a modern, educational Python database engine inspired by the best ideas from PostgreSQL, TiDB, AlloyDB, and ClickHouse. It features a hybrid transactional/analytical (HTAP) storage engine, 8KB block-based file layout, columnar analytics, tombstone deletes, GDPR-ready compaction, and a growing SQL command set.

> **FreshDB** is the perfect open-source project for anyone who wants to learn the internals of a database, contribute to real systems code, or just experiment with modern DBMS concepts!

---

## ✨ **Key Features**

- **Hybrid Storage:** OLTP (block-based row store + WAL) and OLAP (compressed column store with tombstones)
- **WAL Recovery:** Crash recovery and durability with per-table write-ahead logs
- **GDPR & Compaction:** Immediate and periodic physical erasure of deleted data (compaction)
- **8KB Block Architecture:** Table data is stored in fixed-size pages like a real RDBMS
- **Columnar Analytics:** Segments support compressed, column-oriented analytics with tombstones for deletes
- **Background Jobs:** Built-in job/event queue for periodic and on-demand tasks (compaction, index rebuild, etc.)
- **REPL & SQL Engine:** Interactive shell with support for CREATE, INSERT, SELECT, DELETE, compaction, and more
- **Schema Catalog:** All table/column definitions are catalogued in a system file (`schema.json`)
- **Extensible:** Easy to add new SQL features, storage formats, or maintenance jobs

---

## 🏗️ **Folder Structure Overview**

minidb/ 
├── data/ # DB files, WALs, column segments, schema catalog
├── config/ # Settings & environment config
├── core/ # Data types, column, row, and table logic
├── schema/ # Table definitions, schema.json
├── storage/ # BlockManager, RowStore, ColumnStore
├── indexing/ # (Planned) B+ tree and index manager
├── query/ # SQL parsing, planning, and execution
├── transaction/ # WAL and transactional logic
├── jobs/ # Background job/event queue
├── maintenance/ # (Optional) Maintenance managers/schedulers
├── cli/ # REPL for interactive use
├── tests/ # Unit tests for all modules
├── utils/ # Exceptions, helpers
├── main.py # Entry point / demo script
└── README.md # You are here!



---

## 🏁 **Getting Started**

### 1. **Clone and Install**

```bash
git clone https://github.com/yourusername/FreshDB.git
cd FreshDB/minidb
pip install -r requirements.txt
```

### 2. **Run the Database REPL**
```bash
python main.py
```

#### **Example session:**

```sql

FreshDB> CREATE TABLE users (id INT, name TEXT)
FreshDB> INSERT INTO users VALUES (1, 'Alice')
FreshDB> SELECT * FROM users
FreshDB> DELETE FROM users WHERE id = 1
FreshDB> gdpr users 1       # Immediately compacts & erases data for user 1 (GDPR)
FreshDB> exit
```

### 3. **Job/Event Loop**
FreshDB runs background jobs for periodic compaction and maintenance using a built-in job queue/event loop.
GDPR deletes trigger immediate compaction for regulatory compliance.

## 🧱 **Architecture**

- **RowStore:** Manages rows in 8KB blocks, supports efficient insert/delete with WAL for durability.

- **WALManager:** Every change is first written to a write-ahead log before updating the data files.

- **ColumnStore:** Stores OLAP segments in compressed columnar files, uses tombstone files to mask deleted rows until compaction.

- **Compaction:** Physically rewrites segments to erase deleted data, meeting GDPR/CCPA/DSGVO standards.

- **Job Queue:** All periodic and on-demand background jobs (e.g., compaction) run in a safe, table-aware event loop.

## 💡 **How to Contribute**
We’re building FreshDB as a learning and real-world project.
All skill levels are welcome! You can help with:

- SQL parsing and query engine extensions (e.g., WHERE, ORDER BY, GROUP BY, JOIN)

- Storage optimizations (indexing, columnar encoding, page caching)

- Background jobs, statistics, backup/restore, or distributed features

- Testing, docs, and code review

#### How to get started: 

1. Check the issues for beginner-friendly and advanced tasks.

2. Discuss your ideas or request guidance in the Discussions/Slack/Discord.

3. Fork the repo and submit a pull request!

## 🧪 **Development & Testing**
- All critical modules have associated unit tests in /tests/

- To run all tests:

``` bash
pytest tests/
```

## 🧭 **Roadmap**
- [ ] JOIN support and more advanced SQL
- [ ] Indexes (B+Tree, bitmap, etc.)
- [ ] Parallel and vectorized query engine
- [ ] Backups, restores, and snapshots
- [ ] Web UI/admin panel
- [ ] Distributed/clustered execution (sharding/replication)

## 📢 **Join Us!**
If you’re interested in how real databases work—or want to make one that’s practical and open for learning—FreshDB is for you.
Check out the code, read the architecture docs, join the conversation, and help build the future of Python-based databases!

## 📄 **License** 
Apache License 2.0 (see LICENSE)

#### Built with ❤️ by database geeks, for learners and professionals everywhere.