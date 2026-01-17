# Logbase

Logbase is a lightweight, persistent key–value store implemented in Go. It is based on an **LSM-tree–style architecture** and is designed to demonstrate real-world database internals such as write-ahead logging, MemTables, SSTables, compaction, and Bloom filters.

The project prioritizes **correctness, durability, and clarity of design** over feature completeness or extreme optimization.

---

## Features

* Write-Ahead Log (WAL) with segmentation and truncation
* In-memory MemTable with tombstone-based deletes
* Immutable on-disk SSTables
* Sparse indexing for SSTables
* Bloom filters for fast negative lookups
* Range queries
* SSTable compaction
* Batch writes
* Simple HTTP API
* Environment-based configuration
* Graceful shutdown

---

## Running Logbase

### Prerequisites

* Go 1.20+

### Start the server

```bash
export LOGBASE_HTTP_PORT=8080
go run ./cmd/server
```

---

## Configuration

All runtime configuration is provided via environment variables.

| Variable                       | Description              | Default   |
| ------------------------------ | ------------------------ | --------- |
| `LOGBASE_HTTP_PORT`            | HTTP server port         | `8080`    |
| `LOGBASE_DATA_DIR`             | Data directory           | `data`    |
| `LOGBASE_MEMTABLE_FLUSH_BYTES` | MemTable flush threshold | `1048576` |
| `LOGBASE_MAX_SSTABLES`         | Compaction trigger       | `4`       |

---

## HTTP API

### Health Check

```
GET /health
```

### Put

```
PUT /kv/{key}
Body: raw bytes
```

### Get

```
GET /kv/{key}
```

### Delete

```
DELETE /kv/{key}
```

### Range Query

```
GET /range?start=a&end=z
```

---

## Notes

* Data is persisted to disk under the configured data directory
* All writes are durable once acknowledged
* Deletes are handled using tombstones and reclaimed during compaction

---

## License

MIT
