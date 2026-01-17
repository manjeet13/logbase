# Logbase – Design & Implementation Walkthrough

## High-Level Architecture

```
Client (HTTP)
    |
    v
Engine
 ├── WAL (durable, append-only)
 ├── MemTable (in-memory, mutable)
 └── SSTables (immutable, on-disk)
        ├── Sparse Index
        └── Bloom Filter
```

The system follows an LSM-tree–inspired architecture optimized for write-heavy workloads.

---

## Write Path

1. Client issues a PUT / DELETE / BatchPut
2. Operation is appended to the Write-Ahead Log (WAL)
3. The update is applied to the MemTable
4. When the MemTable exceeds a size threshold:

   * It is flushed to a new immutable SSTable
   * WAL is rotated and older segments are truncated
   * Compaction may be triggered

This ensures durability before acknowledgment.

---

## Write-Ahead Log (WAL)

* Append-only binary log
* Segmented into multiple files
* Each record is prefixed with an operation type byte
* WAL is replayed on startup to reconstruct the MemTable
* Old WAL segments are deleted only after successful SSTable flush

Concurrency:

* WAL writes, rotation, and close are serialized using a mutex

---

## MemTable

* In-memory map protected by a mutex
* Stores both values and tombstones
* Tombstones represent deletes
* Acts as the authoritative source for the most recent writes

---

## SSTables

* Immutable, sorted key–value files on disk
* Created by flushing the MemTable
* Never modified after creation

### Indexing

* Each SSTable maintains a sparse in-memory index
* Index entries map keys to file offsets
* Used to narrow disk scans during reads

---

## Bloom Filters

* One Bloom filter per SSTable
* Built at SSTable creation time
* Stored as a sidecar file and loaded on startup
* Used during point lookups to skip SSTables that cannot contain a key

Bloom filters guarantee no false negatives.

---

## Read Path

1. Check MemTable
2. Check SSTables from newest to oldest

   * Consult Bloom filter
   * Use sparse index to limit scanning
3. Tombstones mask older values

---

## Deletes & Tombstones

Deletes are implemented using tombstones:

* Delete operations are written to the WAL
* A tombstone is stored in the MemTable
* Tombstones are flushed to SSTables
* Physical removal occurs during compaction

This matches standard LSM-tree semantics.

---

## Range Queries

Range queries:

* Scan MemTable and SSTables for keys within the range
* Merge results from newest to oldest
* Respect tombstones

---

## Compaction

Logbase implements a simple Level-0 compaction strategy:

* Triggered when SSTable count exceeds a threshold
* All SSTables are merged into one
* Newer entries override older ones
* Tombstones are dropped
* Old SSTables are deleted

---

## Batch Writes

BatchPut:

* Appends multiple entries to the WAL under a single lock
* Flushes WAL once
* Applies batch to MemTable
* Reduces write amplification while preserving durability

---

## Shutdown Semantics

On shutdown:

1. Remaining MemTable contents are flushed
2. WAL is flushed and closed
3. File descriptors are released

This guarantees no acknowledged writes are lost.

---

## Tradeoffs & Simplifications

* Single-level compaction
* No background compaction threads
* No MVCC or snapshots
* No replication

These were deliberate choices to keep the system understandable while remaining correct.

---

## Future Improvements

* Multi-level compaction
* Background compaction workers
* Snapshot isolation
* Metrics and observability
* Replication and sharding

---

## Summary

Logbase demonstrates a complete and correct LSM-based storage engine with clear layering, durability guarantees, and realistic design tradeoffs suitable for a senior R&D engineering role.
