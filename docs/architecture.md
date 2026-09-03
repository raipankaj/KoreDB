# KoreDB Engine Architecture & LSM Internals

KoreDB is engineered from the ground up as a **Log-Structured Merge-tree (LSM)** storage engine specifically tailored for mobile and embedded devices with flash memory (NAND/eMMC/UFS).

Unlike traditional B-tree engines (such as SQLite) that perform in-place page writes, random I/O overwrites, and complex tree rebalancing, KoreDB converts all write operations into sequential disk appends.

---

## 1. The Core LSM Hierarchy

KoreDB organizes data across four distinct storage tiers:

```
 WRITE PIPELINE                                READ PIPELINE
 
   [Write Request]                               [Point / Range Read]
          │                                               │
   ┌──────┴───────────────┐                               ▼
   ▼                      ▼                      ┌─────────────────┐
┌──────────────┐   ┌──────────────┐              │  LRU Cache      │
│ Write-Ahead  │   │ In-Memory    │              │  (Hot Objects)  │
│ Log (WAL)    │   │ MemTable     │◄─────────────┤  O(1) Memory    │
│ (Sequential) │   │ (Concurrent  │              └────────┬────────┘
└──────────────┘   │  SkipList)   │                       │ (Cache Miss)
                   └──────┬───────┘                       ▼
                          │ (Flushed at 16MB)    ┌─────────────────┐
                          ▼                      │ In-Memory       │
                   ┌──────────────┐              │ MemTable        │
                   │ Immutable    │◄─────────────┤ O(log N) RAM    │
                   │ MemTable     │              └────────┬────────┘
                   └──────┬───────┘                       │ (Not Found)
                          │ (Background Flush)            ▼
                          ▼                      ┌─────────────────┐
                   ┌──────────────┐              │ Immutable       │
                   │ Level 0      │◄─────────────┤ MemTable        │
                   │ SSTable      │              └────────┬────────┘
                   └──────┬───────┘                       │ (Not Found)
                          │ (Leveled Compaction)          ▼
                          ▼                      ┌─────────────────┐
                   ┌──────────────┐              │ Bloom Filter    │
                   │ Level 1, 2…  │◄─────────────┤ Check: 10 bits  │
                   │ SSTables     │              │ per key         │
                   └──────────────┘              └────────┬────────┘
                                                          │ (May Exist)
                                                          ▼
                                                 ┌─────────────────┐
                                                 │ Sparse Index &  │
                                                 │ Memory-Mapped   │
                                                 │ SSTable Block   │
                                                 └─────────────────┘
```

---

## 2. Write-Ahead Log (WAL)

The Write-Ahead Log provides immediate, synchronous crash durability before any in-memory mutation occurs.

### Binary Framing Format
Every transaction in the WAL is delimited by binary markers and protected with a 64-bit CRC32 checksum:

```
┌─────────────────┬───────────────────┬───────────────────┬─────────────┬───────────┐
│ RECORD_BEGIN    │ Record 1: PUT/DEL │ Record N: PUT/DEL │ BATCH_CRC   │ COMMIT    │
│ (4 Bytes: 0x01) │ [Op][KeyLen][Val] │ [Op][KeyLen][Val] │ (8-Byte CRC)│ (4 Bytes) │
└─────────────────┴───────────────────┴───────────────────┴─────────────┴───────────┘
```

### Crash Recovery & Corrupt Tail Discard
When KoreDB boots, it replays the WAL sequentially:
1. Validates each batch header and computes the CRC32 checksum across all keys and values.
2. If a transaction batch matches its stored CRC32, mutations are repopulated into the MemTable.
3. If an ungraceful crash or power loss occurs mid-write, KoreDB detects the truncated tail, discards the uncommitted partial batch, and truncates the WAL to the last valid commit boundary (`truncateCorruptTail`).

---

## 3. In-Memory MemTable

The MemTable is backed by Java's `ConcurrentSkipListMap`, providing lock-free, concurrent $O(\log N)$ point lookups, concurrent ordered range scans, and thread-safe batch insertions.

- **Threshold**: When the active MemTable reaches `16 MB` (`MEMTABLE_FLUSH_THRESHOLD_BYTES`), it is atomically frozen into an `ImmutableMemTable`.
- **Background Flusher**: A background coroutine serializes the immutable MemTable into a new sorted Level 0 SSTable on disk, resets the immutable reference, and truncates the WAL to 0 bytes.

---

## 4. Tiered SSTables & Memory-Mapped Zero-Copy I/O

When a MemTable is written to disk, it produces an **SSTable** (`Sorted String Table`).

### SSTable File Layout
```
┌────────────────────────────────────────────────────────┐
│ Block 0: Key1-Val1, Key2-Val2... (Sorted Key-Values)   │
├────────────────────────────────────────────────────────┤
│ Block 1: KeyN-ValN...                                  │
├────────────────────────────────────────────────────────┤
│ Block 2: ...                                           │
├────────────────────────────────────────────────────────┤
│ Bloom Filter BitSet (10 bits per key, 1% false positive)│
├────────────────────────────────────────────────────────┤
│ Sparse Block Index (Offsets to 4KB Block Starts)       │
├────────────────────────────────────────────────────────┤
│ Metadata Footer (Bloom Offset, Codec, Magic Number)   │
└────────────────────────────────────────────────────────┘
```

### Zero-Copy Memory-Mapped Access
- Every SSTable is opened using `FileChannel.map(READ_ONLY)` into a `MappedByteBuffer`.
- Reads bypass standard Java heap allocations. Vector similarity queries and point lookups scan directly against OS kernel page buffers.
- Vector payloads (`vec:...`) remain raw, uncompressed IEEE-754 floats to guarantee zero-copy floating point arithmetic without array conversion.

---

## 5. Leveled Compaction with Index Truth Oracle

Over time, multiple SSTables accumulate tombstones (deletions) and superseded document versions. KoreDB uses a multi-level compaction strategy:

1. **Streaming K-Way Merge**:
   - Compaction initializes `SSTableIterator` instances across all participating SSTables and feeds them into a `PriorityQueue`.
   - Keys are merged in $O(K \log K)$ streaming fashion directly to a new SSTable file without intermediate heap buffering.
2. **Truth Oracle Index Pruning**:
   - Secondary indices (`idx:...` and `g:idx:v_prop:...`) are validated against reverse pointers (`rptr:...`).
   - If a document was updated, older index entries pointing to stale values are pruned during compaction, reclaiming 60%+ disk space.
3. **Cascading Levels**:
   - L0 SSTables merge into Level 1.
   - When Level 1 exceeds 10 MB, it cascades into Level 2.
   - When Level 2 exceeds 50 MB, it cascades into Level 3.
