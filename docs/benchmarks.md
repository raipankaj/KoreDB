# KoreDB vs Room / SQLite: Head-to-Head Comparative Benchmark

This document presents the detailed methodology and performance metrics comparing **KoreDB 0.2.0** against **Android Room / SQLite (WAL mode)**.

---

## 1. Test Environment & Methodology

- **Hardware**: Apple Silicon (M-series ARM64, 16 GB unified RAM).
- **Runtime**: OpenJDK 21 (Adoptium 64-Bit Server VM).
- **SQLite Configuration**: SQLite 3.x in `WAL` (Write-Ahead Logging) mode, `synchronous = NORMAL`.
- **KoreDB Configuration**: KoreDB 0.2.0, synchronous WAL durability, binary CBOR serialization.
- **Dataset**: 10,000 realistic e-commerce `Product` entities:
  - Fields: `id: String`, `title: String`, `category: String`, `price: Double`, `stock: Int`
  - Indices: Secondary String Index on `category`, Numeric Range Index on `price`.

---

## 2. Real Hardware Benchmark: Google Pixel 7 Pro (cheetah, Android 14/15)

Executed natively on a physical **Google Pixel 7 Pro** over USB via Android Instrumentation Runner (`./gradlew :app:connectedDebugAndroidTest`). **64 out of 64 tests passed with 100% success rate**.

| Category | Operation / Workload | Room / SQLite | KoreDB (Optimized) | Outcome on Pixel 7 Pro |
| :--- | :--- | :---: | :---: | :---: |
| **Point Ops** | **Single Writes (5,000 ops)** | 15,125 ms | **585 ms** | **🏆 KoreDB (25.8x Faster)** |
| **Point Ops** | **Single Reads (5,000 ops)** | 5,622 ms | **88 ms** | **🏆 KoreDB (63.9x Faster)** |
| **Point Ops** | **Point Reads (10,000 ops, cached)** | 8,905 ms | **6 ms** | **🏆 KoreDB (1,484x Faster)** |
| **Lookups** | **Negative Lookups (5,000 ops)** | 4,099 ms | **33 ms** | **🏆 KoreDB (124.2x Faster)** |
| **Bulk Ops** | **Bulk Ingest (50,000 items)** | 926 ms | **612 ms** | **🏆 KoreDB (1.51x Faster)** |
| **Bulk Ops** | **Random Updates (10,000 items)** | 308 ms | **101 ms** | **🏆 KoreDB (3.05x Faster)** |
| **Range Queries** | **Large Content Range (50KB/rec, 5x)**| 1,381 ms | **220 ms** | **🏆 KoreDB (6.28x Faster)** |
| **Range Queries** | **Complex Range (50KB/rec)** | 225 ms | **26 ms** | **🏆 KoreDB (8.65x Faster)** |
| **Concurrency** | **Parallel Reads (8 threads)** | 3,638 ms | **209 ms** | **🏆 KoreDB (17.4x Faster)** |
| **Concurrency** | **Concurrent Writes (8 threads)** | 208 ms | **117 ms** | **🏆 KoreDB (1.78x Faster)** |
| **Vectors** | **Vector Search (top-10 × 50)** | 3,384 ms | **89 ms** | **🏆 KoreDB (38.0x Faster)** |
| **Vectors** | **KNN Search (50 queries, 15k vecs)** | 29,871 ms | **135 ms** | **🏆 KoreDB (221.3x Faster)** |
| **Vectors** | **Vector Delete (500 items)** | 1,273 ms | **74 ms** | **🏆 KoreDB (17.2x Faster)** |
| **Vectors** | **Vector Update (500 items)** | 1,290 ms | **145 ms** | **🏆 KoreDB (8.9x Faster)** |
| **Secondary Idx**| **Index Lookups (1,000 ops)** | 4,357 ms | **257 ms** | **🏆 KoreDB (17.0x Faster)** |
| **Graph Engine** | **Graph Build (2K Nodes, 10K Edges)** | 25,921 ms | **484 ms** | **🏆 KoreDB (53.6x Faster)** |
| **Graph Engine** | **2-Hop Traversal (toIdList)** | 3 ms | **10 ms** | *Room SQL JOIN (3.3x)* |
| **Graph Engine** | **PageRank (500 nodes, 5 iter)** | *Unsupported* | **195 ms** | **🏆 KoreDB Native Graph** |
| **Graph Engine** | **Dijkstra Shortest Path** | *Unsupported* | **54 ms** | **🏆 KoreDB Native Graph** |

---

## 3. JVM Desktop vs Real Mobile Device Comparison

---

## 3. Deep-Dive Analysis by Workload

### Workload 1: 1,000 Discrete Transactions (KoreDB 4.3x Faster)
- **Room / SQLite**: 42.11 ms (~42 µs per commit).
- **KoreDB**: **9.77 ms** (~9.7 µs per commit, **>102,000 transactions/sec**).
- **Why KoreDB Wins**:
  KoreDB uses a synchronous, zero-allocation commit pipeline (`writeBatchDirect`). The transaction commits directly to the in-memory MemTable and appends to the WAL buffer on the caller thread with zero coroutine scheduling or queue contention.

### Workload 2: 2,000 Numeric Range Scans (KoreDB 6.1x Faster)
- **Room / SQLite**: 131.90 ms.
- **KoreDB**: **21.46 ms**.
- **Why KoreDB Wins**:
  SQLite scans B-tree nodes with floating point comparisons in SQL evaluation bytecodes. KoreDB encodes IEEE-754 doubles using **Order-Preserving Binary Encoding**, transforming numeric ranges into fast, contiguous byte-range scans directly across memory-mapped files.

### Workload 3: 2,000 Secondary Index Scans (KoreDB 5.7x Faster)
- **Room / SQLite**: 129.83 ms.
- **KoreDB**: **22.67 ms**.
- **Why KoreDB Wins**:
  KoreDB's secondary indices are flat prefixes in the LSM-tree (`idx:$name:$idxName:$val:$id`). Prefix scans execute as sequential sub-millisecond memory-mapped iterations without index table joining overhead.

### Workload 4: 10,000 Point Lookups by ID (KoreDB 3.9x Faster)
- **Room / SQLite**: 39.72 ms.
- **KoreDB**: **10.06 ms** (~1.0 µs per lookup).
- **Why KoreDB Wins**:
  KoreDB checks the tiered storage hierarchy: in-memory LRU cache ($O(1)$), active MemTable ($O(\log N)$ in RAM), and SSTable Bloom filters. 99% of negative seeks are eliminated before touching the disk.

### Workload 5: 10,000 Bulk Ingestion
- **Room / SQLite**: 29.12 ms.
- **KoreDB**: **45.56 ms**.
- **Analysis**:
  SQLite executes directly inside compiled C (`-O3`) operating on pre-allocated 4KB memory buffers with raw pointers. KoreDB executes on the JVM, encoding 10,000 Kotlin objects into CBOR byte arrays and populating 30,000 index nodes. Through recent optimizations (contiguous batch serialization and non-vararg index formatting), KoreDB closed the gap from 71.56 ms down to 45.56 ms.

---

## 4. Reproducing the Benchmark

To execute the benchmark on your machine:

```bash
./gradlew :koredb:testDebugUnitTest --tests com.pankaj.koredb.benchmark.RoomVsKoreDbBenchmarkTest --info
```
