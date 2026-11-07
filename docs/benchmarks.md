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

## 4. 2026 SOTA Agent Memory & Vector Benchmark (KoreDB vs. Append-Only SQLite & Flat Vector Baselines)

This benchmark suite ([`SotaAgentMemoryAndVectorBenchmarkTest.kt`](../koredb/src/test/java/com/pankaj/koredb/benchmark/SotaAgentMemoryAndVectorBenchmarkTest.kt)) evaluates **KoreDB's Cognitive Memory Pyramid & Vector Engine** head-to-head against:
1. **Append-Only SQLite + FTS5 + Flat BLOB Vector Scan** (conventional relational agent memory architecture).
2. **Standard Flat Vector RAG** (append-only vector store baseline).
3. **Naive 2-Hop BFS GraphRAG** (standard GraphRAG without Hub-Noise Damping).

### 4.1 Storage-Layer Contradiction & Temporal Retrieval (Temporal Contradiction & Point-in-Time Recall)
* **Methodology**: Controlled deterministic subspace embeddings across 50 evolving user/project attributes over 4 historical sessions (`T1 -> T2 -> T3 -> T4`, totaling 200 episodic facts). Earlier sessions contain natural conversational keyword repetition while `T4` represents the latest state update. Isolates **storage & retrieval layer accuracy** from upstream LLM extraction noise.

| Engine / Retrieval Architecture | Active Truth Acc (`T4`) | Stale Fact Pollution (Top-3) | Point-in-Time `asOf(T2)` Acc | Lineage Audit (`T4->T1`) |
| :--- | :---: | :---: | :---: | :---: |
| **Flat Vector RAG** (Append-Only Vector Store) | 100.0% | 100.0% (Stale facts in Top-3) | N/A (0.0%) | N/A (0.0%) |
| **Append-Only SQLite FTS5 + Vector RRF** | 54.0% | 100.0% (Stale facts in Top-3) | N/A (0.0%) | N/A (0.0%) |
| **KoreDB Bi-Temporal Pyramid** (`KoreMemoryPyramid` + `AtomicFact`) | **100.0%** | **0.0%** | **100.0%** | **100.0%** |

### 4.2 Multi-Hop Associative Recall under High-Degree Hub Noise
* **Methodology**: 290 chunks with 25 2-hop reasoning chains (`Seed Fact -> Specific Bridge Entity -> Orthogonal Target Fact`) embedded inside 240 distractor chunks connected to 3 high-degree **Hub Entities** (`80+` edges each). The hop-2 target chunk is orthogonal to the query vector (`sim ~ 0.0`) and can only be discovered by traversing the specific bridge entity without hub-distractor flooding.

| Retrieval Engine | Hop-2 Recall@5 | Avg Query Latency | Architectural Behavior |
| :--- | :---: | :---: | :--- |
| **Flat Vector Search** (SQLite / Dense RAG) | 0.0% | `< 0.5 ms` | Cannot retrieve hop-2 passages orthogonal to the query vector. |
| **Naive 2-Hop BFS GraphRAG** (Vector Reranked) | 0.0% | `~ 1.8 ms` | Floods candidate pool with 80+ hub distractors that outscore the orthogonal target. |
| **KoreDB Hub-Damped PPR** (`hubDampingBeta = 0.8`) | **100.0%** | **1.59 ms** | Hub-noise damping ($W_{uv} = A_{uv} / d_{\text{in}}(v)^\beta$) suppresses hubs and routes probability mass through specific bridge entities. |

### 4.3 Symbolic Working Memory & Context Token Efficiency (25-Step Agent Trajectory)
* **Methodology**: 25-step Android debugging trajectory (`adb logcat`, stack traces, WAL diagnostics) totaling **31,645 tokens** (**126,525 bytes**). KoreDB compresses raw tool logs into L0 via pure-Kotlin **LZ4**, clusters L1 facts into L2 Scenarios via zero-LLM **Louvain community detection**, encodes active working state as a **Mermaid graph** (`GraphExport.toMermaid`), and drills down on demand (`pyramid.drillDown`).

| Metric | Raw Transcript Dumping | Text Summary Baseline | KoreDB Symbolic Working Memory |
| :--- | :---: | :---: | :---: |
| **Active Prompt Context Tokens** | 31,645 tokens | ~12,340 tokens | **325 tokens** |
| **Prompt Token Reduction (%)** | 0.0% | 61.0% | **99.0% fewer tokens** |
| **On-Disk L0 Payload Size** | 126,525 bytes | 126,525 bytes (Plain SQLite Text) | **18,120 bytes (6.98x LZ4 Compression)** |
| **1-Call `L2 -> L1 -> L0` Drill-Down** | N/A | SQL Join | **2.24 ms** (100% lossless byte recovery) |

### 4.4 ANN Vector Search: Latency, QPS, Recall@10, & Memory Footprint (3,000 Vectors x 256-dim)
* **Methodology**: 3,000 multi-resolution prefix-structured 256-dim vectors across 30 semantic clusters, evaluated over 100 queries (`topK = 10`, after JVM JIT warmup).

| Vector Engine Configuration | Avg Latency / Query | Throughput (QPS) | Speedup vs. SQLite | Recall@10 | Vector RAM (Compression) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SQLite BLOB Scan + Cosine** (Baseline) | 2.732 ms | 366 QPS | 1.0x | 100.0% | 3,000 KB (1.0x) |
| **KoreDB HNSW FP32** (16-Lane Fused SIMD Cosine) | **0.056 ms** | **17,939 QPS** | **49.0x faster** | **100.0%** | 3,000 KB (1.0x) |
| **KoreDB HNSW FP32 + Prefix-64 Funnel** | **0.048 ms** | **20,774 QPS** | **56.8x faster** | **100.0%** | 3,000 KB (1.0x) |
| **KoreDB HNSW SQ8** (8-Lane Unrolled Quantizer) | **0.076 ms** | **13,124 QPS** | **35.9x faster** | **97.6%** | **752 KB (4.0x smaller)** |
| **KoreDB HNSW SQ8 + Prefix-64 Combined** | **0.067 ms** | **14,846 QPS** | **40.6x faster** | **97.6%** | **752 KB (4.0x smaller)** |
| **KoreDB Rotated 1-Bit POPCNT** (2-Stage Rescored) | **0.394 ms** | **2,538 QPS** | **6.9x faster** | **100.0%** | **117 KB (25.6x smaller)** |

### 4.5 ACT-R / Ebbinghaus Memory Consolidation (1,000 Memories over 30 Days)
* **Methodology**: 1,000 L1 memories aged over a 30-day timeline (100 Permanent Invariants, 200 Spaced-Repetition Active facts, 200 Mid-Retention L2 Candidates, 500 Ephemeral Noise logs) consolidated via `KoreMemoryConsolidator.consolidate()`.

| Consolidation Metric | Result |
| :--- | :---: |
| **Permanent Invariants Preserved (`PERMANENT_INVARIANT`)** | **100 / 100 (100.0%)** |
| **Spaced-Repetition Memories Preserved (`ACTIVE`)** | **200 / 200 (100.0%)** |
| **Mid-Retention L2 Candidates Identified (`CANDIDATE`)** | **200 / 200 (100.0%)** |
| **Ephemeral Noise Memories Pruned via Cascading Delete (`DECAYED`)** | **500 / 500 (100.0%)** |
| **Total 1,000-Node Consolidation Execution Time** | **50.50 ms** |

### 4.6 On-Device `KoreEmbeddingPipeline` Throughput (Zipfian Agent Workload)
* **Methodology**: 400 agent embedding calls (`768-dim -> Prefix 128-dim` L2-normalized) over a skewed Zipfian query distribution (`87.0%` cache hit rate).

| Pipeline Configuration | Total Time (400 Queries) | Effective Throughput | Speedup vs. Uncached |
| :--- | :---: | :---: | :---: |
| **Direct Uncached Embedder + Naive Normalization** | 18.72 ms | 21,371 emb/sec | 1.0x |
| **`KoreEmbeddingPipeline.embed()`** (LRU Cache + 16-Lane Prefix Norm) | **1.24 ms** | **322,451 emb/sec** | **15.1x faster** |
| **`KoreEmbeddingPipeline.embedBatch()`** (Deduplicated Batch + Cache) | **0.84 ms** | **474,989 emb/sec** | **22.2x faster** |

---

## 5. Reproducing the Benchmarks

To execute the Room/SQLite and 2026 SOTA Agent Memory & Vector benchmarks on your machine:

```bash
./gradlew :koredb:testDebugUnitTest --tests "com.pankaj.koredb.benchmark.RoomVsKoreDbBenchmarkTest" --tests "com.pankaj.koredb.benchmark.SotaAgentMemoryAndVectorBenchmarkTest" --info
```
