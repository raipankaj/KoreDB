# KoreDB 🚀

<p align="center">
  <img src="logo.png" alt="KoreDB Logo" width="344">
  <br>
  <b>The AI-Native, High-Performance NoSQL & Graph Database for Modern Android.</b>
</p>

KoreDB is a pure Kotlin, embedded database engine built from the ground up using a **Log-Structured Merge-tree (LSM)** architecture. Unlike SQLite (designed for spinning disks in 2000), KoreDB is optimized for modern flash storage, high-concurrency Coroutines, on-device AI, and complex relationship mapping.

---

## ✨ Features

*   **⚡ Blazing Performance:** LSM architecture offers $O(1)$ write performance with a "Nitro" parallel serialization path.
*   **🤖 AI-Native Vector Store:** Sub-millisecond Approximate Nearest Neighbor (ANN) search using a **Hierarchical Navigable Small World (HNSW)** index.
*   **🕸️ Built-in Graph DB:** First-class support for property graphs with bidirectional traversals and optimized relationship indices.
*   **🏗️ Pure Kotlin:** 100% Kotlin with Zero JNI overhead. No more `sqlite3.so` bloat.
*   **🔗 Coroutine First:** Built for non-blocking I/O and reactive UI with `Flow`, featuring background indexing and **automatic hydration** for vectors.
*   **🛡️ Crash Resilient:** Write-Ahead Logging (WAL) with CRC32 checksums ensures your data survives process death or system crashes.
*   **🔍 Optimized Reads:** Bloom Filters, Sparse Indexing, and HNSW graphs ensure fast lookups by avoiding unnecessary disk I/O.
*   **📦 Lightweight:** Minimal footprint, perfect for mobile apps.

---

## 🚀 Quick Start

### 1. Installation

Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.raipankaj:koredb:0.0.5")
}
```

### 2. Setup Serialization (Recommended)

KoreDB uses **Kotlinx Serialization** to handle Data Classes. To use the built-in `collection<T>` API, add the serialization plugin to your project:

**Project `build.gradle.kts`:**
```kotlin
plugins {
    kotlin("plugin.serialization") version "2.0.21" 
}
```

**Module `build.gradle.kts`:**
```kotlin
plugins {
    kotlin("plugin.serialization")
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
}
```

### 3. Initialize Database

Initialize KoreDB once in your `Application` class:

```kotlin
class MyApplication : Application() {
    lateinit var database: KoreDatabase

    override fun onCreate() {
        super.onCreate()
        // Initializes the engine in the app's secure internal storage
        database = KoreAndroid.create(this, "my_notes_db")
    }
}
```

---

## ✅ Enterprise-Grade Reliability

KoreDB is rigorously tested to ensure stability, data integrity, and performance under extreme conditions. Our test suite covers **100+ scenarios**, including:

*   **🛡️ Crash Recovery**: Validated against truncated WALs and power-loss scenarios.
*   **⚡ High Concurrency**: Thread-safety verified with 100+ concurrent writers and readers.
*   **🔄 Schema Evolution**: Supports adding/removing fields in data classes without migration scripts (Forward/Backward compatibility).
*   **🕸️ Graph Integrity**: Validated against complex topologies (Supernodes, Cycles, Self-Loops) and "Ghost Index" anomalies.
*   **🤖 Vector Robustness**: Handles edge cases like zero vectors, dimensionality mismatches, and overflows gracefully.
*   **🔥 Stress Testing**: Proven stable with 100,000+ record bulk ingestion and mixed R/W workloads.

---

## 🛠️ Supported Operations

### 📦 Typed Collections
Manage your data classes with ease using `KoreCollection<T>`.

| Operation | Description |
| :--- | :--- |
| `insert(id, doc)` | Inserts or updates a document. |
| `insertBatch(map)` | Efficiently saves multiple documents in one transaction. |
| `getById(id)` | Retrieves a document by its unique ID. |
| `getAll()` | Retrieves all documents in the collection. |
| `delete(id)` | Deletes a document (uses $O(1)$ Tombstones). |
| `observeById(id)` | Returns a `Flow<T?>` that emits updates for a specific record. |
| `observeAll()` | Returns a `Flow<List<T>>` that emits whenever the collection changes. |
| `createIndex(name, extractor)` | Registers a secondary index for fast querying. |

### 🤖 AI Vector Collections
Store and search high-dimensional embeddings with `KoreVectorCollection`.

| Operation | Description |
| :--- | :--- |
| `insert(id, floatArray)` | Saves a vector embedding. Indexing happens in the background. |
| `insertBatch(map)` | Efficiently saves multiple vectors using parallel serialization. |
| `search(query, limit)` | Performs O(log N) HNSW search for sub-millisecond retrieval. |
| `waitForIndexing()` | (Experimental) Blocks until background HNSW indexing is complete. |

### 🕸️ Graph Database
Store entities and relationships with `GraphStorage`.

| Operation | Description |
| :--- | :--- |
| `putNode(node)` | Saves a node with labels and properties. |
| `getNode(id)` | Retrieves a node by ID. |
| `putEdge(edge)` | Creates a bidirectional relationship. |
| `query { ... }` | Fluent DSL for traversing the graph (Outbound/Inbound). |
| `transaction { ... }` | Executes graph mutations atomically. |

---

## 💡 Usage Examples

### Basic CRUD & Observation
```kotlin
@Serializable
data class Note(val id: String, val title: String, val content: String)

val notes = database.collection<Note>("notes")

// Insert
notes.insert("1", Note("1", "Hello", "KoreDB is fast!"))

// Observe
notes.observeById("1").collect { note ->
    println("Note updated: $note")
}
```

### AI Vector Similarity Search (HNSW)
```kotlin
val vectors = database.vectorCollection("embeddings")

// Insert (Returns immediately, indexing happens in background)
vectors.insert("vec1", floatArrayOf(0.1f, 0.5f, 0.9f))

// Search for top 5 similar items (Uses HNSW Index)
val results = vectors.search(queryVector = floatArrayOf(0.1f, 0.5f, 0.9f), limit = 5)

results.forEach { (id, score) ->
    println("Found $id with similarity score: $score")
}
```

### Graph Traversals (Kotlin DSL)
```kotlin
val graph = database.graph()

// Find friends of friends who live in Tokyo
val tokyoFriends = graph.query {
    startingWith("Person", "city", "Tokyo")
    outbound("KNOWS", hops = 2)
}.toNodeList()

// Atomic graph updates
graph.transaction {
    putNode(Node(id = "user1", labels = setOf("Person"), properties = mapOf("name" to "Alice")))
    putEdge(Edge(sourceId = "user1", targetId = "user2", type = "FOLLOWS"))
}
```

### Graph Algorithms
```kotlin
// Calculate PageRank to find influential nodes
val influenceScores = GraphAlgorithms.pageRank(graph, seedNodes = allUsers, edgeType = "FOLLOWS")

// Find the shortest path using Dijkstra
val path = GraphAlgorithms.shortestPathDijkstra(
    storage = graph, 
    startNodeId = "userA", 
    endNodeId = "userB", 
    edgeType = "KNOWS"
)
```

---

## 🛠️ Architecture & Data Lifecycle

KoreDB follows the classic LSM-Tree pattern used by industrial databases like **Bigtable**, **Cassandra**, and **RocksDB**.

### 📝 The Write Path ($O(1)$ Complexity)
When you save data, KoreDB performs two actions simultaneously:
1.  **CommitLog (WAL):** Appends the change to a sequential log on disk. This is extremely fast and ensures your data isn't lost if the app crashes.
2.  **MemTable:** Updates an in-memory sorted tree. Your write is now "complete" and available for reading instantly.

### 📖 The Read Path (Multi-Tiered Lookup)
To find a record, KoreDB searches in this order:
1.  **MemTable:** Check the fastest tier (RAM) first.
2.  **Bloom Filter:** For disk-based files, KoreDB uses a probabilistic filter to check if a key *actually* exists before opening the file, avoiding 99% of unnecessary disk I/O.
3.  **SSTables (Disk):** If found in the filter, it performs a binary search on the disk file using a **Sparse Index** to locate the exact record.

### 🤖 HNSW Background Hydration
Unlike standard in-memory vector stores that require a "warm-up" period, KoreDB uses an asynchronous hydration model:
*   **Zero-Block Startup**: The database opens instantly. HNSW reconstruction begins in a low-priority background thread.
*   **Hybrid Search**: While hydrating, KoreDB automatically falls back to an optimized **Flat Scan** for data not yet in the graph.
*   **Eventual Max Speed**: Once hydration completes (usually <2s for 25k vectors), searches transition to sub-millisecond HNSW navigation.

---

## 📊 KoreDB vs Room: Real-World Benchmarks

Benchmarks were conducted using `KoreFurtherBenchmark.kt` on Android.

### 🚀 Write & Insert Performance
KoreDB's LSM-tree architecture shines here, appending data sequentially rather than updating B-Tree pages in place.

| Operation | KoreDB (LSM) | Room (SQLite) | Speedup |
| :--- | :--- | :--- | :--- |
| **Single Write (1k ops)** | **539 ms** | 1,176 ms | **2.2x** |
| **Bulk Insert (50k items)** | **538 ms** | 824 ms | **1.5x** |
| **Graph Build (1k Nodes, 5k Edges)** | **496 ms** | 12,813 ms | **25.8x** |
| **Random Updates** | **88 ms** | 315 ms | **3.6x** |

### ⚡ Read & Lookup Latency
KoreDB is optimized for key-based retrieval, using Bloom Filters to instantly negate non-existent keys.

| Operation | KoreDB | Room | Speedup |
| :--- | :--- | :--- | :--- |
| **Cold Start (Open DB)** | **4 ms** | 66 ms | **16.5x** |
| **Negative Lookup (Bloom Filter)** | **5 ms** | 1,182 ms | **236x** |
| **Point Read (10k ops)** | **250 ms** | 10,215 ms | **40x** |
| **Parallel Reads (8 Threads)** | **1,119 ms** | 7,003 ms | **6.2x** |

### 🤖 AI & Graph Capabilities
KoreDB provides native graph algorithms and vector search without external plugins.

| Operation | KoreDB | Room | Notes |
| :--- | :--- | :--- | :--- |
| **Vector Insert (25k vectors)** | **799 ms** | **475 ms** | Nitro Parallel Path. |
| **Vector Search (25k vectors)** | **47 ms** | 18.5 s | **393x Faster (HNSW Index)** |
| **Graph 2-Hop (IDs only)** | 19 ms | **3 ms** | Room's C++ SQL engine is highly optimized for joins. |
| **Graph 2-Hop (Full Objects)** | **140 ms** | - | KoreDB avoids overhead until the final result. |
| **PageRank (500 nodes)** | **242 ms** | - | Native Algorithm Support. |
| **Dijkstra Shortest Path** | **143 ms** | - | Pathfinding on 1k nodes/5k edges. |

### ⚠️ Trade-offs
No database is perfect. Room's B-Tree architecture currently outperforms KoreDB's LSM-tree in range scans and full table iterations.

| Operation | KoreDB | Room | Why? |
| :--- | :--- | :--- | :--- |
| **Sequential Scan (100k items)** | 983 ms | **402 ms** | B-Trees have better locality for linear scans. |
| **Prefix Scan** | 18.1 s | **7.9 s** | Iterating merged LSM segments is more expensive. |

---

## 📜 License

```text
Copyright 2026 KoreDB Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

## 🤝 Contributing

We welcome contributions! Please feel free to submit a Pull Request or open an issue on our [GitHub repository](https://github.com/pankajrai/KoreDB).

Made with ❤️ by Pankaj Rai.
