# KoreDB 🚀

<p align="center">
  <img src="logo.png" alt="KoreDB Logo" width="344">
  <br>
  <b>The AI-Native, Multi-Model Database Engine for Modern Android.</b>
  <br>
  <i>Documents + Vectors + Graphs — One Engine. Zero Dependencies.</i>
</p>

KoreDB is a pure Kotlin, embedded database engine built from the ground up using a **Log-Structured Merge-tree (LSM)** architecture. Unlike SQLite (designed for spinning disks in 2000), KoreDB is optimized for modern flash storage, high-concurrency Coroutines, on-device AI, and complex relationship mapping.

**KoreDB is the only embedded database that unifies all three data paradigms in a single library:**

| Pillar | What It Does | Competitors |
| :--- | :--- | :--- |
| 📦 **Document Store** | Typed collections, secondary indexes, query DSL, aggregation | Room, Realm |
| 🤖 **Vector Database** | HNSW ANN search, hybrid filtering, scalar quantization | ChromaDB, Qdrant |
| 🕸️ **Graph Database** | Property graphs, A* pathfinding, community detection, DOT export | Neo4j |

---

## ✨ Features

### 📦 Collection Engine
*   **⚡ Blazing Performance:** LSM architecture offers $O(1)$ write performance with a "Nitro" parallel serialization path.
*   **🔍 Query DSL:** Range queries, multi-predicate filtering, sorting, limit/offset pagination.
*   **📊 Aggregation:** Built-in `count`, `sum`, `avg`, `min`, `max` — no SQL required.
*   **✏️ Partial Updates:** Modify individual fields without rewriting the entire document.
*   **🔗 Secondary Indexes:** O(log N) lookups with reverse-pointer staleness detection.
*   **📡 Reactive Flows:** `observeById()` and `observeAll()` for real-time UI updates.

### 🤖 Vector Engine
*   **🧠 HNSW Index:** Sub-millisecond Approximate Nearest Neighbor search with RNG pruning heuristic.
*   **🧬 Hybrid Search:** Pre-filtered HNSW traversal with 10 metadata operators (`eq`, `gt`, `lt`, `inList`, `contains`...).
*   **📐 4 Distance Metrics:** Cosine, Euclidean, Inner Product, Manhattan — all SIMD-friendly with 4x loop unrolling.
*   **📦 Scalar Quantization (SQ8):** 4x memory reduction with <1% recall loss.
*   **🗑️ Delete & Update:** Soft-delete with tombstones + background compaction.
*   **📚 Multi-Vector:** Store multiple named vector fields per document.
*   **🏗️ Namespace Isolation:** Logical multi-tenancy with separate key prefixes and index files.

### 🕸️ Graph Engine
*   **🕸️ Property Graphs:** Nodes with labels + properties, Edges with types + properties.
*   **🗑️ Cascading Delete:** `deleteNode()` removes node + ALL connected edges + ALL indexes atomically.
*   **📦 Batch Operations:** `putNodes()` / `putEdges()` for high-throughput graph construction.
*   **🧭 A\* Pathfinding:** Heuristic-guided shortest path with custom cost functions.
*   **👥 Community Detection:** Louvain-inspired algorithm for social network analysis.
*   **🔗 Variable-Length Paths:** Traverse 2-N hops with automatic cycle detection.
*   **📊 Analytics:** PageRank, degree centrality, connected components.
*   **📤 Export:** DOT (Graphviz) and GraphML for visualization in Gephi/yEd/Cytoscape.

### 🌉 Unified Graph + Vector Bridge (Unique!)
*   **No other database** — embedded or cloud — offers combined graph traversal + vector similarity in a single query.
*   **Vector-First:** Find similar vectors → filter by graph structure.
*   **Graph-First:** Traverse relationships → rerank by vector similarity.

### 🏗️ Core Engine
*   **🏗️ Pure Kotlin:** 100% Kotlin with Zero JNI overhead. No `sqlite3.so` bloat.
*   **🔗 Coroutine First:** Non-blocking I/O with `Flow`, background indexing and automatic hydration.
*   **🛡️ Crash Resilient:** Write-Ahead Logging (WAL) with CRC32 checksums.
*   **🔍 Optimized Reads:** Bloom Filters, Sparse Indexing, Object Cache (65K entries).
*   **📦 Lightweight:** Minimal footprint, perfect for mobile apps.

---

## 🚀 Quick Start

### 1. Installation

```kotlin
dependencies {
    implementation("io.github.raipankaj:koredb:0.0.6")
}
```

### 2. Setup Serialization

```kotlin
// Project build.gradle.kts
plugins {
    kotlin("plugin.serialization") version "2.0.21"
}

// Module build.gradle.kts
dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
}
```

### 3. Initialize Database

```kotlin
class MyApplication : Application() {
    lateinit var database: KoreDatabase

    override fun onCreate() {
        super.onCreate()
        database = KoreAndroid.create(this, "my_notes_db")
    }
}
```

---

## ⚡ App Startup Optimization

### 1. Lazy Initialization (Recommended)
```kotlin
class MyApplication : Application() {
    val database: KoreDatabase by lazy {
        KoreAndroid.create(this, "my_db")
    }
}
```

### 2. Background Warmup
```kotlin
override fun onCreate() {
    super.onCreate()
    CoroutineScope(Dispatchers.IO).launch {
        database.engine // Triggers lazy init
    }
}
```

### 3. Asynchronous Vector Hydration
KoreDB already handles HNSW graph hydration in a low-priority background thread, ensuring instant app opens even with tens of thousands of vectors.

---

## 📦 Pillar 1: Document Collections

### Define Your Data Model
```kotlin
@Serializable
data class Product(
    val id: String,
    val name: String,
    val category: String,
    val price: Double,
    val inStock: Boolean = true
)
```

### CRUD Operations
```kotlin
val products = database.collection<Product>("products")

// Insert
products.insert("p1", Product("p1", "Headphones", "electronics", 99.99))

// Bulk Insert
products.insertBatch(mapOf(
    "p2" to Product("p2", "Shoes", "clothing", 149.99),
    "p3" to Product("p3", "Book", "books", 19.99)
))

// Lookups
val product = products.getById("p1")
val rangeProducts = products.getByIdRange("p1", "p3")
val prefixProducts = products.getByIdPrefix("p")

// Delete
products.delete("p1")
products.deleteAll()
```

### Secondary Indexing
```kotlin
products.createIndex("category") { it.category }
val electronics = products.getByIndex("category", "electronics")
```

### Query DSL with Range Queries & Aggregation
```kotlin
// Register properties for the query engine
products.registerProperty("price") { it.price.toString() }
products.registerProperty("category") { it.category }

// Range query with sorting
val cheapShoes = products.query()
    .where("category") { it == "clothing" }
    .where("price") { it.toDouble() < 100 }
    .sortBy("price") { it.toDouble() }
    .limit(20)
    .execute()

// Aggregation (count, sum, avg, min, max)
val stats = products.query()
    .where("category") { it == "electronics" }
    .aggregate {
        count()
        sum("price") { it.toDouble() }
        avg("price") { it.toDouble() }
        min("price") { it.toDouble() }
        max("price") { it.toDouble() }
    }
println("Count: ${stats.getCount()}, Avg: ${stats.getAvg("price")}")
```

### Partial Document Updates
```kotlin
// Update specific fields without rewriting the entire document
products.updateFields("p1") { it.copy(price = 79.99, inStock = false) }

// Batch partial updates
products.updateFieldsBatch(listOf("p1", "p2")) { it.copy(inStock = false) }
```

### Reactive Flows
```kotlin
products.observeById("p1").collect { product -> updateUi(product) }
products.observeAll().collect { allProducts -> updateList(allProducts) }
```

### 📦 Collection Operations Reference

| Operation | Description |
| :--- | :--- |
| `insert(id, doc)` | Inserts or updates a document. |
| `insertBatch(map)` | Bulk saves in one transaction. |
| `getById(id)` | Retrieves by unique ID. |
| `getByIdRange(start, end)` | Range scan [start, end). |
| `getByIdPrefix(prefix)` | Prefix scan. |
| `getByIndex(name, val)` | Secondary index lookup. |
| `delete(id)` | Tombstone-based deletion. |
| `deleteAll()` | Wipes the collection. |
| `count()` | Returns document count. |
| `query()` | Creates a query builder (filter, sort, limit, aggregate). |
| `updateFields(id, transform)` | Partial document update. |
| `updateFieldsBatch(ids, transform)` | Batch partial update. |
| `registerProperty(name, extractor)` | Registers a property for queries. |
| `observeById(id)` | Reactive `Flow<T?>` for a single record. |
| `observeAll()` | Reactive `Flow<List<T>>` for the collection. |
| `createIndex(name, extractor)` | Registers a secondary index. |

---

## 🤖 Pillar 2: Vector Database

### Create a Vector Collection
```kotlin
val vectors = database.vectorCollection("embeddings") {
    dimensions = 768
    metric = DistanceMetric.COSINE          // or EUCLIDEAN, INNER_PRODUCT, MANHATTAN
    quantization = true                      // Enable SQ8 for 4x memory savings
    maxConnections = 16                      // HNSW M parameter
    efConstruction = 200                     // Build-time quality
    efSearch = 50                            // Search-time quality
    namespace = "user_123"                   // Multi-tenant isolation
}
```

### Insert Vectors with Metadata
```kotlin
vectors.insert("doc_1", embedding, metadata = mapOf(
    "category" to "electronics",
    "price" to 99.99,
    "brand" to "Sony"
))

// Batch insert
vectors.insertBatch(
    vectors = mapOf("doc_1" to vec1, "doc_2" to vec2),
    metadataMap = mapOf(
        "doc_1" to mapOf("category" to "electronics"),
        "doc_2" to mapOf("category" to "clothing")
    )
)
```

### Hybrid Search (Vector + Metadata Filtering)
```kotlin
val results = vectors.search(queryVector, limit = 10) {
    where("category", eq("electronics"))
    where("price", lte(999.0))
    where("brand", inList("Sony", "Apple"))
}
```

#### Hybrid Search Operators (Pre-filtering)
| Operator | Description | Example |
| :--- | :--- | :--- |
| `eq(v)` | Equals | `where("status", eq("active"))` |
| `neq(v)` | Not Equals | `where("type", neq("internal"))` |
| `gt(v)` / `gte(v)` | Greater than (or equal) | `where("price", gte(49.99))` |
| `lt(v)` / `lte(v)` | Less than (or equal) | `where("rating", lt(4.0))` |
| `inList(v1, v2)` | Value is in the list | `where("tags", inList("AI", "Mobile"))` |
| `notInList(...)` | Value is not in the list | `where("category", notInList("hidden"))` |
| `contains(s)` | String contains substring | `where("name", contains("pro"))` |
| `exists()` | Field is not null | `where("metadata", exists())` |

### Multi-Vector Per Document
```kotlin
vectors.insertMultiVector("product_123",
    vectors = mapOf(
        "title" to titleEmbedding,
        "image" to imageEmbedding,
        "description" to descEmbedding
    ),
    metadata = mapOf("category" to "shoes")
)

// Search a specific vector field
val imageResults = vectors.searchField("image", imageQuery, limit = 10)
```

### Delete, Update & Maintenance
```kotlin
vectors.delete("doc_1")
vectors.deleteBatch(listOf("doc_1", "doc_2"))
vectors.update("doc_1", newVector, mapOf("price" to 79.99))
vectors.updateMetadata("doc_1", mapOf("price" to 59.99))  // No re-indexing

vectors.waitForIndexing()   // Block until HNSW build completes
vectors.compactIndex()      // Remove tombstoned nodes
println(vectors.stats())    // Index health monitoring
```

### 🤖 Vector Operations Reference

| Operation | Description |
| :--- | :--- |
| `insert(id, vector, metadata?)` | Insert with optional metadata. |
| `insertBatch(vectors, metadataMap?)` | High-throughput batch insert. |
| `insertMultiVector(id, vectors, metadata?)` | Multiple named vectors per doc. |
| `search(query, limit, filter?)` | HNSW search with metadata filtering. |
| `searchField(field, query, limit)` | Search a specific vector field. |
| `delete(id)` / `deleteBatch(ids)` | Soft-delete with tombstones. |
| `update(id, vector, metadata?)` | Update vector + metadata. |
| `updateMetadata(id, metadata)` | Metadata-only update (no re-index). |
| `getVector(id)` | Retrieve raw vector. |
| `getMetadata(id)` | Retrieve metadata. |
| `waitForIndexing()` | Block until HNSW is built. |
| `compactIndex()` | Remove deleted nodes physically. |
| `stats()` | Index health (nodes, edges, levels). |

---

## 🕸️ Pillar 3: Graph Database

### Nodes & Edges
```kotlin
val graph = database.graph()

// Single insert
graph.putNode(Node("u1", labels = setOf("User"), properties = mapOf("name" to "Alice", "city" to "Tokyo")))
graph.putEdge(Edge("u1", "u2", "FOLLOWS"))

// Batch insert (high-throughput)
graph.putNodes(listOf(node1, node2, node3))
graph.putEdges(listOf(edge1, edge2, edge3))

// Cascading delete (removes node + ALL edges + ALL indexes)
graph.deleteNode("u1")
```

### Transactions
```kotlin
graph.transaction {
    putNode(Node("u1", labels = setOf("User"), properties = mapOf("name" to "Alice")))
    putNode(Node("u2", labels = setOf("User"), properties = mapOf("name" to "Bob")))
    putEdge(Edge("u1", "u2", "FOLLOWS"))
}
```

### Query DSL
```kotlin
// Basic traversal
val friends = graph.query {
    startingWith("User", "name", "Alice")
    outbound("FOLLOWS")
}.toNodeList()

// Variable-length path (2 to 5 hops) with cycle detection
val extendedNetwork = graph.query {
    startingWith("alice")
    outboundRange("KNOWS", minHops = 2, maxHops = 5)
    hasProperty("city", "Tokyo")
    limit(10)
}.toNodeList()

// Count without materializing
val count = graph.query {
    startingWith("User", "city", "Tokyo")
    outbound("FOLLOWS")
}.count()
```

### Graph Algorithms
```kotlin
// Shortest path (Dijkstra)
val path = GraphAlgorithms.shortestPathDijkstra(graph, "NYC", "LA", "ROAD")

// A* pathfinding (with heuristic)
val smartPath = GraphAlgorithms.aStarPath(graph, "NYC", "LA", "ROAD") { nodeId, goalId ->
    haversineDistance(getCoords(nodeId), getCoords(goalId))
}

// Variable-length path traversal
val reachable = GraphAlgorithms.variableLengthPath(graph, "alice", "KNOWS", minHops = 2, maxHops = 5)

// Community detection (Louvain-inspired)
val communities = GraphAlgorithms.detectCommunities(graph, allNodeIds, "FOLLOWS")

// Connected components
val components = GraphAlgorithms.connectedComponents(graph, allNodeIds, "FOLLOWS")

// Centrality measures
val pageRanks = GraphAlgorithms.pageRank(graph, allNodeIds, "FOLLOWS")
val degreeCentrality = GraphAlgorithms.degreeCentrality(graph, allNodeIds, "FOLLOWS")
```

### Graph Export (Visualization)
```kotlin
// Export to DOT (Graphviz)
val dot = GraphExport.toDot(graph, nodeIds, "FOLLOWS")
File("social_graph.dot").writeText(dot)
// Then: dot -Tpng social_graph.dot -o social_graph.png

// Export to GraphML (Gephi, yEd, Cytoscape)
val graphml = GraphExport.toGraphML(graph, nodeIds, "FOLLOWS")
File("social_graph.graphml").writeText(graphml)
```

### 🕸️ Graph Operations Reference

| Operation | Description |
| :--- | :--- |
| `putNode(node)` | Saves a node with labels and properties. |
| `putNodes(nodes)` | Batch insert nodes atomically. |
| `getNode(id)` | Retrieves a node by ID. |
| `getNodesByLabel(label)` | Gets all nodes with a label. |
| `getNodesByProperty(label, key, value)` | Property-indexed lookup. |
| `deleteNode(id)` | Cascading delete (node + edges + indexes). |
| `putEdge(edge)` | Creates a bidirectional relationship. |
| `putEdges(edges)` | Batch insert edges atomically. |
| `removeEdge(src, type, tgt)` | Removes a specific edge. |
| `getOutboundEdges(id, type)` | Gets outgoing edges. |
| `getInboundEdges(id, type)` | Gets incoming edges. |
| `getAllOutboundEdges(id)` | Gets ALL outgoing edges (any type). |
| `getAllInboundEdges(id)` | Gets ALL incoming edges (any type). |
| `getOutboundEdgeTypes(id)` | Lists distinct outbound edge types. |
| `query { ... }` | Fluent DSL for graph traversal. |
| `transaction { ... }` | Atomic mutations. |

| Algorithm | Description |
| :--- | :--- |
| `bfs(storage, start, edgeType)` | Breadth-first traversal (lazy). |
| `dfs(storage, start, edgeType)` | Depth-first traversal (lazy). |
| `shortestPathDijkstra(...)` | Weighted shortest path. |
| `aStarPath(...)` | Heuristic-guided shortest path. |
| `variableLengthPath(...)` | Multi-hop traversal with cycle detection. |
| `detectCommunities(...)` | Louvain-inspired community detection. |
| `connectedComponents(...)` | Find connected components. |
| `pageRank(...)` | Centrality ranking. |
| `degreeCentrality(...)` | Normalized degree centrality. |

---

## 🌉 Unified Graph + Vector Bridge

**KoreDB's killer feature** — no other database offers this.

```kotlin
val bridge = database.graphVectorBridge(vectorCollection)

// Vector-First: Find similar, then filter by graph structure
val results = bridge.vectorSearch(queryEmbedding, limit = 50)
    .filterByGraph { productId ->
        graph.getOutboundTargetIds(productId, "MADE_BY")
            .any { it in userFollowedBrands }
    }

// Graph-First: Traverse relationships, then rank by similarity
val ranked = bridge.graphTraversal("user_123", "PURCHASED", hops = 2)
    .rerankByVector(queryEmbedding)
    .take(10)

// Property-based graph start → vector rerank
val results = bridge.graphQuery("Product", "category", "shoes")
    .rerankByVector(queryEmbedding)
    .take(10)
```

---

## ✅ Enterprise-Grade Reliability

KoreDB is rigorously tested to ensure stability, data integrity, and performance under extreme conditions:

*   **🛡️ Crash Recovery**: Validated against truncated WALs and power-loss scenarios.
*   **⚡ High Concurrency**: Thread-safety verified with 100+ concurrent writers and readers.
*   **🔄 Schema Evolution**: Adding/removing fields without migration scripts.
*   **🕸️ Graph Integrity**: Validated against complex topologies (Supernodes, Cycles, Self-Loops).
*   **🤖 Vector Robustness**: Handles zero vectors, dimensionality mismatches, and overflows.
*   **🔥 Stress Testing**: Stable with 100,000+ record bulk ingestion.

---

## 🛠️ Architecture & Data Lifecycle

KoreDB follows the classic LSM-Tree pattern used by **Bigtable**, **Cassandra**, and **RocksDB**.

### 📝 The Write Path ($O(1)$ Complexity)
1.  **CommitLog (WAL):** Appends to a sequential log on disk (crash-safe).
2.  **MemTable:** Updates in-memory sorted tree. Write is complete and readable instantly.

### 📖 The Read Path (Multi-Tiered Lookup)
1.  **Object Cache:** LRU cache (up to 65K objects) bypasses JSON deserialization.
2.  **MemTable:** Check RAM first.
3.  **Bloom Filter:** Probabilistic check avoids 99% of unnecessary disk I/O.
4.  **SSTables:** Binary search on disk files using Sparse Index.

### 🤖 HNSW Background Hydration
*   **Zero-Block Startup**: Database opens instantly. HNSW reconstruction in background.
*   **Hybrid Search**: Automatic flat scan fallback while hydrating.
*   **Eventual Max Speed**: Sub-millisecond HNSW navigation once complete.

---

## 📊 KoreDB vs Room: Real-World Benchmarks

*(Benchmarks conducted on an Android device comparing KoreDB's LSM/HNSW/Graph engines against a standard Room/SQLite implementation)*

### 📦 Pillar 1: Document Collection (5,000 Documents)
| Operation | KoreDB (LSM) | Room (SQLite) | Speedup |
| :--- | :--- | :--- | :--- |
| **Bulk Insert** | **94 ms** | 91 ms | 1x |
| **Point Read (x1000)** | **1 ms** | 677 ms | **677x** |
| **Index Lookup (x100)** | **140 ms** | 81 ms | 0.6x |
| **Delete (x500)** | **122 ms** | 1,080 ms | **8.8x** |
| **Query+Filter (x50)** | **375 ms** | 338 ms | 0.9x |
| **Aggregation (Count+Sum)** | **191 ms** | N/A | Native |
| **Partial Update (x500)** | **904 ms** | 1,406 ms | **1.5x** |

### 🤖 Pillar 2: Vector Engine (5,000 Vectors, 128-dim)
| Operation | KoreDB | Room | Speedup |
| :--- | :--- | :--- | :--- |
| **Insert** | **152 ms** | 349 ms | **2.3x** |
| **Search (top-10 × 50)** | **69 ms** | 3,011 ms | **43.6x** |
| **Hybrid Search (× 50)** | **156 ms** | 814 ms | **5.2x** |
| **Delete (× 500)** | **24 ms** | 1,180 ms | **49x** |
| **Update (× 500)** | **255 ms** | 639 ms | **2.5x** |
| **Metadata Update (× 1000)** | **508 ms** | 5,712 ms | **11.2x** |
| **SQ8 Quantized Search** | **47 ms** | N/A | 4x less RAM |

### 🕸️ Pillar 3: Graph Engine (500 Nodes, 2500 Edges)
| Operation | KoreDB | Room | Notes |
| :--- | :--- | :--- | :--- |
| **Node Lookup (x50)** | **1 ms** | N/A | |
| **Edge Query (x20)** | **2 ms** | N/A | |
| **Batch Edge Insert** | **136 ms** | 15,691 ms | **115x** |
| **1-Hop Traversal (x20)** | **0 ms** | N/A | |
| **2-4 Hop Path (x10)** | **7 ms** | N/A | |
| **Dijkstra Shortest Path** | **218 ms** | N/A | |
| **A* Pathfinding** | **225 ms** | N/A | |
| **PageRank** | **372 ms** | N/A | |
| **Community Detection** | **43 ms** | N/A | |
| **Cascading Delete (x50)** | **52 ms** | 120 ms | Room lacks edge cascade |

### ⚠️ Trade-offs
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

We welcome contributions! Please feel free to submit a Pull Request or open an issue on our [GitHub repository](https://github.com/raipankaj/KoreDB).

Made with ❤️ by Pankaj Rai.
