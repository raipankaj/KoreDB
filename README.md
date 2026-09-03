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

## 📚 Documentation & Guides

Explore the comprehensive documentation portal in the [`docs/`](docs/index.md) folder:

*   🌐 **[Documentation Portal (HTML)](docs/index.html)** — Interactive, searchable documentation website.
*   🏛️ **[LSM Engine Architecture](docs/architecture.md)** — WAL replay, MemTable, SSTables, Bloom filters, and compaction.
*   📄 **[Document Collections & Queries](docs/document_engine.md)** — CBOR serialization, secondary indices, numeric ranges, BM25 text search.
*   ⚡ **[Vector Engine & HNSW](docs/vector_engine.md)** — HNSW indexing, SQ8 quantization, off-heap mmap, metadata filtering.
*   🕸️ **[Property Graph Engine](docs/graph_engine.md)** — Bidirectional graphs, cascading deletes, BFS, Dijkstra, PageRank.
*   🧬 **[Unified Graph RAG Bridge](docs/hybrid_graph_rag.md)** — Hybrid semantic + graph traversals + Reciprocal Rank Fusion (RRF).
*   🔒 **[Transactions & Snapshot Isolation](docs/transactions_mvcc.md)** — ACID MVCC, First-Committer-Wins conflict resolution.
*   🛡️ **[Enterprise Capabilities](docs/enterprise_features.md)** — Change Data Capture (CDC), AES-GCM-256 encryption, LZ4 compression.
*   🚀 **[Android Feature & Decision Guide](docs/android_features_guide.md)** — Jetpack Compose, WorkManager, Android Keystore, memory trim.
*   🔄 **[Room to KoreDB Migration Guide](docs/migration_from_room.md)** — Step-by-step Room DAO conversion & zero-downtime migration.
*   🛡️ **[Production Readiness Checklist](docs/production_checklist.md)** — Architecture checklist, R8 rules, and deployment advice.
*   📊 **[Head-to-Head Benchmarks](docs/benchmarks.md)** — Real hardware benchmark results on **Google Pixel 7 Pro** vs Room/SQLite.
*   📱 **[Complete Sample Project](docs/sample_codebase.md)** — Production-grade Kotlin Android architecture (Entities, Repository, ViewModel).

---

## ✨ Features

### 📦 Collection Engine
*   **⚡ Blazing Performance:** LSM architecture offers $O(1)$ write performance with a "Nitro" parallel serialization path.
*   **🚀 Order-Preserving Numeric Byte Range LSM Pushdown:** Binary search ranges (`whereBetween`, `whereGt`, `whereLt`) evaluated directly inside the LSM storage tier — **5.7× faster** than in-memory filtering.
*   **🔍 Okapi BM25 Full-Text Search:** Sub-millisecond keyword inverted indexing with Robertson-Spärck Jones IDF, term frequency saturation, and document length normalization.
*   **🔍 Query DSL:** Range queries, multi-predicate filtering, sorting, limit/offset pagination.
*   **📊 Aggregation:** Built-in `count`, `sum`, `avg`, `min`, `max` — no SQL required.
*   **✏️ Partial Updates:** Modify individual fields without rewriting the entire document.
*   **🔗 Secondary Indexes:** O(log N) lookups with reverse-pointer staleness detection.
*   **📡 Reactive Flows:** `observeById()` and `observeAll()` for real-time UI updates.

### 🤖 Vector Engine
*   **🧠 HNSW Index:** Sub-millisecond Approximate Nearest Neighbor search with RNG pruning heuristic.
*   **⚡ 16-Lane SIMD Unrolled Kernels:** Dot product, Euclidean, Manhattan, and Cosine vector operations optimized for ARM NEON and AVX.
*   **📦 Product Quantization (PQ 32×):** 32× vector compression with Asymmetric Distance Computation (ADC) executing **6.3M distance evaluations/sec**.
*   **🧬 Hybrid Search:** Pre-filtered HNSW traversal with 10 metadata operators (`eq`, `gt`, `lt`, `inList`, `contains`...).
*   **🔀 Reciprocal Rank Fusion (RRF):** Blends BM25 keyword rankings with dense vector similarity for state-of-the-art search precision.
*   **📐 4 Distance Metrics:** Cosine, Euclidean, Inner Product, Manhattan — all SIMD-friendly with 16-lane loop unrolling.
*   **📦 Scalar Quantization (SQ8):** 4x memory reduction with <1% recall loss.
*   **💾 Memory-Mapped (mmap) Indexing:** Near-zero memory footprint during read/search operations via direct file-to-buffer mapping.
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
*   **No other database** — embedded or cloud — offers combined graph traversal + vector similarity + BM25 keyword search in a single query.
*   **🔀 BM25 + Semantic Hybrid Search (RRF):** Fuses keyword inverted search and HNSW vector search with zero glue code.
*   **🧠 Cost-Based Adaptive Query Planner:** Automatically chooses between Graph-First and Vector-First execution based on cost estimation.
*   **🔄 Dynamic Over-Fetching:** Solves recall drops by iteratively expanding $k$ ($2x \to 4x$) until requested limit is satisfied.
*   **🔍 EXPLAIN Query Profiling:** Inspect query strategies, estimated costs, predicate selectivities, and execution latencies.
*   **Vector-First:** Find similar vectors → filter by graph structure.
*   **Graph-First:** Traverse relationships → rerank by vector similarity.

### 🏗️ Core Engine & Production Hardening
*   **🔒 True MVCC ACID Engine:** Multi-Version Concurrency Control with Snapshot Isolation, optimistic conflict detection, and **30,800+ committed ACID tx/sec**.
*   **🗜️ LZ4 High-Speed Block Compression:** Zero-dependency pure-Kotlin LZ4 codec delivering **1.05 GB/s** decompression throughput.
*   **⚡ 2-Tier Block Cache:** Sub-microsecond LRU block cache directly integrated into SSTable readers.
*   **🛡️ Multi-Process File Locking:** Exclusive OS-level `FileLock` (`kore.lock`) preventing concurrent process corruption (`DatabaseLockedException`).
*   **🔄 Schema Versioning & Migrations:** Built-in `targetSchemaVersion` and `onMigrate` hooks for safe upgrades across app releases.
*   **📱 Android LMK & Memory Trim:** Integrates with `ComponentCallbacks2` to automatically evict block caches on memory pressure (`TRIM_MEMORY_RUNNING_CRITICAL`).
*   **🛡️ Torn-Write Auto-Quarantine:** Corrupted SSTable segments from sudden battery pull are automatically isolated to `.corrupt` without failing startup.
*   **🏗️ Pure Kotlin:** 100% Kotlin with Zero JNI overhead. No `sqlite3.so` bloat.
*   **🛡️ Crash Resilient:** Write-Ahead Logging (WAL) with CRC32 checksums and crash replay recovery.

---

## 🚀 Quick Start

### 1. Installation

```kotlin
dependencies {
    implementation("io.github.raipankaj:koredb:0.2.0")
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

### 🔍 Okapi BM25 Full-Text Search (Embedded Inverted Index)
KoreDB includes a zero-dependency, Lucene-grade full-text search engine powered by Okapi BM25 with Robertson-Spärck Jones IDF, term frequency saturation ($k_1 = 1.2$), and document length normalization ($b = 0.75$).

```kotlin
// 1. Enable full-text search on specific fields (Opt-in)
products.searchableFields({ it.name }, { it.category })

// 2. Query keywords with sub-millisecond inverted index lookups
val matches = products.searchBM25("wireless noise-cancelling headphones", limit = 10)

matches.forEach { (product, bm25Score) ->
    println("${product.name} (BM25 Score: $bm25Score)")
}
```

### 📦 Collection Operations Reference

| Operation | Description |
| :--- | :--- |
| `insert(doc)` | Inserts with an auto-generated unique ID. |
| `insert(id, doc, ttlSeconds)` | Inserts with optional Time-to-Live auto-expiration. |
| `insertBatch(map)` | Bulk saves in one atomic transaction. |
| `delete(id)` | Tombstone-based deletion. |
| `deleteBatch(ids)` | Atomic batch deletion of multiple IDs and indices. |
| `deleteAll()` | Wipes all documents and indices in the collection. |
| `db.dropCollection(name)` | Drops collection and purges all indexes & caches. |
| `getById(id)` | Retrieves document (returns `null` if expired or missing). |
| `getByIdRange(start, end)` | Range scan [start, end). |
| `getByIdPrefix(prefix)` | Prefix scan. |
| `getByIndex(name, val)` | Secondary index lookup. |
| `count()` | Returns valid document count. |
| `rebuildIndexes()` | Backfills secondary & numeric indexes on existing data. |
| `query().asFlow()` | Reactive Jetpack Compose Flow for filtered queries. |
| `searchableFields(...)` | Enables Okapi BM25 full-text indexing on fields. |
| `searchBM25(query, limit)` | Executes a sub-millisecond BM25 keyword search. |
| `KoreDatabase.inMemory()` | Ephemeral in-memory database for unit tests. |
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

## 🌉 Unified Graph + Vector Bridge (GraphRAG Native & Adaptive Planner)

**KoreDB's killer feature** — no other database offers this. It gives you everything needed out-of-the-box to build **GraphRAG (Graph Retrieval-Augmented Generation)**, with a built-in **cost-based adaptive query optimizer**.

```kotlin
val bridge = database.graphVectorBridge(vectorCollection)

// 🧠 1. Adaptive Hybrid Search (Planner decides Graph-First vs Vector-First):
// Automatically routes small candidate pools to Graph-First and large searches to Vector-First with dynamic over-fetching.
val results = bridge.searchAdaptive(
    query = promptEmbedding,
    targetLimit = 10,
    graphPredicate = { nodeId -> graph.getNode(nodeId)?.labels?.contains("Verified") == true }
)

// 🔍 2. Query Plan Inspection (EXPLAIN):
val plan = bridge.explain(
    query = promptEmbedding,
    targetLimit = 10,
    predicateTag = "verified_products",
    graphPredicate = { ... }
)
println(plan.explainString())
/*
=== KoreDB Hybrid Query Execution Plan ===
Strategy: VECTOR_FIRST_ADAPTIVE
Estimated Costs:
  • Graph-First Cost:  125.00
  • Vector-First Cost: 18.50
Selectivity Estimate:  25.0%
Adaptive Planning:
  • Initial K:         40
  • Search Iterations: 1
Execution Metrics:
  • Execution Time:    2ms
  • Vectors Scored:    40
  • Nodes Inspected:   10
  • Final Results:     10
==========================================
*/

// 🔄 3. Adaptive Vector-First Search (Dynamic Over-fetching):
// Automatically expands k (2x -> 4x) until requested limit is satisfied, eliminating recall loss.
val adaptiveResults = bridge.adaptiveVectorSearch(
    query = promptEmbedding,
    targetLimit = 10,
    predicateTag = "brand_filter"
) { productId ->
    graph.getOutboundTargetIds(productId, "MADE_BY").any { it in userFollowedBrands }
}

// 🔀 4. BM25 + Semantic Hybrid Search (Reciprocal Rank Fusion / RRF):
// Blends exact keyword matching with dense vector similarity with zero external dependencies.
val hybridResults = bridge.searchHybrid(
    collection = products,
    queryText = "wireless noise-cancelling headphones",
    queryVector = promptEmbedding,
    limit = 10,
    bm25Weight = 1.0f,
    vectorWeight = 1.0f
)
hybridResults.forEach { (product, rrfScore) ->
    println("${product.name} (RRF Score: $rrfScore)")
}

// 🌟 5. Full GraphRAG Pipeline (1-liner)
// Finds semantic seeds -> traverses graph for context -> reranks for LLM
val graphRagContext = bridge.graphRAGQuery(
    query = promptEmbedding,
    initialLimit = 5,           // 1. Vector Search: Find top 5 relevant seed nodes
    edgeType = "DEPENDS_ON",    // 2. Graph Expansion: Traverse specific relationships
    maxHops = 2,                // 3. Graph Expansion: Pull context up to 2 levels deep
    finalLimit = 10             // 4. Rerank: Return the 10 best contextual nodes
)

// 🔍 6. Direct Vector-First (Fixed k):
val results = bridge.vectorSearch(queryEmbedding, limit = 50)
    .filterByGraph { productId ->
        graph.getOutboundTargetIds(productId, "MADE_BY")
            .any { it in userFollowedBrands }
    }

// 🕸️ 7. Direct Graph-First: Traverse relationships, then rank by similarity
val ranked = bridge.graphTraversal("user_123", "PURCHASED", hops = 2)
    .rerankByVector(queryEmbedding)
    .take(10)
```

---

## 💾 Snapshot Backup & Restore

Create consistent, point-in-time database snapshots with CRC32 integrity verification:

```kotlin
val backupDir = File(context.filesDir, "backups/snapshot_v1")

// 1. Create Snapshot Backup
val metadata = database.createBackup(backupDir)
println("Backup created: ${metadata.sstableFiles.size} SSTables, ${metadata.totalSizeBytes} bytes")

// 2. Restore from Snapshot
// Verifies CRC32 checksums before safely replacing database state.
val success = database.restoreFromBackup(backupDir)
```

---

## 📊 Real-Time Observability & Metrics

Inspect engine throughput, memory utilization, and compaction health in real time:

```kotlin
val metrics = database.getMetrics()
println("""
    Reads:             ${metrics.readCount}
    Writes:            ${metrics.writeCount}
    Compactions:       ${metrics.compactionCount}
    MemTable RAM:      ${metrics.memTableSizeBytes} bytes
    Active SSTables:   ${metrics.activeSSTables}
    Total Disk Usage:  ${metrics.totalDiskUsageBytes} bytes
""".trimIndent())
```

---

## 🪵 Structured & Pluggable Logging

Configure log levels or integrate custom logging frameworks (e.g. Android Logcat, Timber, SLF4J):

```kotlin
// Set log level
KoreLogger.level = KoreLogger.LogLevel.INFO

// Attach custom logger backend
KoreLogger.logger = object : KoreLogger {
    override fun info(message: String) = Log.i("KoreDB", message)
    override fun error(message: String, throwable: Throwable?) = Log.e("KoreDB", message, throwable)
    override fun warn(message: String) = Log.w("KoreDB", message)
    override fun debug(message: String) = Log.d("KoreDB", message)
}
```

---

## 🔐 Hardware-Accelerated AES-256-GCM Encryption at Rest

Secure stored data using 256-bit AES-GCM encryption with per-record random 12-byte IVs and authenticated Additional Authenticated Data (AAD) bound to the key:

```kotlin
// 1. Generate or load 256-bit symmetric key (e.g. from Android KeyStore)
val key = AesGcmCrypto.generateKey()
val crypto = AesGcmCrypto(key)

// 2. Pass crypto instance to database
val database = KoreDatabase(
    directory = File(context.filesDir, "secure_db"),
    crypto = crypto
)

// All document inserts, updates, and reads are transparently encrypted/decrypted
val secrets = database.collection<SecretNote>("secrets")
secrets.insert("note_1", SecretNote("Super secret token"))
```

---

## 🗜️ Pluggable SSTable Block Compression

Reduce on-disk storage footprint by over 50% using built-in or custom compression codecs:

```kotlin
// Use Deflate or Gzip compression
val database = KoreDatabase(
    directory = File(context.filesDir, "compressed_db"),
    compressionCodec = DeflateCompressionCodec() // or GzipCompressionCodec
)
```

---

## 📤 Data Export & Import Engine (JSON & CSV)

Backup and migrate collection datasets to JSON or CSV:

```kotlin
val users = database.collection<User>("users")

// 1. Export collection to JSON
val jsonFile = File(context.filesDir, "users_backup.json")
val exportStats = users.exportToJson(jsonFile)
println("Exported ${exportStats.totalRecords} records (${exportStats.totalBytes} bytes)")

// 2. Import collection from JSON
val importStats = users.importFromJson(jsonFile)

// 3. Export to CSV
val csvFile = File(context.filesDir, "users.csv")
users.exportToCsv(
    outputFile = csvFile,
    headers = listOf("id", "name", "email"),
    rowMapper = { listOf(it.name, it.email) }
)

// 4. Import from CSV
users.importFromCsv(
    inputFile = csvFile,
    hasHeader = true,
    rowParser = { tokens -> Pair(tokens[0], User(tokens[0], tokens[1], tokens[2])) }
)
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

*(Benchmarks conducted on a connected **Pixel 7 Pro** device running `ComprehensiveBenchmark` comparing KoreDB's LSM/HNSW/Graph engines against a standard Room/SQLite implementation)*

### 🎯 Point Operations (5,000 Operations, 1-by-1)
| Operation | KoreDB (LSM) | Room (SQLite) | Winner | Speedup |
| :--- | :---: | :---: | :---: | :---: |
| **Single Writes** | **6,288 ms** | 19,525 ms | 🏆 KoreDB | **3.11x** |
| **Single Reads** | **60 ms** | 3,935 ms | 🏆 KoreDB | **65.58x** |
| **Negative Lookups** | **70 ms** | 4,246 ms | 🏆 KoreDB | **60.66x** |

### 🚀 Massive Bulk Operations (50,000 Records / 10,000 Updates)
| Operation | KoreDB (LSM) | Room (SQLite) | Winner | Speedup |
| :--- | :---: | :---: | :---: | :---: |
| **Bulk Insert (50K)** | **408 ms** | 804 ms | 🏆 KoreDB | **1.97x** |
| **Random Updates (10K)** | **113 ms** | 260 ms | 🏆 KoreDB | **2.30x** |

### 🤖 Vector Similarity (15,000 Vectors, 384-dim)
| Operation | KoreDB (HNSW) | Room (Flat Scan) | Winner | Speedup |
| :--- | :---: | :---: | :---: | :---: |
| **Vector Insert** | **462 ms** | 3,086 ms | 🏆 KoreDB | **6.68x** |
| **Vector Search (50 queries)** | **179 ms** | 43,234 ms | 🏆 KoreDB | **241.53x** |
| **HNSW Hydration** | **148 ms** | N/A | KoreDB | Loads instantly from disk |

### 🕸️ Graph & Relational Traversal (2,000 Nodes, 10,000 Edges)
| Operation | KoreDB (Graph) | Room (SQLite Join) | Winner | Speedup / Note |
| :--- | :---: | :---: | :---: | :--- |
| **Graph Build** | **432 ms** | 31,167 ms | 🏆 KoreDB | **72.15x** |
| **2-Hop Traversal (100x)** | 211 ms | **74 ms** | 🏆 Room | **0.35x** (Relational Join) |
| **PageRank (5iter, 500 nodes)** | **268 ms** | N/A | KoreDB | Native Graph Engine |
| **Dijkstra Shortest Path** | **189 ms** | N/A | KoreDB | Native Graph Engine |

### 📖 Prefix & Range Queries (50,000 Records)
| Operation | KoreDB (LSM) | Room (SQLite) | Winner | Speedup |
| :--- | :---: | :---: | :---: | :---: |
| **Prefix Scan (50x)** | **3,587 ms** | 3,682 ms | 🏆 KoreDB | **1.03x** |
| **Range Query (500 items, 50x)** | **90 ms** | 202 ms | 🏆 KoreDB | **2.24x** |
| **Large Range (50KB/rec, 5x)** | **220 ms** | 1,542 ms | 🏆 KoreDB | **7.01x** |

### 🔍 BM25 & Semantic Hybrid Search (2,000 Documents, 128-dim)
| Operation | KoreDB (BM25 + RRF) | Baseline (Linear String Scan) | Winner | Speedup / Impact |
| :--- | :---: | :---: | :---: | :---: |
| **Pure Keyword Search (Top-10)** | **0.35 ms** | 17.61 ms | 🏆 KoreDB | **~50x - 115x faster** |
| **Pure Vector Search (Top-10)** | **0.04 ms** | 0.04 ms | 🏆 KoreDB | Sub-millisecond HNSW |
| **Hybrid Search (BM25 + RRF)** | **1.24 ms** | 21.34 ms | 🏆 KoreDB | **~17x faster** + True Relevance Ranking |
| **Document Edit & Re-query** | **1.09 ms** | 1.20 ms | 🏆 KoreDB | Instant Stale Term Purging |

### ⚠️ Performance Trade-offs
| Operation | KoreDB (LSM) | Room (SQLite B-Tree) | Winner | Why? |
| :--- | :---: | :---: | :---: | :--- |
| **Full Sequential Scan** | 180 ms | **168 ms** | 🏆 Room | B-Trees have better data locality for linear disk scans. |
| **2-Hop Traversal (100x)** | 211 ms | **74 ms** | 🏆 Room | SQLite's optimizer is highly mature for direct indices joins. |

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
