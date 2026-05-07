# KoreDB Documentation

Welcome to the official documentation for **KoreDB** — the AI-Native, Multi-Model Database Engine for Modern Android.

KoreDB is unique because it unifies three distinct data paradigms into a single, dependency-free Kotlin library, powered by an underlying Log-Structured Merge-tree (LSM) storage engine.

## Core Pillars

1. **[Document Collection Engine](api_collection.md)**: Store Kotlin `@Serializable` data classes. Supports O(1) writes, secondary indexing, range queries, and aggregations (sum, min, max, count).
2. **[Vector Database Engine](api_vector.md)**: Store high-dimensional embeddings. Supports sub-millisecond Approximate Nearest Neighbor (ANN) search via HNSW, scalar quantization (SQ8), and hybrid filtering.
3. **[Graph Database Engine](api_graph.md)**: Store Nodes and Edges. Supports variable-length traversals, A* pathfinding, PageRank, and Louvain community detection.
4. **[Unified Graph+Vector Bridge](api_bridge.md)**: Our killer feature. Combine graph traversals with vector similarity search in a single fluent query.

## Setup & Initialization

### Dependency
Add KoreDB to your `build.gradle.kts`:
```kotlin
dependencies {
    implementation("io.github.raipankaj:koredb:0.0.6")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
}
```

Ensure the serialization plugin is enabled in your project.

### Initialization

Initialize KoreDB once in your `Application` class using a `lazy` delegate. This ensures disk I/O and WAL replay are deferred until the database is first accessed, keeping app startup times under 10ms.

```kotlin
class MyApplication : Application() {
    val database: KoreDatabase by lazy {
        KoreAndroid.create(this, "my_koredb_instance")
    }

    override fun onCreate() {
        super.onCreate()
        
        // Optional: Trigger background warmup
        CoroutineScope(Dispatchers.IO).launch {
            database.engine
        }
    }
}
```

## Architecture

KoreDB is built on an **LSM-Tree** architecture (like RocksDB or Cassandra), which is heavily optimized for modern flash storage.
- **Writes** are O(1). Data is appended to a Write-Ahead Log (WAL) and an in-memory MemTable.
- **Reads** use a multi-tiered approach: Object Cache -> MemTable -> Bloom Filter -> Disk SSTable via Sparse Index.
- **Deletes** use Tombstones, making deletions O(1) instantaneous operations, rather than expensive O(N) tree-rebalancing operations like in SQLite.

To learn how to utilize these engines, proceed to the specific API guides in this folder.
