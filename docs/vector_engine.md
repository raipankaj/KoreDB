# Vector Database Engine (HNSW)

The KoreDB Vector Engine enables ultra-low-latency semantic vector search, high-dimensional embedding storage, and AI-driven retrieval directly on mobile and edge devices.

---

## 1. Core Architecture

KoreDB implements a hierarchical multi-layer graph architecture (**Hierarchical Navigable Small World - HNSW**):

- **Sub-millisecond Search**: $O(\log N)$ average query time across tens of thousands of embeddings.
- **Asynchronous Construction**: Vector ingestion writes immediately to the LSM log and offloads graph edge construction to a background coroutine channel.
- **Dual Runtime Modes**:
  1. `In-Memory HNSW`: Ultra-fast query latency (~0.3 ms).
  2. `Off-Heap Memory-Mapped HNSW (MmapHNSWIndex)`: Maps a flat binary HNSW file into virtual memory via `MappedByteBuffer`, consuming near-zero JVM heap memory.
- **SIMD Loop Unrolling**: Dot product and cosine similarity calculations utilize 8-lane loop unrolling with hardware instruction pipelining.

---

## 2. Configuration & Initialization

Create a vector collection specifying vector dimensionality and configuration options:

```kotlin
val vectors = db.vectorCollection("embeddings") {
    dimensions = 128
    metric = DistanceMetric.COSINE // Options: COSINE, EUCLIDEAN, INNER_PRODUCT, MANHATTAN
    m = 16                        // Number of bidirectional links per node (default: 16)
    efConstruction = 64           // Size of the dynamic candidate list during build (default: 64)
    efSearch = 32                 // Size of the candidate list during search (default: 32)
    quantization = true           // Enable 8-bit Scalar Quantization (SQ8)
}
```

---

## 3. Ingestion & Multi-Vector Support

### Single Vector Ingestion
```kotlin
val embedding = floatArrayOf(0.15f, -0.42f, 0.98f, /* ... 128 floats */)
val metadata = mapOf("category" to "electronics", "in_stock" to true, "price" to 499.0)

vectors.insert("prod_101", embedding, metadata)
```

### High-Throughput Batch Ingestion
```kotlin
val embeddingsMap: Map<String, FloatArray> = generateEmbeddings()
val metaMap: Map<String, Map<String, Any>> = generateMetadata()

// Parallel batch ingestion
vectors.insertBatch(embeddingsMap, metaMap)

// Optional: Wait for background HNSW graph construction to finish
vectors.waitForIndexing()
```

### Multi-Vector Embeddings (Document Multi-Modality)
Attach multiple named vectors (e.g., text embedding + image embedding) to a single document ID:

```kotlin
vectors.insertMultiVector("prod_101", mapOf(
    "text" to textEmbedding,
    "image" to imageEmbedding
), metadata = mapOf("brand" to "Apple"))

// Search against a specific vector field
val imageMatches = vectors.searchField("image", queryImageEmbedding, limit = 5)
```

---

## 4. Approximate Nearest Neighbor (ANN) Search

Query for the top-$K$ most similar vectors:

```kotlin
val queryVector = floatArrayOf(0.10f, -0.40f, 0.95f, /* ... */)

val topMatches: List<Pair<String, Float>> = vectors.search(queryVector, limit = 5)

topMatches.forEach { (id, similarity) ->
    println("Vector ID: $id, Similarity Score: $similarity")
}
```

---

## 5. Metadata Pre-Filtering & Hybrid Search DSL

KoreDB allows filtering vector candidates by structured metadata attributes during the graph traversal, preventing irrelevant nodes from consuming search budget:

```kotlin
val filteredMatches = vectors.search(queryVector, limit = 10) {
    where("category", eq("electronics"))
    where("price", lte(600.0))
    where("brand", inList(listOf("Apple", "Sony")))
}
```

### Supported Filter Operators:
- `eq(value)`: Strict equality
- `neq(value)`: Not equal
- `gt(num)` / `gte(num)`: Greater than / Greater than or equal
- `lt(num)` / `lte(num)`: Less than / Less than or equal
- `inList(collection)`: Set membership check

---

## 6. Quantization (SQ8 & Product Quantization)

High-dimensional float vectors consume significant memory (128 floats = 512 bytes per vector; 1,536 floats = 6 KB per vector).

KoreDB provides **Scalar Quantization (SQ8)**:
- Quantizes each 32-bit float into an 8-bit signed byte (`-128` to `127`) based on learned dimension min/max boundaries.
- **Result**: **4x RAM reduction** (512 bytes becomes 128 bytes) with **>98% recall accuracy retention**.
