# Vector Database Engine

KoreDB includes a purpose-built, highly optimized Vector Database utilizing a **Hierarchical Navigable Small World (HNSW)** index. It is designed for on-device AI applications (RAG, semantic search, image similarity).

## Initialization

```kotlin
val vectors = database.vectorCollection("embeddings") {
    dimensions = 128
    metric = DistanceMetric.COSINE          // COSINE, EUCLIDEAN, INNER_PRODUCT, MANHATTAN
    quantization = true                      // Enable SQ8 for 4x memory savings
    maxConnections = 16                      // M parameter for HNSW
    efConstruction = 200                     // Build quality vs speed
    efSearch = 50                            // Search quality vs speed
    namespace = "user_1"                     // Multi-tenant isolation
}
```

## Insertion & Batching

```kotlin
// Single insert
vectors.insert("doc1", floatArrayOf(0.1f, 0.2f, ...), mapOf("category" to "A"))

// Batch insert (High throughput)
vectors.insertBatch(
    vectors = mapOf("doc1" to vec1, "doc2" to vec2),
    metadataMap = mapOf("doc1" to mapOf("cat" to "A"), "doc2" to mapOf("cat" to "B"))
)
```
*Note: HNSW graph building happens asynchronously in the background. If you need to ensure indexing is finished before querying, call `vectors.waitForIndexing()`.*

## Semantic Search

```kotlin
// Standard ANN search
val results = vectors.search(queryVector, limit = 10)
for ((id, score) in results) {
    println("Found $id with similarity $score")
}
```

## Hybrid Search (Pre-Filtering)

KoreDB supports filtering during the HNSW graph traversal. This ensures you always receive the exact `limit` number of results, avoiding the "post-filtering" problem where matching vectors are dropped.

```kotlin
val results = vectors.search(queryVector, limit = 10) {
    where("category", eq("shoes"))
    where("price", lte(99.99))
    where("brand", inList("Nike", "Adidas"))
}
```

### Supported Operators
- `eq(val)`, `neq(val)`
- `gt(val)`, `gte(val)`, `lt(val)`, `lte(val)`
- `inList(v1, v2)`, `notInList(v1, v2)`
- `contains(str)`
- `exists()`

## Multi-Vector Documents

You can store multiple named embeddings for a single entity (e.g., product image + product description).

```kotlin
vectors.insertMultiVector("product_1",
    vectors = mapOf(
        "image" to imageEmbedding,
        "text" to textEmbedding
    ),
    metadata = mapOf("price" to 50.0)
)

// Search specifically against the 'image' embeddings
val imageMatches = vectors.searchField("image", queryVector, limit = 5)
```

## Deletion & Updates

KoreDB uses **tombstones** for vector deletion, making deletes $O(1)$. 

```kotlin
vectors.delete("doc1")
vectors.deleteBatch(listOf("doc1", "doc2"))

// Update vector AND metadata
vectors.update("doc1", newVector, newMetadata)

// Ultra-fast Metadata-only update (Does not require re-indexing the vector!)
vectors.updateMetadata("doc1", mapOf("price" to 40.0))
```

### Compaction
Since deletes use tombstones, the graph may accumulate dead nodes. You can physically prune the graph:
```kotlin
vectors.compactIndex()
```

## Quantization (SQ8)
If you set `quantization = true` during setup, KoreDB compresses 32-bit floats into 8-bit integers.
- Reduces memory usage by **75%** (e.g., 2500KB -> 625KB).
- Distance calculations are significantly faster using bitwise operations.
- Recall accuracy loss is typically under 1%.
