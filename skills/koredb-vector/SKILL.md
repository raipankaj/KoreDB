---
name: koredb-vector
description: Implement on-device vector similarity search and embeddings with the KoreDB HNSW engine. Use when configuring high-dimensional vector embeddings, setting up Cosine/Euclidean/IP/Manhattan distance metrics, enabling SQ8 or Product Quantization (PQ), executing metadata-filtered KNN searches, or managing off-heap mmap storage.
---

# KoreDB Vector Engine Guide

KoreDB includes a native, pure-Kotlin Hierarchical Navigable Small World (HNSW) vector index designed for on-device AI, semantic search, and RAG retrieval pipelines. It features 16-lane SIMD-unrolled distance math, SQ8 quantization, off-heap mmap hydration, and metadata filtering.

---

## 1. Initializing a Vector Collection

Configure dimension size, distance metric, and HNSW graph hyperparameters during initialization:

```kotlin
import com.pankaj.koredb.hnsw.DistanceMetric

val vectors = db.vectorCollection("article_embeddings") {
    dimensions = 384                     // Vector dimension (e.g. MiniLM-L6-v2, MobileBERT)
    metric = DistanceMetric.COSINE       // COSINE, EUCLIDEAN, INNER_PRODUCT, or MANHATTAN
    m = 16                               // Number of bidirectional links per node (default: 16)
    efConstruction = 100                 // Build search depth (higher = higher recall, default: 100)
    efSearch = 50                        // Query search depth (default: 50)
    enableQuantization = true            // Enables SQ8 scalar quantization (4x RAM reduction)
}
```

### Supported Distance Metrics
| Metric | Value | Best Used For |
| :--- | :--- | :--- |
| `COSINE` | `DistanceMetric.COSINE` | Normalized text & image embeddings (MiniLM, OpenAI, CLIP). Scores: `[-1.0, 1.0]`. |
| `EUCLIDEAN` | `DistanceMetric.EUCLIDEAN` | Spatial coordinates, geometric embeddings. Scores: `(-inf, 0.0]`. |
| `INNER_PRODUCT`| `DistanceMetric.INNER_PRODUCT`| Unit-normalized vectors, recommendation dot-products. |
| `MANHATTAN` | `DistanceMetric.MANHATTAN` | Grid-like high-dimensional sparsity metrics. |

---

## 2. Ingesting Vectors & Metadata

Store vectors (`FloatArray`) with optional key-value metadata for hybrid filtering:

### Single Vector Ingest
```kotlin
val embedding: FloatArray = getEmbeddingFromTFLite("On-device AI with KoreDB")

vectors.insert(
    id = "doc_101",
    vector = embedding,
    metadata = mapOf(
        "category" to "ai",
        "author" to "pankaj",
        "year" to 2026
    )
)
```

### Batch Ingest
```kotlin
val batchMap: Map<String, FloatArray> = generateBatchEmbeddings()

// Concurrent batch insertion
vectors.insertBatch(batchMap)

// Optional: Block until the background HNSW index worker finishes linking nodes
vectors.waitForIndexing()
```

---

## 3. Vector Similarity Search (ANN)

Search returns `List<Pair<String, Float>>` sorted by descending similarity:

```kotlin
val queryVector: FloatArray = getEmbeddingFromTFLite("machine learning databases")

val topResults: List<Pair<String, Float>> = vectors.search(
    query = queryVector,
    limit = 5
)

topResults.forEach { (docId, similarityScore) ->
    println("Matched Doc: $docId (Score: $similarityScore)")
}
```

---

## 4. Metadata-Filtered Hybrid Vector Search

Filter candidates during graph traversal to ensure results match categorical or numeric criteria:

```kotlin
val filteredMatches = vectors.search(queryVector, limit = 10) {
    where("category", eq("ai"))
    where("year", gte(2025))
    where("author", inList(listOf("alice", "bob", "pankaj")))
}
```

Supported filter operators in the DSL:
* `eq(value)`
* `neq(value)`
* `gt(num)` / `gte(num)`
* `lt(num)` / `lte(num)`
* `inList(collection)`
* `contains(substring)`

---

## 5. Multi-Vector Support per Entity

Store multiple distinct embedding spaces (e.g. text embedding + image embedding) for the same entity:

```kotlin
// Store text embedding
vectors.insertField("prod_99", "text_emb", textVector)

// Store image embedding
vectors.insertField("prod_99", "image_emb", imageVector)

// Query specific embedding space
val imageMatches = vectors.searchField(
    field = "image_emb",
    query = userCapturedImageVector,
    limit = 10
)
```

---

## 6. Memory Optimization & Quantization

### SQ8 Scalar Quantization (Built-in)
* **What it does**: Compresses 32-bit floats into 8-bit integers (`int8`).
* **Benefits**: 4x reduction in RAM usage with $<1\%$ recall drop.
* **Enable**: Set `enableQuantization = true` in config.

### Off-Heap Memory Mapping (`MmapHNSWIndex`)
When `KoreDatabase` reopens, the HNSW index file (`hnsw_<name>.bin`) is mapped directly into off-heap virtual memory using `FileChannel.map(READ_ONLY)`.
* Zero JVM heap allocation during startup.
* Queries read directly from kernel page cache.
* Instant cold restarts even with 100,000+ stored vectors.

---

## 7. Updates & Deletions

```kotlin
// Update vector (replaces vector and updates HNSW links)
vectors.insert("doc_101", updatedEmbedding)

// Delete vector (soft tombstone + purges from search results)
vectors.delete("doc_101")
```
