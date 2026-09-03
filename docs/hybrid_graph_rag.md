# Unified Graph RAG Bridge (`GraphVectorBridge`)

The **Unified Graph RAG Bridge** is KoreDB's killer architectural feature. It bridges the **Property Graph Engine**, the **HNSW Vector Engine**, and **BM25 Full-Text Search** into a single, cohesive retrieval system for on-device AI and Retrieval-Augmented Generation (RAG).

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         GraphVectorBridge Query                             │
 └───────────────────────┬─────────────────────────────┬───────────────────────┘
                         │                             │
             ┌───────────▼────────────┐    ┌───────────▼────────────┐
             │ Graph Traversal Filter │    │ Semantic Vector Search │
             │ - BFS Community Scope  │    │ - Top-K Embeddings     │
             │ - Relationship Hops    │    │ - Cosine Similarity    │
             └───────────┬────────────┘    └───────────┬────────────┘
                         │                             │
                         └──────────────┬──────────────┘
                                        │
                               ┌────────▼────────┐
                               │ Reciprocal Rank │
                               │  Fusion (RRF)   │
                               └────────┬────────┘
                                        │
                               ┌────────▼────────┐
                               │ Top Candidates  │
                               │ + Documents     │
                               └─────────────────┘
```

---

## 1. Why Graph RAG on Mobile?

Pure vector search (semantic retrieval) often suffers from hallucination or false positives because it ignores **relational context**:
- Example: Searching for *"laptop chargers"* might return irrelevant accessories from different brands with high cosine similarity.
- With **Graph RAG**, KoreDB filters candidate vectors strictly within the user's explicit relationships (e.g., *only chargers compatible with user's specific laptop model in the graph*).

---

## 2. Graph-First Querying (`graphFirst`)

Traverse relationships in the graph first, and then rank the discovered nodes using vector similarity:

```kotlin
import com.pankaj.koredb.bridge.GraphVectorBridge

val bridge = GraphVectorBridge(db)

// Find all products purchased by people in Alice's social circle,
// ranked by semantic similarity to the user's search query
val results = bridge.graphFirst("user_alice")
    .traverse(edgeType = "FOLLOWS", maxDepth = 2)
    .traverse(edgeType = "PURCHASED", maxDepth = 1)
    .rerankByVector(
        vectorCollectionName = "products",
        queryVector = searchEmbedding,
        limit = 10
    )

results.forEach { result ->
    println("Item: ${result.id}, Combined Score: ${result.score}")
}
```

---

## 3. Vector-First Querying (`vectorFirst`)

Perform an Approximate Nearest Neighbor (ANN) vector search first, and then expand the top matches along graph relationships:

```kotlin
// Retrieve top 5 most similar articles, and expand to find their co-authors and related topics
val expandedContext = bridge.vectorFirst(
        vectorCollectionName = "articles",
        queryVector = promptEmbedding,
        limit = 5
    )
    .expandGraph(edgeType = "WRITTEN_BY", direction = Direction.OUTBOUND)
    .expandGraph(edgeType = "TAGGED_WITH", direction = Direction.OUTBOUND)
    .execute()
```

---

## 4. Multi-Modal Hybrid Search (`searchHybrid`)

Execute **Vector Semantic Search** and **BM25 Keyword Search** simultaneously across coroutines, merging the results using **Reciprocal Rank Fusion (RRF)**:

```kotlin
val hybridResults = bridge.searchHybrid(
    collectionName = "knowledge_base",
    queryText = "LSM-tree write stall recovery",
    queryVector = queryEmbedding,
    vectorWeight = 0.7f,
    textWeight = 0.3f,
    limit = 10
)

hybridResults.forEach { match ->
    println("Doc ID: ${match.id}, Score: ${match.score}, Matched by: ${match.source}")
}
```

### Reciprocal Rank Fusion (RRF) Formula:
$$\text{RRF Score}(d) = \sum_{m \in M} \frac{w_m}{k + r_m(d)}$$
where $k=60$ is the smoothing constant, $r_m(d)$ is the rank position of document $d$ under model $m$, and $w_m$ is the model weight.
