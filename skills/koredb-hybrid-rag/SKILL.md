---
name: koredb-hybrid-rag
description: Build on-device Hybrid Graph RAG and multi-model AI retrieval pipelines with KoreDB GraphVectorBridge. Use when combining HNSW dense vector search with knowledge graph structure, executing Reciprocal Rank Fusion (RRF) between BM25 keyword search and vector rankings, choosing Graph-First vs Vector-First traversal, or assembling local LLM context.
---

# KoreDB Hybrid Graph RAG Guide

KoreDB is the only embedded mobile database featuring a native **`GraphVectorBridge`** that seamlessly unifies **Dense Vector Similarity** with **Knowledge Graph Relationships** and **BM25 Keyword Search** into a single query pipeline with zero glue code.

---

## 1. Why Hybrid Graph RAG?

* **Vector Search Alone** suffers from semantic hallucination and cannot reason about exact structural relationships (e.g. *"Authored by a verified researcher in Alice's department"*).
* **Graph Search Alone** requires exact keyword matches and cannot understand natural language synonyms or fuzzy semantic concepts.
* **KoreDB Graph RAG** combines both: dense embeddings discover semantic candidates, while the property graph enforces factual consistency and ontological structure.

---

## 2. Setting Up the Bridge

Link your existing `KoreGraph`, `KoreVectorCollection`, and `KoreCollection` together:

```kotlin
import com.pankaj.koredb.bridge.GraphVectorBridge

val bridge = GraphVectorBridge(
    graph = db.graph("knowledge_base"),
    vectors = db.vectorCollection("embeddings") { dimensions = 384 },
    documents = db.collection("documents", Document.serializer()) { it.id }
)
```

---

## 3. Query Strategies

The bridge provides two complementary execution strategies depending on your query goal:

### Strategy A: Vector-First (Semantic Search $\to$ Structural Filter)
1. Executes an HNSW vector search to find the top-$K$ semantically relevant items.
2. Expands each result through the graph to ensure it connects to required entities or passes structural constraints.

```kotlin
val queryEmbedding: FloatArray = embedQuery("quantum computing algorithms")

// Find semantically similar papers that are cited by Dr. Smith (user_42)
val results = bridge.queryVectorFirst(
    queryVector = queryEmbedding,
    candidateLimit = 50,
    finalLimit = 5
) { candidateNodeId ->
    // Graph predicate: Must be connected via CITES to user_42
    graph.query().v(candidateNodeId).out("CITES").toIdList().contains("user_42")
}
```

### Strategy B: Graph-First (Structural Traversal $\to$ Semantic Rerank)
1. Traverses the knowledge graph to discover all structurally valid candidate entities (e.g. all products in the user's favorite category or department).
2. Computes cosine similarity scores on the candidates against the query embedding and returns the top-$K$.

```kotlin
// Get all courses taken by Alice's direct peers, reranked by semantic interest
val peerCourses = bridge.queryGraphFirst(
    startNode = "user_alice",
    traversal = { v("user_alice").out("FRIEND").out("ENROLLED_IN").toIdList() },
    queryVector = userInterestEmbedding,
    limit = 5
)
```

---

## 4. Reciprocal Rank Fusion (RRF)

Reciprocal Rank Fusion blends dense vector similarity rankings with sparse BM25 keyword rankings or graph PageRank centrality into a single, highly accurate score:

$$\text{RRF}(d) = \sum_{m \in M} \frac{1}{k + \text{rank}_m(d)}$$

```kotlin
// Execute BM25 + Vector Hybrid Search using RRF
val hybridRankings: List<Pair<String, Float>> = bridge.hybridSearchRrf(
    queryText = "offline neural networks",
    queryVector = queryEmbedding,
    limit = 10,
    k = 60 // Standard RRF smoothing constant (default: 60)
)

hybridRankings.forEach { (docId, rrfScore) ->
    println("Ranked Result: $docId (RRF Score: $rrfScore)")
}
```

---

## 5. Assembling Context for On-Device LLMs (RAG)

Assemble enriched context directly for local models (e.g. MediaPipe GenAI, llama.cpp, ExecuTorch):

```kotlin
suspend fun buildPromptContext(userQuery: String): String {
    val queryVector = localEmbedder.embed(userQuery)

    // Retrieve top hybrid results
    val matches = bridge.hybridSearchRrf(userQuery, queryVector, limit = 3)

    return buildString {
        appendLine("### Retrieved Knowledge Context:")
        for ((id, score) in matches) {
            val doc = documents.getById(id) ?: continue
            val relatedEntities = graph.query().v(id).out("RELATED_TO").toNodeList()

            appendLine("- Document: ${doc.title}")
            appendLine("  Content: ${doc.body}")
            if (relatedEntities.isNotEmpty()) {
                appendLine("  Entities: ${relatedEntities.joinToString { it.label ?: it.id }}")
            }
        }
    }
}
```

---

## 6. Query Profiling (`EXPLAIN`)

Profile query strategies and execution costs:

```kotlin
val explanation = bridge.explain(queryText = "database engine", queryVector = queryVector)
println(explanation)
// Output:
// Strategy Chosen: VECTOR_FIRST
// Estimated Candidates: 45
// Graph Traversals: 12ms | HNSW Search: 0.8ms | Total Latency: 13.2ms
```
