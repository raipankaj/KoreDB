# Unified Graph + Vector Bridge

The **`GraphVectorBridge`** is KoreDB's killer feature. No other embedded database natively unifies semantic vector search with complex graph traversals.

It allows you to answer queries like:
> *"Find products semantically similar to this image, but ONLY if they are manufactured by a brand that my friends follow."*

## Initialization

```kotlin
val graph = database.graph()
val vectors = database.vectorCollection("products")

val bridge = database.graphVectorBridge(vectors)
```

## Pattern 1: Vector-First (Find Semantic, Filter by Relationship)

This pattern is useful when semantic relevance is the primary goal, but the results must adhere to a strict graph topology constraint.

```kotlin
val results = bridge.vectorSearch(queryEmbedding, limit = 50)
    .filterByGraph { productId ->
        // Condition: Product must have an outbound "MADE_BY" edge to a known brand
        val brands = graph.getOutboundTargetIds(productId, "MADE_BY")
        brands.any { it in userFollowedBrands }
    }
    .take(10) // Take top 10 after graph filtering
```

**Built-in Graph Filters:**
```kotlin
// Only return items that are directly connected to "category_tech"
val results = bridge.vectorSearch(queryEmbedding)
    .connectedTo("category_tech", "BELONGS_TO")
```

## Pattern 2: Graph-First (Traverse Network, Rank by Semantic)

This pattern is useful when the candidate pool is defined by network relationships (e.g., content from my network), and you want to surface the most semantically relevant items from that specific pool.

```kotlin
// 1. Traverse the graph to find all products purchased by friends (2 hops)
// User -> KNOWS -> Friend -> PURCHASED -> Product
val results = bridge.graphTraversal(startNodeId = "user_123", edgeType = "PURCHASED", hops = 2)
    
    // 2. Rank that exact subset of nodes using Vector Similarity
    .rerankByVector(queryEmbedding)
    
    .take(10)

for (res in results) {
    println("Item: ${res.id}, Similarity: ${res.similarity}, GraphNode: ${res.node}")
}
```

## Pattern 3: Property-First Reranking

If you don't need a deep traversal, but just want to filter nodes by a specific indexed property before doing a vector comparison:

```kotlin
val results = bridge.graphQuery(label = "Product", propertyKey = "status", propertyValue = "in_stock")
    .rerankByVector(queryEmbedding)
    .take(5)
```
