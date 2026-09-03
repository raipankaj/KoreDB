---
name: koredb-graph
description: Build, traverse, and analyze property graphs in KoreDB. Use when creating nodes and directed edges, executing multi-hop graph traversals, utilizing the high-speed key-only traversal fast-path (toIdList), performing cascading node deletions, or running graph algorithms like Dijkstra shortest path, A*, BFS, and PageRank.
---

# KoreDB Property Graph Engine Guide

KoreDB includes a native property graph engine built directly on top of the LSM storage tier. It maintains atomic bidirectional indexes (`g:e:out` and `g:e:in`) for instantaneous incoming and outgoing relationship traversals without recursive SQL joins.

---

## 1. Initializing a Graph

```kotlin
val graph = db.graph("social_network") // or default db.graph()
```

---

## 2. Ingesting Nodes & Edges

### Nodes
Nodes have an ID, an optional label, and arbitrary key-value properties:

```kotlin
graph.putNode(
    id = "user_1",
    label = "User",
    properties = mapOf(
        "name" to "Alice",
        "city" to "San Francisco",
        "age" to 28
    )
)

graph.putNode(
    id = "user_2",
    label = "User",
    properties = mapOf(
        "name" to "Bob",
        "city" to "New York",
        "age" to 32
    )
)
```

### Directed Edges
Edges connect a `fromId` to a `toId`, specify a relationship `type`, an optional float `weight`, and arbitrary properties:

```kotlin
// Alice FOLLOWS Bob
graph.putEdge(
    fromId = "user_1",
    toId = "user_2",
    type = "FOLLOWS",
    weight = 1.0f,
    properties = mapOf("since" to 2024)
)
```

### High-Throughput Batch Construction
```kotlin
// Batch insert nodes and edges for 50x faster graph ingestion
graph.batch {
    for (i in 1..1000) {
        putNode("u$i", "User", mapOf("index" to i))
    }
    for (i in 1..999) {
        putEdge("u$i", "u${i + 1}", "CONNECTED", 1.0f)
    }
}
```

---

## 3. Graph Traversal DSL

Query relationships fluently with the Graph Query DSL:

### 1-Hop & Multi-Hop Traversal
```kotlin
// Find all people that Alice follows
val followingAlice: List<KoreNode> = graph.query()
    .v("user_1")
    .out("FOLLOWS")
    .toNodeList()

// 2-Hop Traversal: Friends of friends (Alice -> FOLLOWS -> FOLLOWS -> ?)
val friendsOfFriends: List<KoreNode> = graph.query()
    .v("user_1")
    .out("FOLLOWS")
    .out("FOLLOWS")
    .toNodeList()
```

### 🚀 Key-Only Fast Path (`toIdList`)
If you only need the target IDs (e.g. for feeding into a document query or recommendation pipeline), use `.toIdList()` instead of `.toNodeList()`.
* **Zero-Deserialization**: Reads edge keys directly from LSM SSTables without fetching or decoding node body CBOR payloads.
* **Result**: **10ms** for 2-hop traversals on physical hardware.

```kotlin
val targetIds: List<String> = graph.query()
    .v("user_1")
    .out("FOLLOWS")
    .out("FOLLOWS")
    .toIdList() // Ultra-fast key-only scan
```

### Inbound vs Outbound Traversal
* `.out("TYPE")`: Follow outgoing edges (`user_1` $\to$ `target`).
* `.inE("TYPE")`: Follow incoming edges (`source` $\to$ `user_1`).
* `.both("TYPE")`: Traverse bidirectional connections.

---

## 4. Cascading Deletions

Deleting a node atomically removes the node record, all outgoing edges, all incoming edges, and property indexes in a single ACID commit:

```kotlin
// Automatically purges incoming/outgoing edges to prevent orphan links
graph.deleteNode("user_1")

// Delete single directed edge
graph.deleteEdge("user_1", "user_2", "FOLLOWS")
```

---

## 5. Built-in Graph Algorithms

KoreDB includes native graph algorithms executing directly against off-heap indexes:

### Dijkstra Shortest Path
Finds the shortest weighted path between two nodes:

```kotlin
val pathResult: PathResult? = graph.dijkstra(
    startNode = "user_1",
    endNode = "user_500",
    edgeType = "CONNECTED" // Optional filter
)

if (pathResult != null) {
    println("Shortest Distance: ${pathResult.totalWeight}")
    println("Path: ${pathResult.nodes.joinToString(" -> ")}")
}
```

### PageRank Centrality
Computes node influence across the network:

```kotlin
val scores: Map<String, Float> = graph.pageRank(
    iterations = 20,
    dampingFactor = 0.85f
)

// Print top influential nodes
scores.entries.sortedByDescending { it.value }.take(5).forEach {
    println("Node ${it.key}: PageRank ${it.value}")
}
```

### Breadth-First Search (BFS) & Depth-First Search (DFS)
```kotlin
// Traverse network level by level
graph.bfs(startNode = "user_1", maxDepth = 3) { node, depth ->
    println("Visited ${node.id} at depth $depth")
}
```

---

## 6. Visualization & Export

Export graph structures to standard formats for visualization in tools like Gephi, yEd, or Cytoscape:

```kotlin
// Generate Graphviz DOT format
val dotString: String = graph.exportDot()
File(context.filesDir, "graph.dot").writeText(dotString)
```
