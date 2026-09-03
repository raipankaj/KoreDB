# Property Graph Engine

The KoreDB Property Graph Engine provides native graph modeling, variable-length relationship traversals, and topological graph algorithms directly on top of the LSM-tree.

---

## 1. Dual-Write Bidirectional Architecture

In real-world graphs, queries require efficient traversal in both directions (e.g., "What are user A's purchases?" vs "Who bought product B?").

KoreDB implements an **Atomic Dual-Write Strategy**:
- **Outbound Edge Key**: `g:e:out:{sourceId}:{type}:{targetId}`
- **Inbound Edge Key**: `g:e:in:{targetId}:{type}:{sourceId}`

Both keys are persisted atomically in a single write batch. This guarantees **$O(1)$ relationship lookups** regardless of traversal direction, without requiring index seeks.

Furthermore, KoreDB features a **Key-Only Traversal Fast-Path**:
Graph traversal algorithms (BFS, Dijkstra) extract target node IDs directly from LSM key strings via `getOutboundTargetIds`, **bypassing disk payload reading and JSON deserialization**.

---

## 2. Managing Nodes & Edges

```kotlin
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.Edge

val graph = db.graph()

// 1. Create Nodes
graph.putNode(Node(
    id = "user_101",
    labels = setOf("User", "Developer"),
    properties = mapOf("name" to "Alice", "country" to "IN")
))

graph.putNode(Node(
    id = "repo_koredb",
    labels = setOf("Repository"),
    properties = mapOf("language" to "Kotlin", "stars" to "2500")
))

// 2. Create Directed Relationships
graph.putEdge(Edge(
    sourceId = "user_101",
    targetId = "repo_koredb",
    type = "CONTRIBUTED_TO",
    weight = 1.0,
    properties = mapOf("commits" to "150", "role" to "maintainer")
))
```

---

## 3. Querying & Traversal

### O(1) Relationship Lookups
```kotlin
// Retrieve all outbound edges of type 'CONTRIBUTED_TO'
val contributions: List<Edge> = graph.getOutboundEdges("user_101", "CONTRIBUTED_TO")

// Key-Only Fast-Path (sub-microsecond traversal without reading edge payloads)
val targetRepoIds: List<String> = graph.getOutboundTargetIds("user_101", "CONTRIBUTED_TO")

// Reverse traversal: Find all contributors to a repository
val contributorIds: List<String> = graph.getInboundSourceIds("repo_koredb", "CONTRIBUTED_TO")
```

### Cascading Node Deletion
Deleting a node atomically removes the node record and **all associated incoming and outgoing edges, label indices, and property indices**:

```kotlin
// Cascading removal across all directional indices
graph.deleteNode("user_101")
```

---

## 4. Graph Algorithms

KoreDB includes a built-in suite of high-performance graph algorithms implemented on top of the storage engine:

### Breadth-First Search (BFS) & Depth-First Search (DFS)
```kotlin
import com.pankaj.koredb.graph.algo.GraphAlgorithms

val algo = GraphAlgorithms(graph)

// Find all reachable nodes up to 3 degrees of separation
val reachableNodeIds = algo.bfs(startNodeId = "user_101", edgeType = "FOLLOWS", maxDepth = 3)
```

### Dijkstra's Shortest Path
Computes the lowest-cost path between two nodes considering edge weights:

```kotlin
val pathResult = algo.shortestPathDijkstra(
    startNodeId = "city_delhi",
    targetNodeId = "city_bangalore",
    edgeType = "CONNECTED_ROAD"
)

println("Shortest distance: ${pathResult.totalWeight}")
println("Optimal route: ${pathResult.path.joinToString(" -> ")}")
```

### A* Pathfinding
Heuristic-accelerated pathfinding between geographical coordinates or feature spaces:

```kotlin
val aStarPath = algo.aStarPath(
    startNodeId = "node_a",
    targetNodeId = "node_z",
    edgeType = "ROAD",
    heuristic = { currentNodeId, targetNodeId ->
        // Manhattan or Euclidean distance estimate
        calculateDistance(currentNodeId, targetNodeId)
    }
)
```

### PageRank & Centrality Analysis
Computes node importance and authority scores across dense graphs:

```kotlin
val rankScores: Map<String, Double> = algo.pageRank(
    iterations = 20,
    dampingFactor = 0.85
)

// Print top 5 most influential nodes
rankScores.entries.sortedByDescending { it.value }.take(5).forEach { (nodeId, score) ->
    println("Node $nodeId rank: $score")
}
```
