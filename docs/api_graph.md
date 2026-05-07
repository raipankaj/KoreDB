# Graph Database Engine

KoreDB includes a native property graph engine. Instead of writing complex SQL `JOIN` statements, you can represent data as `Nodes` and `Edges` and traverse them efficiently.

## Core Models

```kotlin
// Node: An entity with labels and properties
val alice = Node(
    id = "u1", 
    labels = setOf("User", "Admin"), 
    properties = mapOf("name" to "Alice", "city" to "Tokyo")
)

// Edge: A directional relationship with properties
val edge = Edge(
    sourceId = "u1", 
    targetId = "u2", 
    type = "FOLLOWS", 
    properties = mapOf("weight" to "1.0")
)
```

## Storage & Mutation

```kotlin
val graph = database.graph()

// Single Operations
graph.putNode(alice)
graph.putEdge(edge)

// Batch Operations (Much faster for large datasets)
graph.putNodes(listOf(node1, node2, node3))
graph.putEdges(listOf(edge1, edge2, edge3))

// Cascading Delete (Atomic: removes node + ALL inbound/outbound edges + indexes)
graph.deleteNode("u1")
```

## GraphQuery DSL

Traverse the graph fluently without raw algorithms.

```kotlin
// Find all users in Tokyo that Alice follows
val friendsInTokyo = graph.query {
    startingWith("u1")
    outbound("FOLLOWS", hops = 1)
    hasProperty("city", "Tokyo")
    limit(10)
}.toNodeList()

// Variable-Length Traversal (e.g. Friends of friends, up to 4 degrees of separation)
// Automatically handles cycle detection to prevent infinite loops.
val network = graph.query {
    startingWith("u1")
    outboundRange("FOLLOWS", minHops = 2, maxHops = 4)
}.toIdList()

// Fast Counting (Avoids materializing node objects)
val followerCount = graph.query {
    startingWith("u1")
    inbound("FOLLOWS")
}.count()
```

## Advanced Graph Algorithms (`GraphAlgorithms.kt`)

KoreDB provides built-in implementations for complex graph mathematics.

### Pathfinding
```kotlin
// Dijkstra (Shortest path considering edge weights)
val path = GraphAlgorithms.shortestPathDijkstra(graph, "NYC", "LA", "ROAD")

// A* (Heuristic-guided shortest path - much faster for spatial data)
val aStarPath = GraphAlgorithms.aStarPath(graph, "NYC", "LA", "ROAD") { node, goal ->
    spatialDistance(node, goal) // Your custom heuristic function
}
```

### Network Analysis
```kotlin
val allIds = listOf("u1", "u2", "u3", ...)

// Community Detection (Louvain-inspired modularity optimization)
val communities = GraphAlgorithms.detectCommunities(graph, allIds, "FOLLOWS")
// Returns: Map<String, Int> (NodeID -> Community ID)

// Connected Components
val components = GraphAlgorithms.connectedComponents(graph, allIds, "FOLLOWS")
// Returns: List<Set<String>> (List of isolated subgraphs)

// PageRank (Determine node importance/influence)
val ranks = GraphAlgorithms.pageRank(graph, allIds, "FOLLOWS")

// Degree Centrality (Normalized connection count)
val centrality = GraphAlgorithms.degreeCentrality(graph, allIds, "FOLLOWS")
```

## Exporting for Visualization

You can export subgraphs to standard formats for visualization in tools like Gephi, yEd, or Graphviz.

```kotlin
// Graphviz DOT format
val dotString = GraphExport.toDot(graph, nodeIds, "FOLLOWS")

// GraphML format
val gmlString = GraphExport.toGraphML(graph, nodeIds, null) // null = all edge types
```
