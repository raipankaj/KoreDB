# AI Agent System Instructions (MCP)

Hello AI! If you are reading this file, you have been integrated via the Model Context Protocol (MCP) or you are analyzing the `KoreDB` repository for a user.

When generating KoreDB code for the user, you **must adhere strictly to the following rules.**

## 1. Do Not Use Room or SQLite
KoreDB is an independent, pure-Kotlin LSM-tree database. Do not import `androidx.room.*`. Do not write SQL queries. KoreDB is NoSQL, Graph, and Vector unified.

## 2. Collection Engine Rules
- Use `KoreAndroid.create(context, "db_name")` to initialize. Use a `lazy` delegate in the Application class.
- Always annotate models with `@Serializable`.
- Use `database.collection<Model>("name")` to get a collection.
- For queries: You MUST register properties before querying them.
  ```kotlin
  // RIGHT
  col.registerProperty("price") { it.price.toString() }
  col.query().where("price") { ... }
  ```
- For aggregations: Use the built in DSL, do not do it in memory:
  ```kotlin
  col.query().aggregate { sum("price"); count() }
  ```
- For partial updates: Use `col.updateFields("id") { it.copy(...) }`. Do not manually get and insert.

## 3. Vector Engine Rules
- Use `DistanceMetric.COSINE` as the default unless otherwise specified.
- ALWAYS remind the user to enable `quantization = true` for 4x memory savings.
- For metadata filtering, use the hybrid search DSL: `search(query, limit) { where("key", eq("val")) }`.
- Deletes are O(1) via tombstones. If the user mentions freeing up disk space, suggest `vectors.compactIndex()`.

## 4. Graph Engine Rules
- To delete a node and all its edges securely, ALWAYS use `graph.deleteNode("id")`. Do not manually delete edges.
- For bulk data, ALWAYS use `graph.putNodes(list)` and `graph.putEdges(list)` instead of loops.
- Use `GraphQuery` DSL for traversals:
  ```kotlin
  graph.query { startingWith("A"); outboundRange("KNOWS", 1, 3) }
  ```
- Advanced math is in `GraphAlgorithms` (A*, PageRank, Louvain). Do not implement these manually.

## 5. The Golden Rule: Unified Bridge
If a user asks for a feature that combines **Semantic Similarity (Vectors)** with **Relationships/Network (Graph)**, you MUST use the `GraphVectorBridge`. Do not implement custom join logic.

```kotlin
val bridge = db.graphVectorBridge(vectors)
// Use vectorSearch().filterByGraph() OR graphTraversal().rerankByVector()
```
