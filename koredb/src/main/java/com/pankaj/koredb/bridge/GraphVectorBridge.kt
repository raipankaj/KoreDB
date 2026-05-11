package com.pankaj.koredb.bridge

import com.pankaj.koredb.core.KoreVectorCollection
import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.hnsw.*

/**
 * Unified Graph + Vector query bridge.
 *
 * This is KoreDB's **unique differentiator** — no other database (embedded or cloud)
 * offers a combined graph traversal + vector similarity search in a single query API.
 *
 * Usage:
 * ```kotlin
 * // Find products similar to a query image, made by brands the user follows
 * val results = GraphVectorBridge(graph, vectorCollection)
 *     .vectorSearch(imageQuery, limit = 50)
 *     .filterByGraph { nodeId ->
 *         // Check if this product is made by a brand the user follows
 *         graph.getInboundSourceIds(nodeId, "MADE_BY")
 *             .any { brandId -> userFollowedBrands.contains(brandId) }
 *     }
 *     .take(10)
 *
 * // Or: Start from graph, then rank by vector similarity
 * val results = GraphVectorBridge(graph, vectorCollection)
 *     .graphTraversal("user_123", "PURCHASED", hops = 2) // Products friends bought
 *     .rerankByVector(queryEmbedding)                     // Rank by similarity
 *     .take(10)
 * ```
 */
class GraphVectorBridge(
    private val graph: GraphStorage,
    private val vectors: KoreVectorCollection
) {
    /**
     * Result item combining graph node data with vector similarity score.
     */
    data class BridgeResult(
        val id: String,
        val similarity: Float,
        val node: Node? = null,
        val metadata: Map<String, Any>? = null
    )

    /**
     * Vector-first search: Find similar vectors, then filter/enrich with graph data.
     *
     * @param query The query vector.
     * @param limit Maximum results from the vector search.
     * @param filter Optional metadata filter.
     * @return A [VectorFirstBuilder] for chaining graph operations.
     */
    suspend fun vectorSearch(
        query: FloatArray,
        limit: Int = 50,
        filter: (VectorFilterBuilder.() -> Unit)? = null
    ): VectorFirstBuilder {
        val results = vectors.search(query, limit, filter)
        return VectorFirstBuilder(graph, results)
    }

    /**
     * Graph-first search: Traverse the graph, then rank results by vector similarity.
     *
     * @param startNodeId The node to start traversal from.
     * @param edgeType The edge type to follow.
     * @param hops Number of hops to traverse.
     * @return A [GraphFirstBuilder] for chaining vector operations.
     */
    fun graphTraversal(
        startNodeId: String,
        edgeType: String,
        hops: Int = 1
    ): GraphFirstBuilder {
        val visited = mutableSetOf<String>()
        var frontier = setOf(startNodeId)
        visited.add(startNodeId)

        for (i in 0 until hops) {
            val next = mutableSetOf<String>()
            for (id in frontier) {
                for (targetId in graph.getOutboundTargetIds(id, edgeType)) {
                    if (visited.add(targetId)) next.add(targetId)
                }
            }
            frontier = next
        }

        // All reachable node IDs (excluding the start node)
        val reachableIds = visited.minus(startNodeId).toList()
        return GraphFirstBuilder(graph, vectors, reachableIds)
    }

    /**
     * Graph-first with property filter: Find nodes by label/property, then rank by similarity.
     */
    fun graphQuery(label: String, propertyKey: String, propertyValue: String): GraphFirstBuilder {
        val nodes = graph.getNodesByProperty(label, propertyKey, propertyValue)
        return GraphFirstBuilder(graph, vectors, nodes.map { it.id })
    }

    /**
     * Executes a full GraphRAG (Graph Retrieval-Augmented Generation) query.
     *
     * This method combines Vector Search and Graph Traversal to retrieve the most contextually
     * rich subgraph for an LLM prompt. It outperforms traditional RAG by ensuring the LLM gets
     * both semantically relevant entries and structurally connected context.
     *
     * @param query The query vector (e.g., prompt embedding).
     * @param initialLimit Number of "seed" nodes to find via vector similarity.
     * @param edgeType Optional. The type of relationship to traverse for context expansion. If null, follows all outbound edges.
     * @param maxHops How deep to traverse from the seed nodes to collect structural context.
     * @param finalLimit The maximum number of context nodes to return to the LLM.
     * @return A list of [BridgeResult] containing the node data and similarity score.
     */
    suspend fun graphRAGQuery(
        query: FloatArray,
        initialLimit: Int = 10,
        edgeType: String? = null,
        maxHops: Int = 2,
        finalLimit: Int = 10
    ): List<BridgeResult> {
        // 1. Semantic Entry: Find seed nodes
        val seedResults = vectors.search(query, initialLimit)
        val seedNodeIds = seedResults.map { it.first }.toSet()

        // 2. Structural Expansion: Traverse graph to pull in connected context
        val expandedNodes = mutableSetOf<String>()
        expandedNodes.addAll(seedNodeIds)

        var frontier = seedNodeIds
        for (i in 0 until maxHops) {
            val nextFrontier = mutableSetOf<String>()
            for (id in frontier) {
                val targets = if (edgeType != null) {
                    graph.getOutboundTargetIds(id, edgeType)
                } else {
                    graph.getAllOutboundEdges(id).map { it.targetId }
                }
                for (targetId in targets) {
                    if (expandedNodes.add(targetId)) {
                        nextFrontier.add(targetId)
                    }
                }
            }
            frontier = nextFrontier
            if (frontier.isEmpty()) break
        }

        // 3. Semantic Reranking: Score all expanded nodes against the prompt
        val results = mutableListOf<BridgeResult>()
        for (id in expandedNodes) {
            val vector = vectors.getVector(id)
            if (vector != null) {
                val similarity = com.pankaj.koredb.hnsw.DistanceMetric.COSINE.compute(query, vector)
                results.add(
                    BridgeResult(
                        id = id,
                        similarity = similarity,
                        node = graph.getNode(id)
                    )
                )
            } else {
                // If a node doesn't have a vector, assign a base similarity
                // so it's still considered but ranked lower than exact matches
                results.add(
                    BridgeResult(
                        id = id,
                        similarity = 0.0f,
                        node = graph.getNode(id)
                    )
                )
            }
        }

        return results.sortedByDescending { it.similarity }.take(finalLimit)
    }

    // ========================================================================
    // VECTOR-FIRST BUILDER
    // ========================================================================

    class VectorFirstBuilder(
        private val graph: GraphStorage,
        private val vectorResults: List<Pair<String, Float>>
    ) {
        /**
         * Filters vector results using a graph predicate.
         * The predicate receives a node ID and returns true to keep it.
         */
        fun filterByGraph(predicate: (String) -> Boolean): List<BridgeResult> {
            return vectorResults
                .filter { (id, _) -> predicate(id) }
                .map { (id, score) ->
                    BridgeResult(
                        id = id,
                        similarity = score,
                        node = graph.getNode(id)
                    )
                }
        }

        /**
         * Enriches vector results with graph node data.
         */
        fun enrichWithGraph(): List<BridgeResult> {
            return vectorResults.map { (id, score) ->
                BridgeResult(
                    id = id,
                    similarity = score,
                    node = graph.getNode(id)
                )
            }
        }

        /**
         * Filters results to only include nodes connected to [targetId] via [edgeType].
         */
        fun connectedTo(targetId: String, edgeType: String): List<BridgeResult> {
            val connectedIds = graph.getInboundSourceIds(targetId, edgeType).toSet() +
                               graph.getOutboundTargetIds(targetId, edgeType).toSet()
            return filterByGraph { it in connectedIds }
        }

        /** Raw results without graph enrichment. */
        fun results(): List<BridgeResult> {
            return vectorResults.map { (id, score) -> BridgeResult(id, score) }
        }
    }

    // ========================================================================
    // GRAPH-FIRST BUILDER
    // ========================================================================

    class GraphFirstBuilder(
        private val graph: GraphStorage,
        private val vectors: KoreVectorCollection,
        private val nodeIds: List<String>
    ) {
        /**
         * Re-ranks graph traversal results by vector similarity to the query.
         * This is the key operation that combines graph structure with semantic meaning.
         */
        suspend fun rerankByVector(query: FloatArray): List<BridgeResult> {
            val results = mutableListOf<BridgeResult>()

            for (id in nodeIds) {
                val vector = vectors.getVector(id)
                if (vector != null) {
                    val similarity = com.pankaj.koredb.hnsw.DistanceMetric.COSINE.compute(query, vector)
                    results.add(BridgeResult(
                        id = id,
                        similarity = similarity,
                        node = graph.getNode(id)
                    ))
                }
            }

            return results.sortedByDescending { it.similarity }
        }

        /**
         * Returns graph-traversal results enriched with node data.
         */
        fun enrichWithGraph(): List<BridgeResult> {
            return nodeIds.map { id ->
                BridgeResult(
                    id = id,
                    similarity = 0f,
                    node = graph.getNode(id)
                )
            }
        }

        /** Returns raw node IDs from the traversal. */
        fun nodeIds(): List<String> = nodeIds
    }
}
