/*
 * Copyright 2026 KoreDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pankaj.koredb.bridge

import com.pankaj.koredb.core.KoreVectorCollection
import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.hnsw.*
import kotlinx.coroutines.*

/**
 * Unified Graph + Vector query bridge with Adaptive Query Planning.
 *
 * This is KoreDB's **unique differentiator** — a combined graph traversal + vector similarity
 * search in a single query API, powered by a cost-based adaptive query optimizer.
 *
 * Usage:
 * ```kotlin
 * val bridge = GraphVectorBridge(graph, vectorCollection)
 *
 * // 1. Adaptive Hybrid Search (Planner decides Graph-First vs Vector-First):
 * val results = bridge.searchAdaptive(
 *     query = imageEmbedding,
 *     targetLimit = 10,
 *     graphPredicate = { nodeId -> graph.hasLabel(nodeId, "Verified") }
 * )
 *
 * // 2. Query Plan Inspection (EXPLAIN):
 * val plan = bridge.explain(imageEmbedding, targetLimit = 10, graphPredicate = { ... })
 * println(plan.explainString())
 * ```
 */
class GraphVectorBridge(
    private val graph: GraphStorage,
    private val vectors: KoreVectorCollection,
    val statsTracker: QueryStatsTracker = QueryStatsTracker(),
    val planner: HybridQueryPlanner = HybridQueryPlanner(statsTracker)
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
        return VectorFirstBuilder(graph, vectors, statsTracker, results)
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
     * Adaptive Vector-First Search: Dynamically over-fetches candidates ($k \to 2k \to 4k$)
     * until [targetLimit] matching items are found or search space is exhausted.
     *
     * @param query Query embedding.
     * @param targetLimit Desired number of passing results.
     * @param predicateTag Optional tag for tracking selectivity across query executions.
     * @param initialK Starting $k$ for HNSW search (defaults to selectivity-informed estimate).
     * @param maxK Upper bound on candidates searched.
     * @param expansionFactor Multiplier applied per iteration if targetLimit is not reached.
     * @param predicate Graph condition each node ID must satisfy.
     */
    suspend fun adaptiveVectorSearch(
        query: FloatArray,
        targetLimit: Int = 10,
        predicateTag: String? = null,
        initialK: Int? = null,
        maxK: Int = 1000,
        expansionFactor: Float = 2.0f,
        predicate: (String) -> Boolean
    ): List<BridgeResult> {
        val startK = initialK ?: statsTracker.calculateInitialK(targetLimit, predicateTag, maxK)
        var currentK = startK.coerceAtMost(maxK)

        val matchedResults = mutableListOf<Pair<String, Float>>()
        val seenIds = mutableSetOf<String>()
        var totalInspected = 0
        var iterations = 0

        while (matchedResults.size < targetLimit && currentK <= maxK) {
            iterations++
            val vectorResults = vectors.search(query, currentK)
            if (vectorResults.isEmpty()) break

            for ((id, score) in vectorResults) {
                if (seenIds.add(id)) {
                    totalInspected++
                    if (predicate(id)) {
                        matchedResults.add(Pair(id, score))
                        if (matchedResults.size >= targetLimit) break
                    }
                }
            }

            // Stop if we received fewer results than requested from HNSW (exhausted dataset)
            if (vectorResults.size < currentK || currentK >= maxK || matchedResults.size >= targetLimit) {
                break
            }

            // Scale up k for next iteration
            val nextK = (currentK * expansionFactor).toInt()
            if (nextK <= currentK) break
            currentK = nextK.coerceAtMost(maxK)
        }

        // Update selectivity statistics
        statsTracker.recordExecution(predicateTag, totalInspected, matchedResults.size)

        // Batch fetch node data for all matched items
        val matchedIds = matchedResults.take(targetLimit).map { it.first }
        val nodeMap = graph.getNodes(matchedIds)

        return matchedResults.take(targetLimit).map { (id, score) ->
            BridgeResult(
                id = id,
                similarity = score,
                node = nodeMap[id]
            )
        }
    }

    /**
     * Adaptive Hybrid Search: Uses cost-based planning to automatically choose
     * between Graph-First and Vector-First execution strategies.
     */
    suspend fun searchAdaptive(
        query: FloatArray,
        targetLimit: Int = 10,
        candidateNodeIds: List<String>? = null,
        predicateTag: String? = null,
        graphPredicate: ((String) -> Boolean)? = null,
        maxK: Int = 1000
    ): List<BridgeResult> {
        val (strategy, _) = planner.chooseStrategy(targetLimit, candidateNodeIds?.size, predicateTag, maxK)

        return when (strategy) {
            QueryStrategy.GRAPH_FIRST -> {
                val nodesToScore = candidateNodeIds ?: emptyList()
                val filteredNodes = if (graphPredicate != null) {
                    nodesToScore.filter(graphPredicate)
                } else {
                    nodesToScore
                }
                GraphFirstBuilder(graph, vectors, filteredNodes)
                    .rerankByVector(query)
                    .take(targetLimit)
            }
            QueryStrategy.VECTOR_FIRST_ADAPTIVE -> {
                val predicate: (String) -> Boolean = { id ->
                    (candidateNodeIds == null || id in candidateNodeIds) &&
                            (graphPredicate == null || graphPredicate(id))
                }
                adaptiveVectorSearch(
                    query = query,
                    targetLimit = targetLimit,
                    predicateTag = predicateTag,
                    maxK = maxK,
                    predicate = predicate
                )
            }
            QueryStrategy.GRAPH_RAG -> {
                graphRAGQuery(query, initialLimit = targetLimit, finalLimit = targetLimit)
            }
        }
    }

    /**
     * Explains the execution plan and cost estimates for a hybrid query.
     */
    suspend fun explain(
        query: FloatArray,
        targetLimit: Int = 10,
        candidateNodeIds: List<String>? = null,
        predicateTag: String? = null,
        graphPredicate: ((String) -> Boolean)? = null,
        maxK: Int = 1000
    ): QueryExecutionPlan {
        val startTime = System.currentTimeMillis()
        val (strategy, costs) = planner.chooseStrategy(targetLimit, candidateNodeIds?.size, predicateTag, maxK)
        val selectivity = statsTracker.estimateSelectivity(predicateTag)
        val initialK = statsTracker.calculateInitialK(targetLimit, predicateTag, maxK)

        val results = searchAdaptive(
            query = query,
            targetLimit = targetLimit,
            candidateNodeIds = candidateNodeIds,
            predicateTag = predicateTag,
            graphPredicate = graphPredicate,
            maxK = maxK
        )
        val elapsed = System.currentTimeMillis() - startTime

        val vectorsScored = when (strategy) {
            QueryStrategy.GRAPH_FIRST -> candidateNodeIds?.size ?: 0
            QueryStrategy.VECTOR_FIRST_ADAPTIVE -> initialK.coerceAtMost(maxK)
            QueryStrategy.GRAPH_RAG -> targetLimit * 2
        }

        return QueryExecutionPlan(
            strategy = strategy,
            estimatedGraphCost = costs.first,
            estimatedVectorCost = costs.second,
            estimatedSelectivity = selectivity,
            initialK = initialK,
            iterations = if (strategy == QueryStrategy.VECTOR_FIRST_ADAPTIVE) 1 else 0,
            vectorsScored = vectorsScored,
            nodesInspected = candidateNodeIds?.size ?: results.size,
            resultCount = results.size,
            actualExecutionTimeMs = elapsed,
            results = results
        )
    }

    /**
     * Executes a full GraphRAG (Graph Retrieval-Augmented Generation) query.
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
        val vectorMap = vectors.getBatchVectors(expandedNodes)
        val nodeMap = graph.getNodes(expandedNodes)
        val results = mutableListOf<BridgeResult>()
        for (id in expandedNodes) {
            val vector = vectorMap[id]
            val node = nodeMap[id]
            if (vector != null) {
                val similarity = com.pankaj.koredb.hnsw.DistanceMetric.COSINE.compute(query, vector)
                results.add(
                    BridgeResult(
                        id = id,
                        similarity = similarity,
                        node = node
                    )
                )
            } else {
                results.add(
                    BridgeResult(
                        id = id,
                        similarity = 0.0f,
                        node = node
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
        private val vectors: KoreVectorCollection,
        private val statsTracker: QueryStatsTracker,
        private val vectorResults: List<Pair<String, Float>>
    ) {
        /**
         * Filters vector results using a graph predicate.
         */
        fun filterByGraph(predicate: (String) -> Boolean): List<BridgeResult> {
            val matched = vectorResults.filter { (id, _) -> predicate(id) }
            val matchedIds = matched.map { it.first }
            val nodeMap = graph.getNodes(matchedIds)
            return matched.map { (id, score) ->
                BridgeResult(
                    id = id,
                    similarity = score,
                    node = nodeMap[id]
                )
            }
        }

        /**
         * Enriches vector results with graph node data.
         */
        fun enrichWithGraph(): List<BridgeResult> {
            val ids = vectorResults.map { it.first }
            val nodeMap = graph.getNodes(ids)
            return vectorResults.map { (id, score) ->
                BridgeResult(
                    id = id,
                    similarity = score,
                    node = nodeMap[id]
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
         */
        suspend fun rerankByVector(query: FloatArray): List<BridgeResult> {
            val vectorMap = vectors.getBatchVectors(nodeIds)
            val nodeMap = graph.getNodes(nodeIds)
            val results = mutableListOf<BridgeResult>()

            for (id in nodeIds) {
                val vector = vectorMap[id]
                if (vector != null) {
                    val similarity = com.pankaj.koredb.hnsw.DistanceMetric.COSINE.compute(query, vector)
                    results.add(BridgeResult(
                        id = id,
                        similarity = similarity,
                        node = nodeMap[id]
                    ))
                }
            }

            return results.sortedByDescending { it.similarity }
        }

        /**
         * Returns graph-traversal results enriched with node data.
         */
        fun enrichWithGraph(): List<BridgeResult> {
            val nodeMap = graph.getNodes(nodeIds)
            return nodeIds.map { id ->
                BridgeResult(
                    id = id,
                    similarity = 0f,
                    node = nodeMap[id]
                )
            }
        }

        /** Returns raw node IDs from the traversal. */
        fun nodeIds(): List<String> = nodeIds
    }

    /**
     * Executes a Hybrid Search query combining BM25 keyword matching and dense vector similarity
     * using Reciprocal Rank Fusion (RRF).
     *
     * @param collection The typed document collection with searchable fields configured.
     * @param queryText The keyword query text for BM25 search.
     * @param queryVector The embedding vector for semantic search.
     * @param limit Maximum number of fused results to return.
     * @param bm25Weight Relative importance weight for BM25 keyword matches (default 1.0).
     * @param vectorWeight Relative importance weight for Vector semantic matches (default 1.0).
     * @param k RRF rank smoothing constant (default 60).
     * @return List of Pair(Document, RRFScore) sorted by fused relevance descending.
     */
    suspend fun <T : Any> searchHybrid(
        collection: com.pankaj.koredb.core.KoreCollection<T>,
        queryText: String,
        queryVector: FloatArray,
        limit: Int = 10,
        bm25Weight: Float = 1.0f,
        vectorWeight: Float = 1.0f,
        k: Int = com.pankaj.koredb.fts.ReciprocalRankFusion.DEFAULT_K
    ): List<Pair<T, Float>> = kotlinx.coroutines.coroutineScope {
        val overfetchLimit = limit * 3

        val bm25Deferred = async(Dispatchers.Default) {
            collection.ftsIndex.search(queryText, limit = overfetchLimit)
        }

        val vectorDeferred = async(Dispatchers.Default) {
            vectors.search(queryVector, limit = overfetchLimit)
        }

        val bm25Results = bm25Deferred.await()
        val vectorResults = vectorDeferred.await()

        val fusedRanks = com.pankaj.koredb.fts.ReciprocalRankFusion.fuse(
            bm25Results = bm25Results,
            vectorResults = vectorResults,
            limit = limit,
            k = k,
            bm25Weight = bm25Weight,
            vectorWeight = vectorWeight
        )

        fusedRanks.mapNotNull { (id, rrfScore) ->
            val doc = collection.getById(id) ?: return@mapNotNull null
            doc to rrfScore
        }
    }
}
