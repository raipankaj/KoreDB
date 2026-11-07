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
                val candidateSet = candidateNodeIds?.let { if (it is Set<*>) @Suppress("UNCHECKED_CAST") (it as Set<String>) else it.toHashSet() }
                val predicate: (String) -> Boolean = { id ->
                    (candidateSet == null || id in candidateSet) &&
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
        val metric = vectors.metric
        for (id in expandedNodes) {
            val vector = vectorMap[id]
            val node = nodeMap[id]
            if (vector != null) {
                val similarity = metric.compute(query, vector)
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
            val metric = vectors.metric

            for (id in nodeIds) {
                val vector = vectorMap[id]
                if (vector != null) {
                    val similarity = metric.compute(query, vector)
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

    /**
     * Executes Hub-Damped Personalized PageRank multi-hop associative retrieval.
     *
     * 1. Dual Retrieval: Vector search (and optional BM25 keyword search if [collection] is provided).
     * 2. Reciprocal Rank Fusion (RRF) to generate seed scores.
     * 3. Hub-Damped Personalized PageRank over graph with hub-noise elimination ($W_{uv} = A_{uv} / d_{in}(v)^\beta$).
     * 4. Passage extraction into [BridgeResult].
     */
    suspend fun associativePageRankQuery(
        queryText: String,
        queryVector: FloatArray,
        collection: com.pankaj.koredb.core.KoreCollection<*>? = null,
        seedLimit: Int = 15,
        maxHops: Int = 2,
        dampingFactor: Double = 0.85,
        hubDampingBeta: Double = 0.5,
        iterations: Int = 20,
        finalLimit: Int = 10,
        targetLabels: Set<String> = setOf("Chunk", "Document")
    ): List<BridgeResult> = kotlinx.coroutines.coroutineScope {
        val overfetch = seedLimit * 2

        val vectorDeferred = async(Dispatchers.Default) {
            vectors.search(queryVector, limit = overfetch)
        }

        val bm25Results = if (collection != null && queryText.isNotBlank()) {
            val bm25Deferred = async(Dispatchers.Default) {
                collection.ftsIndex.search(queryText, limit = overfetch)
            }
            bm25Deferred.await()
        } else {
            emptyList()
        }

        val vectorResults = vectorDeferred.await()

        val seedScores = mutableMapOf<String, Double>()
        if (bm25Results.isNotEmpty()) {
            val fused = com.pankaj.koredb.fts.ReciprocalRankFusion.fuse(
                bm25Results = bm25Results,
                vectorResults = vectorResults,
                limit = seedLimit
            )
            for ((id, score) in fused) {
                seedScores[id] = score.toDouble()
            }
        } else {
            for ((rank, res) in vectorResults.take(seedLimit).withIndex()) {
                val rrfScore = 1.0 / (60.0 + (rank + 1))
                seedScores[res.first] = rrfScore
            }
        }

        val pprRanks = com.pankaj.koredb.graph.algo.GraphAlgorithms.personalizedPageRank(
            storage = graph,
            seedScores = seedScores,
            edgeTypes = null,
            maxHops = maxHops,
            iterations = iterations,
            dampingFactor = dampingFactor,
            hubDampingBeta = hubDampingBeta
        )

        val candidateIds = pprRanks.keys.take(finalLimit * 3)
        val nodeMap = graph.getNodes(candidateIds)

        val results = mutableListOf<BridgeResult>()
        for ((id, score) in pprRanks) {
            val node = nodeMap[id] ?: graph.getNode(id)
            if (targetLabels.isEmpty() || (node != null && node.labels.any { it in targetLabels })) {
                results.add(
                    BridgeResult(
                        id = id,
                        similarity = score.toFloat(),
                        node = node,
                        metadata = node?.properties
                    )
                )
                if (results.size >= finalLimit) break
            }
        }

        if (results.isEmpty() && pprRanks.isNotEmpty()) {
            for ((id, score) in pprRanks.entries.take(finalLimit)) {
                val node = nodeMap[id] ?: graph.getNode(id)
                results.add(
                    BridgeResult(
                        id = id,
                        similarity = score.toFloat(),
                        node = node,
                        metadata = node?.properties
                    )
                )
            }
        }

        results
    }

    suspend fun associativePageRankQuery(
        queryText: String,
        queryVector: FloatArray,
        seedLimit: Int = 15,
        maxHops: Int = 2,
        dampingFactor: Double = 0.85,
        hubDampingBeta: Double = 0.5,
        iterations: Int = 20,
        finalLimit: Int = 10,
        targetLabels: Set<String> = setOf("Chunk", "Document")
    ): List<BridgeResult> = associativePageRankQuery(
        queryText = queryText,
        queryVector = queryVector,
        collection = null,
        seedLimit = seedLimit,
        maxHops = maxHops,
        dampingFactor = dampingFactor,
        hubDampingBeta = hubDampingBeta,
        iterations = iterations,
        finalLimit = finalLimit,
        targetLabels = targetLabels
    )

    // ========================================================================
    // ON-DEVICE AI: AUTO-CHUNKED GRAPHRAG INGESTION, MEMORY & CONTEXT PACKING
    // ========================================================================

    /**
     * Scored episodic/semantic memory result combining vector similarity, exponential temporal decay,
     * and intrinsic memory importance.
     */
    data class MemorySearchResult(
        val id: String,
        val finalScore: Float,
        val similarityScore: Float,
        val recencyScore: Float,
        val importanceScore: Float,
        val node: Node?
    )

    /**
     * Assembled, token-budgeted context payload ready to inject into an on-device SLM prompt.
     */
    data class PromptContextWindow(
        val formattedContext: String,
        val includedNodeIds: List<String>,
        val estimatedTokens: Int,
        val maxTokenBudget: Int,
        val truncatedCount: Int
    )

    /**
     * Executes a Maximal Marginal Relevance (MMR) Diversity Search enriched with graph [Node] data.
     *
     * Balances semantic similarity to [query] against redundancy among selected results:
     * $$\text{MMR}(i) = \lambda \cdot \text{Sim}(q, v_i) - (1 - \lambda) \cdot \max_{j \in S} \text{Sim}(v_i, v_j)$$
     */
    suspend fun searchMMR(
        query: FloatArray,
        limit: Int = 10,
        fetchK: Int = maxOf(limit * 4, 20),
        diversityLambda: Float = 0.5f,
        filter: (VectorFilterBuilder.() -> Unit)? = null
    ): List<BridgeResult> {
        val mmrResults = vectors.searchMMR(
            query = query,
            limit = limit,
            fetchK = fetchK,
            diversityLambda = diversityLambda,
            filterBuilder = filter
        )
        if (mmrResults.isEmpty()) return emptyList()
        val nodeMap = graph.getNodes(mmrResults.map { it.first })
        return mmrResults.map { (id, score) ->
            BridgeResult(id = id, similarity = score, node = nodeMap[id])
        }
    }

    /**
     * Automatically chunks a long document using [com.pankaj.koredb.ai.KoreTextChunker],
     * stores the parent `Document` node and child `Chunk` nodes in [GraphStorage],
     * links them with `HAS_CHUNK`, `CHUNK_OF`, `NEXT_CHUNK`, and `PREV_CHUNK` edges,
     * optionally extracts shared `Entity` nodes (`Chunk -[MENTIONS]-> Entity -[MENTIONED_IN]-> Chunk`)
     * for cross-document multi-hop GraphRAG discovery, and indexes each chunk's embedding in [KoreVectorCollection].
     *
     * @param documentId Unique ID for the parent document node.
     * @param title Human-readable document title.
     * @param text Full document body text to split and index.
     * @param maxChunkChars Maximum characters per chunk (default 512).
     * @param overlapChars Overlap characters between consecutive chunks (default 64).
     * @param metadata Optional additional properties stored on the parent document node.
     * @param extractEntities Whether to extract shared `Entity` graph nodes from each chunk (default `true`).
     * @param entityExtractor Function that extracts [com.pankaj.koredb.ai.KoreEntityExtractor.EntityMention]s from chunk text.
     * @param embedder Suspend callback that computes an embedding vector for a chunk string.
     * @return List of generated chunk node IDs (`"${documentId}#chunk_0"`, etc.).
     */
    suspend fun ingestDocumentWithChunks(
        documentId: String,
        title: String,
        text: String,
        maxChunkChars: Int = 512,
        overlapChars: Int = 64,
        metadata: Map<String, String> = emptyMap(),
        extractEntities: Boolean = true,
        entityExtractor: (String) -> List<com.pankaj.koredb.ai.KoreEntityExtractor.EntityMention> =
            com.pankaj.koredb.ai.KoreEntityExtractor::extract,
        embedder: suspend (String) -> FloatArray
    ): List<String> {
        val chunks = com.pankaj.koredb.ai.KoreTextChunker.chunk(
            text = text,
            maxChunkChars = maxChunkChars,
            overlapChars = overlapChars
        )

        val docProps = HashMap<String, String>(metadata.size + 3)
        docProps.putAll(metadata)
        docProps["title"] = title
        docProps["chunkCount"] = chunks.size.toString()
        graph.putNode(Node(id = documentId, labels = setOf("Document"), properties = docProps))

        if (chunks.isEmpty()) return emptyList()

        val chunkIds = ArrayList<String>(chunks.size)
        val vectorBatch = LinkedHashMap<String, FloatArray>(chunks.size)
        val seenDocEntities = HashSet<String>()

        for (i in chunks.indices) {
            val chunk = chunks[i]
            val chunkId = "${documentId}#chunk_${chunk.index}"
            chunkIds.add(chunkId)

            val chunkNode = Node(
                id = chunkId,
                labels = setOf("Chunk"),
                properties = mapOf(
                    "documentId" to documentId,
                    "title" to title,
                    "text" to chunk.text,
                    "chunkIndex" to chunk.index.toString(),
                    "startOffset" to chunk.startOffset.toString(),
                    "endOffset" to chunk.endOffset.toString(),
                    "estimatedTokens" to chunk.estimatedTokens.toString()
                )
            )
            graph.putNode(chunkNode)
            graph.putEdge(com.pankaj.koredb.graph.Edge(documentId, chunkId, "HAS_CHUNK"))
            graph.putEdge(com.pankaj.koredb.graph.Edge(chunkId, documentId, "CHUNK_OF"))

            if (i > 0) {
                val prevId = chunkIds[i - 1]
                graph.putEdge(com.pankaj.koredb.graph.Edge(prevId, chunkId, "NEXT_CHUNK"))
                graph.putEdge(com.pankaj.koredb.graph.Edge(chunkId, prevId, "PREV_CHUNK"))
            }

            if (extractEntities) {
                val entities = entityExtractor(chunk.text)
                for (entity in entities) {
                    val entityNode = Node(
                        id = entity.id,
                        labels = setOf("Entity", entity.type),
                        properties = mapOf(
                            "name" to entity.name,
                            "title" to entity.name,
                            "entityType" to entity.type
                        )
                    )
                    graph.putNode(entityNode)
                    graph.putEdge(com.pankaj.koredb.graph.Edge(chunkId, entity.id, "MENTIONS"))
                    graph.putEdge(com.pankaj.koredb.graph.Edge(entity.id, chunkId, "MENTIONED_IN"))
                    if (seenDocEntities.add(entity.id)) {
                        graph.putEdge(com.pankaj.koredb.graph.Edge(documentId, entity.id, "MENTIONS_ENTITY"))
                    }
                }
            }

            vectorBatch[chunkId] = embedder(chunk.text)
        }

        vectors.insertBatch(vectorBatch)
        vectors.waitForIndexing()
        return chunkIds
    }

    /**
     * Agentic Episodic Memory Retrieval with Exponential Temporal Decay and Importance Weighting.
     *
     * Computes a unified memory relevance score:
     * $$\text{Score}(m) = w_{\text{sim}} \cdot \text{Sim}(q, m) + w_{\text{recency}} \cdot 2^{-\Delta t / T_{1/2}} + w_{\text{importance}} \cdot \text{Importance}(m)$$
     *
     * @param query Query embedding vector.
     * @param limit Number of top memories to return.
     * @param nowMs Reference timestamp in milliseconds (defaults to `System.currentTimeMillis()`).
     * @param halfLifeHours Half-life $T_{1/2}$ in hours at which the recency score decays to `0.5` (default `24.0` hours).
     * @param similarityWeight Weight $w_{\text{sim}}$ for semantic similarity (default `0.65f`).
     * @param recencyWeight Weight $w_{\text{recency}}$ for temporal recency (default `0.20f`).
     * @param importanceWeight Weight $w_{\text{importance}}$ for memory importance (default `0.15f`).
     * @param timestampPropertyKey Node property key storing the creation timestamp in ms (default `"timestampMs"`).
     * @param importancePropertyKey Node property key storing importance in $[0.0, 1.0]$ (default `"importance"`).
     */
    suspend fun searchMemory(
        query: FloatArray,
        limit: Int = 10,
        nowMs: Long = System.currentTimeMillis(),
        halfLifeHours: Double = 24.0,
        similarityWeight: Float = 0.65f,
        recencyWeight: Float = 0.20f,
        importanceWeight: Float = 0.15f,
        timestampPropertyKey: String = "timestampMs",
        importancePropertyKey: String = "importance"
    ): List<MemorySearchResult> {
        val candidateLimit = (limit * 4).coerceAtLeast(20)
        val rawCandidates = vectors.search(query, candidateLimit)
        if (rawCandidates.isEmpty()) return emptyList()

        val nodeMap = graph.getNodes(rawCandidates.map { it.first })
        val safeHalfLife = halfLifeHours.coerceAtLeast(1e-6)
        val decayLambda = kotlin.math.ln(2.0) / safeHalfLife

        val scored = ArrayList<MemorySearchResult>(rawCandidates.size)
        for ((id, sim) in rawCandidates) {
            val node = nodeMap[id]
            val createdAtMs = node?.properties?.get(timestampPropertyKey)?.toLongOrNull() ?: nowMs
            val ageHours = ((nowMs - createdAtMs).coerceAtLeast(0L)).toDouble() / 3_600_000.0
            val recency = kotlin.math.exp(-decayLambda * ageHours).toFloat()

            val importance = (node?.properties?.get(importancePropertyKey)?.toFloatOrNull() ?: 0.5f)
                .coerceIn(0.0f, 1.0f)

            val finalScore = (similarityWeight * sim) +
                    (recencyWeight * recency) +
                    (importanceWeight * importance)

            scored.add(
                MemorySearchResult(
                    id = id,
                    finalScore = finalScore,
                    similarityScore = sim,
                    recencyScore = recency,
                    importanceScore = importance,
                    node = node
                )
            )
        }

        scored.sortByDescending { it.finalScore }
        return if (scored.size <= limit) scored else scored.subList(0, limit)
    }

    /**
     * Token-Budgeted Context Window Assembler for On-Device Small Language Models (SLMs).
     *
     * Executes a GraphRAG retrieval (with optional MMR diversity reranking via [diversityLambda])
     * and packs the highest-ranked passages plus their 1-hop knowledge-graph relationships into a
     * deduplicated, structured prompt context guaranteed not to exceed [maxTokenBudget].
     *
     * @param query Query embedding vector.
     * @param maxTokenBudget Maximum token budget for the assembled context block (default `1024`).
     * @param candidateLimit Number of initial semantic seed nodes to retrieve (default `10`).
     * @param maxHops Graph expansion depth (default `1`).
     * @param includeGraphRelationships Whether to append 1-hop graph edges for included nodes (default `true`).
     * @param diversityLambda Optional MMR diversity trade-off $\lambda \in [0.0, 1.0]$ (`null` = pure similarity ordering; e.g. `0.5f` balances relevance and diversity).
     * @param textPropertyKeys Ordered list of node property keys to inspect for passage text.
     */
    suspend fun assemblePromptContext(
        query: FloatArray,
        maxTokenBudget: Int = 1024,
        candidateLimit: Int = 10,
        maxHops: Int = 1,
        includeGraphRelationships: Boolean = true,
        diversityLambda: Float? = null,
        textPropertyKeys: List<String> = listOf("text", "content", "summary", "body")
    ): PromptContextWindow {
        require(maxTokenBudget > 0) { "maxTokenBudget must be positive" }

        val rawRagResults = graphRAGQuery(
            query = query,
            initialLimit = candidateLimit,
            maxHops = maxHops,
            finalLimit = candidateLimit * 3
        )

        val ragResults = if (diversityLambda != null && rawRagResults.size > 1) {
            diversifyBridgeResultsWithMMR(rawRagResults, diversityLambda.coerceIn(0.0f, 1.0f))
        } else {
            rawRagResults
        }

        val sb = StringBuilder()
        val includedIds = ArrayList<String>()
        val seenTexts = HashSet<String>()
        var usedTokens = 0
        var truncatedCount = 0

        for (result in ragResults) {
            val node = result.node ?: continue
            // Skip standalone structural Entity nodes that don't carry passage text of their own
            if ("Entity" in node.labels && textPropertyKeys.none { node.properties.containsKey(it) }) {
                continue
            }
            val rawText = textPropertyKeys.firstNotNullOfOrNull { key ->
                node.properties[key]?.takeIf { it.isNotBlank() }
            } ?: node.properties.entries.joinToString(", ") { "${it.key}: ${it.value}" }

            if (rawText.isBlank() || !seenTexts.add(rawText)) continue

            val titlePrefix = node.properties["title"]?.let { "[$it] " } ?: "[${node.id}] "
            val relationSuffix = if (includeGraphRelationships) {
                val edges = graph.getAllOutboundEdges(node.id)
                    .filter { it.type != "NEXT_CHUNK" && it.type != "PREV_CHUNK" && it.type != "HAS_CHUNK" }
                    .take(5)
                if (edges.isNotEmpty()) {
                    "\n  Relations: " + edges.joinToString("; ") { "${node.id} -[${it.type}]-> ${it.targetId}" }
                } else ""
            } else ""

            val entryBlock = "- $titlePrefix$rawText$relationSuffix\n"
            val entryTokens = com.pankaj.koredb.ai.KoreTextChunker.estimateTokens(entryBlock)

            if (usedTokens + entryTokens > maxTokenBudget) {
                truncatedCount++
                continue
            }

            sb.append(entryBlock)
            usedTokens += entryTokens
            includedIds.add(node.id)
        }

        return PromptContextWindow(
            formattedContext = sb.toString().trimEnd(),
            includedNodeIds = includedIds,
            estimatedTokens = usedTokens,
            maxTokenBudget = maxTokenBudget,
            truncatedCount = truncatedCount
        )
    }

    private suspend fun diversifyBridgeResultsWithMMR(
        candidates: List<BridgeResult>,
        lam: Float
    ): List<BridgeResult> {
        if (candidates.size <= 1 || lam >= 0.999f) return candidates
        val vecMap = vectors.getBatchVectors(candidates.map { it.id })
        val metric = vectors.metric
        val selected = ArrayList<BridgeResult>(candidates.size)
        val selectedVectors = ArrayList<FloatArray>(candidates.size)
        val remaining = candidates.toMutableList()

        val first = remaining.removeAt(0)
        selected.add(first)
        vecMap[first.id]?.let { selectedVectors.add(it) }

        while (remaining.isNotEmpty()) {
            var bestIdx = 0
            var bestScore = -Float.MAX_VALUE
            for (i in remaining.indices) {
                val cand = remaining[i]
                val candVec = vecMap[cand.id]
                var maxSimToSelected = 0.0f
                if (candVec != null && selectedVectors.isNotEmpty()) {
                    for (selVec in selectedVectors) {
                        val sim = metric.compute(candVec, selVec)
                        if (sim > maxSimToSelected) maxSimToSelected = sim
                    }
                }
                val mmr = (lam * cand.similarity) - ((1.0f - lam) * maxSimToSelected)
                if (mmr > bestScore) {
                    bestScore = mmr
                    bestIdx = i
                }
            }
            val picked = remaining.removeAt(bestIdx)
            selected.add(picked)
            vecMap[picked.id]?.let { selectedVectors.add(it) }
        }
        return selected
    }

    /**
     * Symbolic Working Memory Navigation: Navigates from high-level clusters/nodes
     * down to L0 raw evidence on demand.
     *
     * @param rootNodeId Node identifier to start drill-down from (L3, L2, or L1).
     * @param maxEvidencePerFact Maximum number of raw evidence items to retrieve per atomic fact.
     * @param pyramid Optional reference to [com.pankaj.koredb.memory.KoreMemoryPyramid] for decompression.
     * @return Structured [com.pankaj.koredb.memory.DrillDownResult].
     */
    suspend fun drillDown(
        rootNodeId: String,
        maxEvidencePerFact: Int = 5,
        pyramid: com.pankaj.koredb.memory.KoreMemoryPyramid? = null
    ): com.pankaj.koredb.memory.DrillDownResult {
        val rootNode = graph.getNode(rootNodeId)
        val rootLevel = when {
            rootNode == null -> when {
                rootNodeId.startsWith("l3:persona") -> com.pankaj.koredb.memory.MemoryLevel.L3_PERSONA
                rootNodeId.startsWith("l3:inv") -> com.pankaj.koredb.memory.MemoryLevel.L3_INVARIANT
                rootNodeId.startsWith("l2:") -> com.pankaj.koredb.memory.MemoryLevel.L2_SCENARIO
                rootNodeId.startsWith("l1:") -> com.pankaj.koredb.memory.MemoryLevel.L1_FACT
                rootNodeId.startsWith("l0:") -> com.pankaj.koredb.memory.MemoryLevel.L0_EVIDENCE
                else -> com.pankaj.koredb.memory.MemoryLevel.UNKNOWN
            }
            rootNode.labels.any { it == "L3_Persona" || it.contains("L3_Persona") } -> com.pankaj.koredb.memory.MemoryLevel.L3_PERSONA
            rootNode.labels.any { it == "L3_Invariant" || it.contains("L3_Invariant") } -> com.pankaj.koredb.memory.MemoryLevel.L3_INVARIANT
            rootNode.labels.any { it == "L2_Scenario" || it.contains("L2") } -> com.pankaj.koredb.memory.MemoryLevel.L2_SCENARIO
            rootNode.labels.any { it == "L1_Fact" || it.contains("L1") } -> com.pankaj.koredb.memory.MemoryLevel.L1_FACT
            rootNode.labels.any { it == "L0_Evidence" || it.contains("L0") } -> com.pankaj.koredb.memory.MemoryLevel.L0_EVIDENCE
            rootNodeId.startsWith("l3:persona") -> com.pankaj.koredb.memory.MemoryLevel.L3_PERSONA
            rootNodeId.startsWith("l3:inv") -> com.pankaj.koredb.memory.MemoryLevel.L3_INVARIANT
            rootNodeId.startsWith("l2:") -> com.pankaj.koredb.memory.MemoryLevel.L2_SCENARIO
            rootNodeId.startsWith("l1:") -> com.pankaj.koredb.memory.MemoryLevel.L1_FACT
            rootNodeId.startsWith("l0:") -> com.pankaj.koredb.memory.MemoryLevel.L0_EVIDENCE
            else -> com.pankaj.koredb.memory.MemoryLevel.UNKNOWN
        }

        val scenarioClusters = mutableListOf<com.pankaj.koredb.memory.L2ScenarioCluster>()
        val targetFactIds = mutableSetOf<String>()

        when (rootLevel) {
            com.pankaj.koredb.memory.MemoryLevel.L3_PERSONA,
            com.pankaj.koredb.memory.MemoryLevel.L3_INVARIANT -> {
                val governedIds = graph.getOutboundTargetIds(rootNodeId, com.pankaj.koredb.memory.MemoryEdgeTypes.GOVERNS).toMutableList()
                var idx = 0
                while (idx < governedIds.size) {
                    val gId = governedIds[idx++]
                    if (gId.startsWith("l3:")) {
                        val subGoverned = graph.getOutboundTargetIds(gId, com.pankaj.koredb.memory.MemoryEdgeTypes.GOVERNS)
                        for (sg in subGoverned) {
                            if (sg !in governedIds) governedIds.add(sg)
                        }
                    } else if (gId.startsWith("l2:") || graph.getNode(gId)?.labels?.any { it.startsWith("L2") } == true) {
                        val clusterNode = graph.getNode(gId)
                        val memberFacts = graph.getOutboundTargetIds(gId, com.pankaj.koredb.memory.MemoryEdgeTypes.CLUSTERS_FACT)
                        targetFactIds.addAll(memberFacts)
                        scenarioClusters.add(
                            com.pankaj.koredb.memory.L2ScenarioCluster(
                                clusterId = gId.removePrefix("l2:cluster_").removePrefix("l2:"),
                                title = clusterNode?.properties?.get("title") ?: gId,
                                memberFactIds = memberFacts.map { it.removePrefix("l1:") },
                                centroidEmbedding = null
                            )
                        )
                    } else if (gId.startsWith("l1:") || graph.getNode(gId)?.labels?.any { it.startsWith("L1") } == true) {
                        targetFactIds.add(gId)
                    }
                }
            }
            com.pankaj.koredb.memory.MemoryLevel.L2_SCENARIO -> {
                val memberFacts = graph.getOutboundTargetIds(rootNodeId, com.pankaj.koredb.memory.MemoryEdgeTypes.CLUSTERS_FACT)
                targetFactIds.addAll(memberFacts)
                if (rootNode != null || memberFacts.isNotEmpty()) {
                    scenarioClusters.add(
                        com.pankaj.koredb.memory.L2ScenarioCluster(
                            clusterId = rootNodeId.removePrefix("l2:cluster_").removePrefix("l2:"),
                            title = rootNode?.properties?.get("title") ?: rootNodeId,
                            memberFactIds = memberFacts.map { it.removePrefix("l1:") },
                            centroidEmbedding = null
                        )
                    )
                }
            }
            com.pankaj.koredb.memory.MemoryLevel.L1_FACT -> {
                targetFactIds.add(rootNodeId)
            }
            else -> {
                if (rootNodeId.startsWith("l1:")) targetFactIds.add(rootNodeId)
            }
        }

        val factsWithEvidence = mutableListOf<com.pankaj.koredb.memory.FactWithEvidence>()
        var totalEvidence = 0

        for (fNodeId in targetFactIds) {
            val fNode = graph.getNode(fNodeId) ?: continue
            val fText = fNode.properties["text"] ?: ""
            val fConf = fNode.properties["confidence"]?.toFloatOrNull()
                ?: fNode.properties["certainty"]?.toFloatOrNull()
                ?: 1.0f

            val evSourceIds = graph.getInboundSourceIds(fNodeId, com.pankaj.koredb.memory.MemoryEdgeTypes.EVIDENCE_FOR)
                .take(maxEvidencePerFact)

            val decompressedList = evSourceIds.mapNotNull { evNodeId ->
                val rawEvId = evNodeId.removePrefix("l0:")
                if (pyramid != null) {
                    pyramid.getDecompressedEvidence(rawEvId)
                } else {
                    val evNode = graph.getNode(evNodeId)
                    if (evNode != null) {
                        val summary = evNode.properties["summary"] ?: ""
                        com.pankaj.koredb.memory.DecompressedEvidence(
                            evidenceId = rawEvId,
                            rawContent = summary,
                            rawBytes = summary.toByteArray(Charsets.UTF_8),
                            mimeType = evNode.properties["mimeType"] ?: "text/plain",
                            timestampMs = evNode.properties["timestampMs"]?.toLongOrNull() ?: System.currentTimeMillis(),
                            originalSize = evNode.properties["originalSize"]?.toIntOrNull() ?: summary.length,
                            compressedSize = evNode.properties["compressedSize"]?.toIntOrNull() ?: summary.length,
                            sourceUri = evNode.properties["sourceUri"],
                            metadata = evNode.properties
                        )
                    } else null
                }
            }
            totalEvidence += decompressedList.size
            factsWithEvidence.add(
                com.pankaj.koredb.memory.FactWithEvidence(
                    factId = fNode.properties["factId"] ?: fNode.properties["id"] ?: fNodeId.removePrefix("l1:"),
                    text = fText,
                    confidence = fConf,
                    properties = fNode.properties,
                    evidenceList = decompressedList
                )
            )
        }

        return com.pankaj.koredb.memory.DrillDownResult(
            rootNodeId = rootNodeId,
            rootLevel = rootLevel,
            scenarioClusters = scenarioClusters,
            atomicFacts = factsWithEvidence,
            totalEvidenceCount = totalEvidence
        )
    }
}

