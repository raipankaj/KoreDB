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

package com.pankaj.koredb.graph.algo

import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.graph.Node
import java.util.PriorityQueue

/**
 * A collection of standard graph algorithms optimized for the KoreDB [GraphStorage] engine.
 *
 * These algorithms leverage the underlying storage engine's ability to efficiently 
 * retrieve node and edge information. Where possible, they utilize fast path methods 
 * that avoid full JSON deserialization to maximize performance.
 */
object GraphAlgorithms {

    /**
     * Performs a Breadth-First Search (BFS) traversal starting from [startNodeId].
     *
     * This implementation yields nodes lazily using a [Sequence], allowing for 
     * early termination without processing the entire reachable subgraph.
     *
     * @param storage The graph storage engine to query.
     * @param startNodeId The ID of the node where the search begins.
     * @param edgeType The type of relationship to traverse.
     * @return A lazy sequence of [Node] objects in BFS order.
     */
    fun bfs(storage: GraphStorage, startNodeId: String, edgeType: String): Sequence<Node> = sequence {
        val visited = mutableSetOf<String>()
        val queue = ArrayDeque<String>()

        queue.add(startNodeId)
        visited.add(startNodeId)

        while (queue.isNotEmpty()) {
            val currId = queue.removeFirst()
            val node = storage.getNode(currId)
            if (node != null) {
                yield(node)

                // Optimization: Directly retrieve target IDs to avoid the overhead of 
                // full edge object creation and JSON property parsing.
                val targetIds = storage.getOutboundTargetIds(currId, edgeType)
                for (targetId in targetIds) {
                    if (visited.add(targetId)) {
                        queue.add(targetId)
                    }
                }
            }
        }
    }

    /**
     * Performs a Depth-First Search (DFS) traversal starting from [startNodeId].
     *
     * Similar to [bfs], this returns a [Sequence] for lazy evaluation.
     *
     * @param storage The graph storage engine to query.
     * @param startNodeId The ID of the node where the search begins.
     * @param edgeType The type of relationship to traverse.
     * @return A lazy sequence of [Node] objects in DFS order.
     */
    fun dfs(storage: GraphStorage, startNodeId: String, edgeType: String): Sequence<Node> = sequence {
        val visited = mutableSetOf<String>()
        val stack = ArrayDeque<String>()

        stack.addLast(startNodeId)

        while (stack.isNotEmpty()) {
            val currId = stack.removeLast()
            if (visited.add(currId)) {
                val node = storage.getNode(currId)
                if (node != null) {
                    yield(node)

                    // Optimization: Directly retrieve target IDs. 
                    // Reversed to maintain traditional left-to-right order when using a stack.
                    val targetIds = storage.getOutboundTargetIds(currId, edgeType)
                    for (targetId in targetIds.reversed()) {
                        if (!visited.contains(targetId)) {
                            stack.addLast(targetId)
                        }
                    }
                }
            }
        }
    }

    /**
     * Calculates the shortest path between two nodes using Dijkstra's Algorithm.
     *
     * This algorithm finds the path with the minimum cumulative weight based on 
     * the specified [weightProperty].
     *
     * @param storage The graph storage engine to query.
     * @param startNodeId The starting node's identifier.
     * @param endNodeId The target node's identifier.
     * @param edgeType The type of relationship to traverse.
     * @param weightProperty The edge property to use as the traversal cost (default is "weight").
     * @return A list of [Edge] objects forming the shortest path, or null if no path exists.
     */
    fun shortestPathDijkstra(
        storage: GraphStorage,
        startNodeId: String,
        endNodeId: String,
        edgeType: String,
        weightProperty: String = "weight"
    ): List<Edge>? {
        val distances = mutableMapOf<String, Double>().withDefault { Double.POSITIVE_INFINITY }
        val previousEdge = mutableMapOf<String, Edge>()

        val pq = PriorityQueue<Pair<String, Double>>(compareBy { it.second })

        distances[startNodeId] = 0.0
        pq.add(Pair(startNodeId, 0.0))

        while (pq.isNotEmpty()) {
            val (currId, currentDist) = pq.poll() ?: break

            if (currId == endNodeId) {
                val path = mutableListOf<Edge>()
                var step = endNodeId
                while (step != startNodeId) {
                    val edge = previousEdge[step] ?: break
                    path.add(edge)
                    step = edge.sourceId
                }
                path.reverse()
                return path
            }

            if (currentDist > distances.getValue(currId)) continue

            // Note: Full edge objects are retrieved here as their property maps 
            // must be inspected to extract the traversal weights.
            val outboundEdges = storage.getOutboundEdges(currId, edgeType)
            for (edge in outboundEdges) {
                val weight = edge.properties[weightProperty]?.toDoubleOrNull() ?: 1.0
                val newDist = currentDist + weight

                if (newDist < distances.getValue(edge.targetId)) {
                    distances[edge.targetId] = newDist
                    previousEdge[edge.targetId] = edge
                    pq.add(Pair(edge.targetId, newDist))
                }
            }
        }
        return null
    }

    /**
     * Calculates node centrality within a subgraph using the PageRank algorithm.
     *
     * PageRank estimates the importance of a node based on the quality and quantity 
     * of inbound links from other nodes within the specified [seedNodes] set.
     *
     * @param storage The graph storage engine to query.
     * @param seedNodes The subset of nodes to participate in the ranking.
     * @param edgeType The type of relationship defining the links.
     * @param iterations The number of power iteration steps (default is 10).
     * @param dampingFactor The probability of following a link vs. jumping to a random node (default is 0.85).
     * @return A map of node IDs to their calculated centrality scores.
     */
    fun pageRank(
        storage: GraphStorage,
        seedNodes: List<String>,
        edgeType: String,
        iterations: Int = 10,
        dampingFactor: Double = 0.85
    ): Map<String, Double> {
        val n = seedNodes.size
        if (n == 0) return emptyMap()

        val initialRank = 1.0 / n
        var ranks = seedNodes.associateWith { initialRank }.toMutableMap()

        // Pre-compute out-degree for performance.
        val outDegree = mutableMapOf<String, Int>()
        for (nodeId in seedNodes) {
            // Optimization: Count outgoing links using fast ID-only lookups.
            outDegree[nodeId] = storage.getOutboundTargetIds(nodeId, edgeType).count { it in seedNodes }
        }

        for (i in 0 until iterations) {
            val nextRanks = mutableMapOf<String, Double>()
            var danglingSum = 0.0

            // Account for nodes with no outbound links (sinks).
            for (nodeId in seedNodes) {
                if ((outDegree[nodeId] ?: 0) == 0) {
                    danglingSum += ranks[nodeId] ?: 0.0
                }
            }

            for (nodeId in seedNodes) {
                var rankSum = 0.0

                // Optimization: Identify inbound links using fast source ID retrieval.
                val sourceIds = storage.getInboundSourceIds(nodeId, edgeType)

                for (sourceId in sourceIds) {
                    if (sourceId in seedNodes) {
                        val sourceOutDegree = outDegree[sourceId] ?: 0
                        if (sourceOutDegree > 0) {
                            rankSum += (ranks[sourceId] ?: 0.0) / sourceOutDegree
                        }
                    }
                }

                val newRank = ((1.0 - dampingFactor) / n) + dampingFactor * (rankSum + danglingSum / n)
                nextRanks[nodeId] = newRank
            }
            ranks = nextRanks
        }

        return ranks
    }

    /**
     * Executes Hub-Damped Personalized PageRank (PPR) with hub-noise elimination.
     *
     * Propagates activation from [seedScores] across the graph, damping transitions into
     * high-degree hub nodes to prevent generic concepts from drowning out multi-hop reasoning.
     *
     * @param storage The graph storage engine.
     * @param seedScores Initial relevance scores from RRF (seedNodeId -> score).
     * @param edgeTypes Optional filter for edge types (null = traverse all edges).
     * @param maxHops Subgraph BFS depth from seeds (default 2).
     * @param iterations Maximum power iteration steps (default 20).
     * @param dampingFactor Probability of traversing links vs teleporting to seeds (default 0.85).
     * @param hubDampingBeta Degree normalization exponent β in [0.0, 1.0] (default 0.5 for symmetric damping).
     * @param tolerance L1 convergence threshold (default 1e-5).
     * @param bidirectional If true, traverses both outbound and inbound edges as candidate transitions.
     * @return Map of nodeId to converged personalized PageRank score, sorted descending.
     */
    fun personalizedPageRank(
        storage: GraphStorage,
        seedScores: Map<String, Double>,
        edgeTypes: Collection<String>? = null,
        maxHops: Int = 2,
        iterations: Int = 20,
        dampingFactor: Double = 0.85,
        hubDampingBeta: Double = 0.5,
        tolerance: Double = 1e-5,
        bidirectional: Boolean = false
    ): Map<String, Double> {
        if (seedScores.isEmpty()) return emptyMap()

        // 1. Normalize seed scores into teleportation probability vector p_0
        val totalSeedScore = seedScores.values.sum()
        if (totalSeedScore <= 0.0) return emptyMap()
        val p0 = seedScores.mapValues { it.value / totalSeedScore }

        // 2. Extract active H-hop subgraph around seed nodes via BFS
        val activeNodes = mutableSetOf<String>()
        activeNodes.addAll(seedScores.keys)

        val edgeTypeSet = edgeTypes?.toSet()
        var frontier = seedScores.keys.toSet()
        for (h in 0 until maxHops) {
            val nextFrontier = mutableSetOf<String>()
            for (id in frontier) {
                val outTargets = if (edgeTypeSet == null) {
                    storage.getAllOutboundEdges(id).map { it.targetId }
                } else {
                    edgeTypeSet.flatMap { storage.getOutboundTargetIds(id, it) }
                }
                val inSources = if (edgeTypeSet == null) {
                    storage.getAllInboundEdges(id).map { it.sourceId }
                } else {
                    edgeTypeSet.flatMap { storage.getInboundSourceIds(id, it) }
                }
                for (neighbor in (outTargets + inSources)) {
                    if (activeNodes.add(neighbor)) {
                        nextFrontier.add(neighbor)
                    }
                }
            }
            frontier = nextFrontier
            if (frontier.isEmpty()) break
        }

        val n = activeNodes.size
        if (n == 0) return emptyMap()

        // 3. Compute in-degree and adjacency within active subgraph
        val inDegree = mutableMapOf<String, Int>()
        val adjacency = mutableMapOf<String, List<Pair<String, Double>>>()

        for (nodeId in activeNodes) {
            val outbound = if (edgeTypeSet == null) {
                storage.getAllOutboundEdges(nodeId).filter { it.targetId in activeNodes }
            } else {
                edgeTypeSet.flatMap { storage.getOutboundEdges(nodeId, it) }.filter { it.targetId in activeNodes }
            }

            val edges = if (bidirectional) {
                val inbound = if (edgeTypeSet == null) {
                    storage.getAllInboundEdges(nodeId).filter { it.sourceId in activeNodes }
                } else {
                    edgeTypeSet.flatMap { storage.getInboundEdges(nodeId, it) }.filter { it.sourceId in activeNodes }
                }
                val mappedInbound = inbound.map { Edge(sourceId = nodeId, targetId = it.sourceId, type = it.type, properties = it.properties) }
                outbound + mappedInbound
            } else {
                outbound
            }

            val edgePairs = edges.map { edge ->
                val w = edge.properties["weight"]?.toDoubleOrNull() ?: 1.0
                edge.targetId to w
            }
            adjacency[nodeId] = edgePairs
            for ((target, _) in edgePairs) {
                inDegree[target] = (inDegree[target] ?: 0) + 1
            }
        }

        // 4. Construct hub-noise damped transition matrix T
        // W(u -> v) = A_uv / (d_in(v)^beta)
        // T_vu = W(u -> v) / sum_v' W(u -> v')
        val transitionProb = mutableMapOf<String, MutableMap<String, Double>>()
        val danglingNodes = mutableSetOf<String>()

        for (u in activeNodes) {
            val outEdges = adjacency[u] ?: emptyList()
            if (outEdges.isEmpty()) {
                danglingNodes.add(u)
                continue
            }

            var weightSum = 0.0
            val weights = mutableMapOf<String, Double>()
            for ((v, rawWeight) in outEdges) {
                val degV = (inDegree[v] ?: 1).coerceAtLeast(1)
                val dampedW = rawWeight / Math.pow(degV.toDouble(), hubDampingBeta)
                weights[v] = (weights[v] ?: 0.0) + dampedW
                weightSum += dampedW
            }

            if (weightSum > 0.0) {
                val probs = mutableMapOf<String, Double>()
                for ((v, w) in weights) {
                    probs[v] = w / weightSum
                }
                transitionProb[u] = probs
            } else {
                danglingNodes.add(u)
            }
        }

        // Invert transition matrix for fast pull-based PageRank: v -> list of (u, T_vu)
        val incomingTransitions = mutableMapOf<String, MutableList<Pair<String, Double>>>()
        for ((u, targets) in transitionProb) {
            for ((v, prob) in targets) {
                incomingTransitions.getOrPut(v) { mutableListOf() }.add(u to prob)
            }
        }

        // 5. Power iteration
        var p = HashMap<String, Double>(n)
        for (node in activeNodes) {
            p[node] = p0[node] ?: 0.0
        }

        for (iter in 0 until iterations) {
            val nextP = HashMap<String, Double>(n)
            var danglingSum = 0.0
            for (u in danglingNodes) {
                danglingSum += p[u] ?: 0.0
            }

            var diff = 0.0
            for (v in activeNodes) {
                var inSum = 0.0
                val inList = incomingTransitions[v]
                if (inList != null) {
                    for (edge in inList) {
                        val u = edge.first
                        val prob = edge.second
                        inSum += (p[u] ?: 0.0) * prob
                    }
                }

                val tele = p0[v] ?: 0.0
                val newRank = (dampingFactor * (inSum + (danglingSum * tele))) + ((1.0 - dampingFactor) * tele)
                nextP[v] = newRank
                diff += kotlin.math.abs(newRank - (p[v] ?: 0.0))
            }

            p = nextP
            if (diff < tolerance) break
        }

        return p.entries.sortedByDescending { it.value }.associate { it.key to it.value }
    }

    // ========================================================================
    // A* PATHFINDING
    // ========================================================================

    /**
     * Finds the shortest path between two nodes using the A* algorithm.
     *
     * A* improves upon Dijkstra by using a heuristic function to guide the search
     * toward the goal, significantly reducing the number of nodes explored.
     *
     * @param storage The graph storage engine.
     * @param startNodeId The starting node's identifier.
     * @param endNodeId The target node's identifier.
     * @param edgeType The type of relationship to traverse.
     * @param weightProperty The edge property to use as cost (default: "weight").
     * @param heuristic A function estimating the cost from a node to the goal.
     *                  Must be admissible (never overestimates). Default: returns 0 (degenerates to Dijkstra).
     * @return A list of [Edge] objects forming the shortest path, or null if unreachable.
     */
    fun aStarPath(
        storage: GraphStorage,
        startNodeId: String,
        endNodeId: String,
        edgeType: String,
        weightProperty: String = "weight",
        heuristic: (nodeId: String, goalId: String) -> Double = { _, _ -> 0.0 }
    ): List<Edge>? {
        val gScore = mutableMapOf<String, Double>().withDefault { Double.POSITIVE_INFINITY }
        val fScore = mutableMapOf<String, Double>().withDefault { Double.POSITIVE_INFINITY }
        val previousEdge = mutableMapOf<String, Edge>()
        val closedSet = mutableSetOf<String>()

        gScore[startNodeId] = 0.0
        fScore[startNodeId] = heuristic(startNodeId, endNodeId)

        val openSet = PriorityQueue<Pair<String, Double>>(compareBy { it.second })
        openSet.add(startNodeId to fScore.getValue(startNodeId))

        while (openSet.isNotEmpty()) {
            val (currId, _) = openSet.poll() ?: break

            if (currId == endNodeId) {
                val path = mutableListOf<Edge>()
                var step = endNodeId
                while (step != startNodeId) {
                    val edge = previousEdge[step] ?: break
                    path.add(edge)
                    step = edge.sourceId
                }
                path.reverse()
                return path
            }

            if (!closedSet.add(currId)) continue

            val edges = storage.getOutboundEdges(currId, edgeType)
            for (edge in edges) {
                if (edge.targetId in closedSet) continue

                val weight = edge.properties[weightProperty]?.toDoubleOrNull() ?: 1.0
                val tentativeG = gScore.getValue(currId) + weight

                if (tentativeG < gScore.getValue(edge.targetId)) {
                    previousEdge[edge.targetId] = edge
                    gScore[edge.targetId] = tentativeG
                    fScore[edge.targetId] = tentativeG + heuristic(edge.targetId, endNodeId)
                    openSet.add(edge.targetId to fScore.getValue(edge.targetId))
                }
            }
        }
        return null
    }

    // ========================================================================
    // VARIABLE-LENGTH PATH TRAVERSAL
    // ========================================================================

    /**
     * Traverses variable-length paths from a starting node, collecting all reachable
     * nodes within the hop range [minHops, maxHops] with cycle detection.
     *
     * ```kotlin
     * // Find all nodes 2-5 hops away via "KNOWS" edges
     * val friends = GraphAlgorithms.variableLengthPath(storage, "alice", "KNOWS", 2, 5)
     * ```
     *
     * @param storage The graph storage engine.
     * @param startNodeId The starting node.
     * @param edgeType The relationship type to traverse.
     * @param minHops Minimum number of hops (inclusive).
     * @param maxHops Maximum number of hops (inclusive).
     * @return A map of nodeId to the shortest hop distance at which it was reached.
     */
    fun variableLengthPath(
        storage: GraphStorage,
        startNodeId: String,
        edgeType: String,
        minHops: Int = 1,
        maxHops: Int = 5
    ): Map<String, Int> {
        require(minHops >= 0 && maxHops >= minHops) { "Invalid hop range: [$minHops, $maxHops]" }

        val result = mutableMapOf<String, Int>()
        val visited = mutableSetOf<String>()
        // BFS with depth tracking: (nodeId, currentDepth)
        val queue = ArrayDeque<Pair<String, Int>>()

        queue.add(startNodeId to 0)
        visited.add(startNodeId)

        while (queue.isNotEmpty()) {
            val (currId, depth) = queue.removeFirst()

            if (depth in minHops..maxHops && currId != startNodeId) {
                result[currId] = depth
            }

            if (depth >= maxHops) continue

            val targetIds = storage.getOutboundTargetIds(currId, edgeType)
            for (targetId in targetIds) {
                if (visited.add(targetId)) {
                    queue.add(targetId to depth + 1)
                }
            }
        }

        return result
    }

    // ========================================================================
    // COMMUNITY DETECTION (LOUVAIN-INSPIRED)
    // ========================================================================

    /**
     * Detects communities within a set of nodes using a Louvain-inspired algorithm.
     *
     * The algorithm iteratively moves nodes between communities to maximize modularity.
     * Since this operates on a directed graph stored in KoreDB, it uses bidirectional
     * edge counts as the connectivity measure.
     *
     * @param storage The graph storage engine.
     * @param nodeIds The nodes to partition into communities.
     * @param edgeType The relationship type defining connections.
     * @param iterations Maximum number of optimization passes.
     * @return A map of nodeId to communityId.
     * Detects communities within a set of nodes using a Louvain-inspired algorithm,
     * supporting multi-relational graphs and weighted edges.
     *
     * The algorithm iteratively moves nodes between communities to maximize connectivity.
     *
     * @param storage The graph storage engine.
     * @param nodeIds The nodes to partition into communities.
     * @param edgeTypes The relationship types to consider (null = all relationship types).
     * @param weightProperty The edge property to use as connection weight (default: "weight", null = 1.0).
     * @param iterations Maximum number of optimization passes.
     * @return A map of nodeId to communityId (0-indexed).
     */
    fun detectCommunities(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeTypes: Collection<String>? = null,
        weightProperty: String? = "weight",
        iterations: Int = 10
    ): Map<String, Int> {
        if (nodeIds.isEmpty()) return emptyMap()

        val nodeSet = nodeIds.toSet()
        val community = mutableMapOf<String, Int>()
        nodeIds.forEachIndexed { i, id -> community[id] = i }

        val edgeTypeSet = edgeTypes?.toSet()

        // Pre-compute weighted adjacency (bidirectional connectivity within nodeSet)
        val adjacency = mutableMapOf<String, MutableMap<String, Double>>()
        for (nodeId in nodeIds) {
            val neighborWeights = adjacency.getOrPut(nodeId) { mutableMapOf() }

            val outEdges = if (edgeTypeSet == null) {
                storage.getAllOutboundEdges(nodeId).filter { it.targetId in nodeSet }
            } else {
                edgeTypeSet.flatMap { storage.getOutboundEdges(nodeId, it) }.filter { it.targetId in nodeSet }
            }

            for (edge in outEdges) {
                val w = if (weightProperty != null) {
                    edge.properties[weightProperty]?.toDoubleOrNull() ?: 1.0
                } else 1.0
                neighborWeights[edge.targetId] = (neighborWeights[edge.targetId] ?: 0.0) + w
                val revWeights = adjacency.getOrPut(edge.targetId) { mutableMapOf() }
                revWeights[nodeId] = (revWeights[nodeId] ?: 0.0) + w
            }
        }

        for (iter in 0 until iterations) {
            var changed = false

            for (nodeId in nodeIds.shuffled()) {
                val neighbors = adjacency[nodeId] ?: continue
                if (neighbors.isEmpty()) continue
                val currentComm = community[nodeId] ?: continue

                // Sum connection weights to each neighboring community
                val commConnections = mutableMapOf<Int, Double>()
                for ((neighbor, weight) in neighbors) {
                    val neighborComm = community[neighbor] ?: continue
                    commConnections[neighborComm] = (commConnections[neighborComm] ?: 0.0) + weight
                }

                val currentCommWeight = commConnections[currentComm] ?: 0.0
                val bestComm = commConnections.maxByOrNull { it.value }
                if (bestComm != null && bestComm.key != currentComm && bestComm.value > currentCommWeight) {
                    community[nodeId] = bestComm.key
                    changed = true
                }
            }

            if (!changed) break
        }

        // Normalize community IDs to 0..N
        val uniqueComms = community.values.distinct().sorted()
        val commMap = uniqueComms.mapIndexed { i, c -> c to i }.toMap()
        return community.mapValues { commMap[it.value] ?: 0 }
    }

    /**
     * Backward-compatible overload for single edge type.
     */
    fun detectCommunities(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeType: String,
        iterations: Int = 10
    ): Map<String, Int> = detectCommunities(
        storage = storage,
        nodeIds = nodeIds,
        edgeTypes = listOf(edgeType),
        weightProperty = null,
        iterations = iterations
    )

    // ========================================================================
    // CONNECTED COMPONENTS
    // ========================================================================

    /**
     * Finds all connected components in the graph (treating edges as undirected).
     *
     * @param storage The graph storage engine.
     * @param nodeIds The set of nodes to analyze.
     * @param edgeType The relationship type defining connections.
     * @return A list of components, each being a set of node IDs.
     */
    fun connectedComponents(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeType: String
    ): List<Set<String>> {
        val nodeSet = nodeIds.toSet()
        val visited = mutableSetOf<String>()
        val components = mutableListOf<Set<String>>()

        for (nodeId in nodeIds) {
            if (nodeId in visited) continue

            // BFS to find all nodes in this component
            val component = mutableSetOf<String>()
            val queue = ArrayDeque<String>()
            queue.add(nodeId)

            while (queue.isNotEmpty()) {
                val curr = queue.removeFirst()
                if (!visited.add(curr)) continue
                component.add(curr)

                val neighbors = (storage.getOutboundTargetIds(curr, edgeType) +
                                 storage.getInboundSourceIds(curr, edgeType))
                    .filter { it in nodeSet && it !in visited }
                queue.addAll(neighbors)
            }

            components.add(component)
        }

        return components
    }

    // ========================================================================
    // DEGREE CENTRALITY
    // ========================================================================

    /**
     * Computes degree centrality for each node (in-degree + out-degree normalized).
     *
     * @param storage The graph storage engine.
     * @param nodeIds The nodes to compute centrality for.
     * @param edgeType The relationship type.
     * @return A map of nodeId to centrality score [0.0, 1.0].
     */
    fun degreeCentrality(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeType: String
    ): Map<String, Double> {
        if (nodeIds.size <= 1) return nodeIds.associateWith { 1.0 }
        val maxDegree = (nodeIds.size - 1).toDouble()

        return nodeIds.associateWith { nodeId ->
            val outDeg = storage.getOutboundTargetIds(nodeId, edgeType).size
            val inDeg = storage.getInboundSourceIds(nodeId, edgeType).size
            (outDeg + inDeg).toDouble() / (2 * maxDegree)
        }
    }
}
