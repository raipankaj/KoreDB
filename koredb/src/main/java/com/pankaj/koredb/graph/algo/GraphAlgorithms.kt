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
     */
    fun detectCommunities(
        storage: GraphStorage,
        nodeIds: List<String>,
        edgeType: String,
        iterations: Int = 10
    ): Map<String, Int> {
        if (nodeIds.isEmpty()) return emptyMap()

        val nodeSet = nodeIds.toSet()
        // Initialize: each node in its own community
        val community = mutableMapOf<String, Int>()
        nodeIds.forEachIndexed { i, id -> community[id] = i }

        // Pre-compute adjacency (only within the node set)
        val adjacency = mutableMapOf<String, List<String>>()
        for (nodeId in nodeIds) {
            val neighbors = storage.getOutboundTargetIds(nodeId, edgeType).filter { it in nodeSet } +
                            storage.getInboundSourceIds(nodeId, edgeType).filter { it in nodeSet }
            adjacency[nodeId] = neighbors.distinct()
        }

        val totalEdges = adjacency.values.sumOf { it.size }.toDouble().coerceAtLeast(1.0)

        for (iter in 0 until iterations) {
            var changed = false

            for (nodeId in nodeIds.shuffled()) {
                val neighbors = adjacency[nodeId] ?: continue
                val currentComm = community[nodeId]!!

                // Count connections to each neighboring community
                val commConnections = mutableMapOf<Int, Int>()
                for (neighbor in neighbors) {
                    val neighborComm = community[neighbor] ?: continue
                    commConnections[neighborComm] = (commConnections[neighborComm] ?: 0) + 1
                }

                // Find the community with the most connections
                val bestComm = commConnections.maxByOrNull { it.value }
                if (bestComm != null && bestComm.key != currentComm && bestComm.value > (commConnections[currentComm] ?: 0)) {
                    community[nodeId] = bestComm.key
                    changed = true
                }
            }

            if (!changed) break
        }

        // Normalize community IDs to 0..N
        val uniqueComms = community.values.distinct().sorted()
        val commMap = uniqueComms.mapIndexed { i, c -> c to i }.toMap()
        return community.mapValues { commMap[it.value]!! }
    }

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
