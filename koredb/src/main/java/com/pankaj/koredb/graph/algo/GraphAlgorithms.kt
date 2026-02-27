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
}
