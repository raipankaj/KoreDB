package com.pankaj.koredb.hnsw

import com.pankaj.koredb.core.VectorMath
import java.util.PriorityQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.ln
import kotlin.random.Random

/**
 * A Hierarchical Navigable Small World (HNSW) index for fast approximate nearest neighbor search.
 *
 * This implementation provides logarithmic-time search by navigating a multi-layered graph.
 *
 * @param maxNeighbors Max connections per node per layer (M).
 * @param efConstruction Size of the dynamic candidate list during construction.
 * @param efSearch Size of the dynamic candidate list during search.
 */
class HNSWIndex(
    private val maxNeighbors: Int = 16,
    private val efConstruction: Int = 200,
    var efSearch: Int = 50
) {
    // Probability factor for layer distribution. 1 / ln(M) is common.
    private val levelMult = 1.0 / ln(maxNeighbors.toDouble())

    private val nodes = ConcurrentHashMap<String, HNSWNode>()
    private val entryNode = AtomicReference<HNSWNode?>(null)
    private val maxLevel = java.util.concurrent.atomic.AtomicInteger(-1)

    /**
     * Represents a vector node in the HNSW graph across multiple layers.
     */
    class HNSWNode(
        val id: String,
        val vector: FloatArray,
        val magnitude: Float,
        val nodeLevel: Int
    ) {
        // Layer -> List of neighbor IDs
        val neighbors = Array(nodeLevel + 1) { ConcurrentHashMap.newKeySet<String>() }
    }

    /**
     * Inserts a vector into the HNSW index.
     */
    fun insert(id: String, vector: FloatArray, magnitude: Float) {
        val level = (-ln(Random.nextDouble()) * levelMult).toInt()
        val newNode = HNSWNode(id, vector, magnitude, level)
        nodes[id] = newNode

        val startNode = entryNode.get()
        if (startNode == null) {
            if (entryNode.compareAndSet(null, newNode)) {
                maxLevel.set(level)
                return
            }
        }

        var currNode: HNSWNode = entryNode.get()!!
        var currDist = cosineSimilarity(vector, magnitude, currNode)

        // 1. Fast navigation through upper layers
        for (l in maxLevel.get() downTo level + 1) {
            var changed = true
            while (changed) {
                changed = false
                val neighbors = currNode.neighbors[l]
                for (neighborId in neighbors) {
                    val neighborNode = nodes[neighborId] ?: continue
                    val d = cosineSimilarity(vector, magnitude, neighborNode)
                    if (d > currDist) {
                        currDist = d
                        currNode = neighborNode
                        changed = true
                    }
                }
            }
        }

        // 2. Precise insertion and neighbor selection for lower layers
        for (l in minOf(level, maxLevel.get()) downTo 0) {
            val candidates = searchLayer(vector, magnitude, currNode, efConstruction, l)
            
            // Connect to M closest neighbors at this layer
            val neighborsToConnect = candidates.take(maxNeighbors)
            for (candidate in neighborsToConnect) {
                val neighborNode = nodes[candidate.first] ?: continue
                
                // Bi-directional connection
                connect(newNode, neighborNode, l)
                connect(neighborNode, newNode, l)
                
                // Ensure max connections isn't exceeded (heuristic pruning)
                pruneConnections(neighborNode, l)
            }
            // Use the closest candidate for next layer down
            if (neighborsToConnect.isNotEmpty()) {
                val nextNode = nodes[neighborsToConnect[0].first]
                if (nextNode != null) {
                    currNode = nextNode
                    currDist = neighborsToConnect[0].second
                }
            }
        }

        // Update entry point if new node reached a higher level
        if (level > maxLevel.get()) {
            entryNode.set(newNode)
            maxLevel.set(level)
        }
    }

    /**
     * Searches for the K most similar vectors in the HNSW graph.
     */
    fun search(query: FloatArray, limit: Int): List<Pair<String, Float>> {
        val startNode = entryNode.get() ?: return emptyList()
        val queryMag = VectorMath.getMagnitude(query)

        var currNode = startNode
        var currDist = cosineSimilarity(query, queryMag, currNode)

        // 1. Navigate upper layers to find a good entry point for the base layer
        val currentMaxLevel = maxLevel.get()
        if (currentMaxLevel >= 1) {
            for (l in currentMaxLevel downTo 1) {
                var changed = true
                while (changed) {
                    changed = false
                    for (neighborId in currNode.neighbors[l]) {
                        val neighborNode = nodes[neighborId] ?: continue
                        val d = cosineSimilarity(query, queryMag, neighborNode)
                        if (d > currDist) {
                            currDist = d
                            currNode = neighborNode
                            changed = true
                        }
                    }
                }
            }
        }

        // 2. Comprehensive search on the base layer (L0)
        return searchLayer(query, queryMag, currNode, maxOf(efSearch, limit), 0)
            .take(limit)
    }

    private fun searchLayer(
        query: FloatArray,
        queryMag: Float,
        entryPoint: HNSWNode,
        ef: Int,
        level: Int
    ): List<Pair<String, Float>> {
        val visited = mutableSetOf<String>()
        // 🚀 CRITICAL: Candidates must be a MAX-HEAP (explore best nodes first)
        val candidates = PriorityQueue<Pair<String, Float>>(compareByDescending { it.second })
        // Results is a MIN-HEAP of size ef (peek() is the worst of the best)
        val results = PriorityQueue<Pair<String, Float>>(compareBy { it.second })

        val initialDist = cosineSimilarity(query, queryMag, entryPoint)
        candidates.add(entryPoint.id to initialDist)
        results.add(entryPoint.id to initialDist)
        visited.add(entryPoint.id)

        while (candidates.isNotEmpty()) {
            val (currId, _) = candidates.poll()!!
            val currNode = nodes[currId] ?: continue
            
            // Optimization: If the best current candidate is already worse 
            // than the worst in results, we can potentially stop if ef is met.
            
            for (neighborId in currNode.neighbors[level]) {
                if (neighborId in visited) continue
                visited.add(neighborId)
                
                val neighborNode = nodes[neighborId] ?: continue
                val dist = cosineSimilarity(query, queryMag, neighborNode)

                if (results.size < ef || dist > results.peek()!!.second) {
                    candidates.add(neighborId to dist)
                    results.add(neighborId to dist)
                    if (results.size > ef) results.poll()
                }
            }
        }

        return results.toList().sortedByDescending { it.second }
    }

    private fun connect(source: HNSWNode, target: HNSWNode, level: Int) {
        source.neighbors[level].add(target.id)
    }

    private fun pruneConnections(node: HNSWNode, level: Int) {
        val connections = node.neighbors[level]
        if (connections.size > maxNeighbors) {
            // Simple pruning heuristic
            val sorted = connections.mapNotNull { id ->
                val neighbor = nodes[id]
                if (neighbor != null) {
                    id to cosineSimilarity(node.vector, node.magnitude, neighbor)
                } else null
            }.sortedByDescending { it.second }
            
            connections.clear()
            sorted.take(maxNeighbors).forEach { connections.add(it.first) }
        }
    }

    private fun cosineSimilarity(v1: FloatArray, m1: Float, node2: HNSWNode): Float {
        val dot = dotProduct(v1, node2.vector)
        return if (m1 == 0f || node2.magnitude == 0f) 0f else dot / (m1 * node2.magnitude)
    }

    private fun dotProduct(v1: FloatArray, v2: FloatArray): Float {
        var dot = 0f
        val size = v1.size
        var i = 0
        while (i <= size - 4) {
            dot += (v1[i] * v2[i]) + (v1[i+1] * v2[i+1]) + (v1[i+2] * v2[i+2]) + (v1[i+3] * v2[i+3])
            i += 4
        }
        while (i < size) {
            dot += v1[i] * v2[i]
            i++
        }
        return dot
    }

    fun size() = nodes.size
}
