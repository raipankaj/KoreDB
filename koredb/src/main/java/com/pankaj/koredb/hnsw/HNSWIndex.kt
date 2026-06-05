package com.pankaj.koredb.hnsw

import com.pankaj.koredb.core.VectorMath
import java.util.PriorityQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.ln
import kotlin.random.Random

/**
 * A production-grade Hierarchical Navigable Small World (HNSW) index.
 *
 * Features:
 * - **Multiple Distance Metrics**: Cosine, Euclidean, Inner Product, Manhattan
 * - **Metadata Filtering**: Pre-filtered search during HNSW traversal
 * - **Vector Deletion**: Soft-delete with tombstone set + periodic compaction
 * - **Scalar Quantization**: Optional SQ8 for 4x memory reduction
 * - **Batch Construction**: Optimized bulk insert for initial data loading
 * - **RNG Pruning**: Relative Neighborhood Graph heuristic for better recall
 * - **Thread-Safe**: ConcurrentHashMap + AtomicReference for lock-free reads
 *
 * @param maxNeighbors Max connections per node per layer (M parameter).
 * @param efConstruction Size of the dynamic candidate list during index building.
 * @param efSearch Default size of the dynamic candidate list during search.
 * @param metric The distance metric to use for similarity computation.
 * @param quantizer Optional scalar quantizer for memory-efficient storage.
 */
class HNSWIndex(
    private val maxNeighbors: Int = 16,
    private val efConstruction: Int = 200,
    var efSearch: Int = 50,
    val metric: DistanceMetric = DistanceMetric.COSINE,
    private val quantizer: ScalarQuantizer? = null
) {
    private val levelMult = 1.0 / ln(maxNeighbors.toDouble())

    private val nodes = ConcurrentHashMap<String, HNSWNode>()
    private val entryNode = AtomicReference<HNSWNode?>(null)
    private val maxLevel = AtomicInteger(-1)
    
    // Tombstone set for soft-deleted nodes (checked during search)
    private val tombstones = ConcurrentHashMap.newKeySet<String>()
    
    // Metadata storage for filtered search
    private val metadata = ConcurrentHashMap<String, Map<String, Any>>()

    // Dynamic training buffer for quantization calibration
    private val trainingVectors = mutableListOf<FloatArray>()
    private val TRAINING_THRESHOLD = 100

    /**
     * A node in the HNSW graph containing a vector and its neighbor connections.
     */
    class HNSWNode(
        val id: String,
        @Volatile var vector: FloatArray?,
        val magnitude: Float,
        val nodeLevel: Int,
        @Volatile var quantizedVector: ByteArray? = null
    ) {
        val neighbors = Array(nodeLevel + 1) { ConcurrentHashMap.newKeySet<String>() }
    }

    private fun trainQuantizerIfNeeded(newVector: FloatArray) {
        val q = quantizer ?: return
        if (q.isTrained()) return
        
        synchronized(trainingVectors) {
            if (q.isTrained()) return
            trainingVectors.add(newVector)
            if (trainingVectors.size >= TRAINING_THRESHOLD) {
                q.train(trainingVectors)
                // Quantize all existing nodes
                for (node in nodes.values) {
                    val rawVec = node.vector
                    if (rawVec != null) {
                        node.quantizedVector = q.quantize(rawVec)
                        node.vector = null // Free memory!
                    }
                }
                trainingVectors.clear()
            }
        }
    }

    // ========================================================================
    // INSERT
    // ========================================================================

    /**
     * Inserts a vector with optional metadata into the HNSW index.
     *
     * @param id Unique identifier for this vector.
     * @param vector The float vector to index.
     * @param magnitude Pre-computed magnitude (L2 norm) of the vector.
     * @param meta Optional metadata map for hybrid search filtering.
     */
    fun insert(id: String, vector: FloatArray, magnitude: Float, meta: Map<String, Any>? = null) {
        // Remove from tombstones if re-inserting a previously deleted vector
        tombstones.remove(id)
        
        if (meta != null) metadata[id] = meta

        trainQuantizerIfNeeded(vector)

        val level = randomLevel()
        val quantized = if (quantizer != null && quantizer.isTrained()) quantizer.quantize(vector) else null
        val storedVector = if (quantizer != null && quantizer.isTrained()) null else vector
        val newNode = HNSWNode(id, storedVector, magnitude, level, quantized)
        nodes[id] = newNode

        val startNode = entryNode.get()
        if (startNode == null || startNode.id == id) {
            synchronized(entryNode) {
                val currentStart = entryNode.get()
                if (currentStart == null || currentStart.id == id) {
                    entryNode.set(newNode)
                    maxLevel.set(maxOf(level, maxLevel.get()))
                    if (currentStart == null) return
                }
            }
        }

        var currNode: HNSWNode = entryNode.get()!!
        var currDist = distance(vector, magnitude, currNode)

        // Navigate upper layers greedily
        for (l in maxLevel.get() downTo level + 1) {
            var changed = true
            while (changed) {
                changed = false
                // Safety check: only traverse if current node has this level
                if (l <= currNode.nodeLevel) {
                    for (neighborId in currNode.neighbors[l]) {
                        if (neighborId in tombstones) continue
                        val neighborNode = nodes[neighborId] ?: continue
                        val d = distance(vector, magnitude, neighborNode)
                        if (d > currDist) {
                            currDist = d; currNode = neighborNode; changed = true
                        }
                    }
                }
            }
        }

        // Insert at target layers with RNG-heuristic pruning
        for (l in minOf(level, maxLevel.get()) downTo 0) {
            // Ensure entry point for this layer actually exists at this level
            if (l > currNode.nodeLevel) {
                currNode = entryNode.get()!!
                while (l > currNode.nodeLevel && currNode.id != newNode.id) {
                    break 
                }
            }

            val candidates = searchLayer(vector, magnitude, currNode, efConstruction, l, VectorFilter.EMPTY)
            val neighborsToConnect = selectNeighborsHeuristic(vector, magnitude, candidates, maxNeighbors)

            for (candidate in neighborsToConnect) {
                val neighborNode = nodes[candidate.first] ?: continue
                if (l <= newNode.nodeLevel && l <= neighborNode.nodeLevel) {
                    connect(newNode, neighborNode, l)
                    connect(neighborNode, newNode, l)
                    pruneConnections(neighborNode, l)
                }
            }

            if (neighborsToConnect.isNotEmpty()) {
                val nextNode = nodes[neighborsToConnect[0].first]
                if (nextNode != null) {
                    currNode = nextNode; currDist = neighborsToConnect[0].second
                }
            }
        }

        if (level > maxLevel.get()) {
            synchronized(maxLevel) {
                if (level > maxLevel.get()) {
                    entryNode.set(newNode)
                    maxLevel.set(level)
                }
            }
        }
    }

    /**
     * Batch insert for efficient initial data loading.
     * Vectors are inserted in order, with parallel-friendly structure.
     */
    fun insertBatch(
        vectors: List<Triple<String, FloatArray, Map<String, Any>?>>,
        progressCallback: ((Int, Int) -> Unit)? = null
    ) {
        for ((index, triple) in vectors.withIndex()) {
            val (id, vector, meta) = triple
            insert(id, vector, VectorMath.getMagnitude(vector), meta)
            progressCallback?.invoke(index + 1, vectors.size)
        }
    }

    // ========================================================================
    // SEARCH
    // ========================================================================

    /**
     * Searches for the K most similar vectors, optionally filtered by metadata.
     *
     * @param query The query vector.
     * @param limit Number of results to return.
     * @param filter Optional metadata filter (pre-filtering during traversal).
     * @return List of (id, similarity) pairs sorted by descending similarity.
     */
    fun search(
        query: FloatArray,
        limit: Int,
        filter: VectorFilter = VectorFilter.EMPTY
    ): List<Pair<String, Float>> {
        val startNode = entryNode.get() ?: return emptyList()
        val firstNode = nodes.values.firstOrNull()
        val indexDim = firstNode?.vector?.size ?: firstNode?.quantizedVector?.size ?: 0
        if (indexDim > 0 && query.size != indexDim) {
            return emptyList()
        }

        val queryMag = VectorMath.getMagnitude(query)

        var currNode = startNode
        var currDist = distance(query, queryMag, currNode)

        // Navigate upper layers
        val currentMaxLevel = maxLevel.get()
        if (currentMaxLevel >= 1) {
            for (l in currentMaxLevel downTo 1) {
                var changed = true
                while (changed) {
                    changed = false
                    if (l <= currNode.nodeLevel) {
                        for (neighborId in currNode.neighbors[l]) {
                            if (neighborId in tombstones) continue
                            val neighborNode = nodes[neighborId] ?: continue
                            val d = distance(query, queryMag, neighborNode)
                            if (d > currDist) {
                                currDist = d; currNode = neighborNode; changed = true
                            }
                        }
                    }
                }
            }
        }

        // Comprehensive base-layer search with over-fetching for filtered queries
        val searchEf = if (filter.isEmpty()) {
            maxOf(efSearch, limit)
        } else {
            maxOf(efSearch, limit * 4)
        }

        val results = searchLayer(query, queryMag, currNode, searchEf, 0, filter)
        return results.take(limit)
    }

    // ========================================================================
    // DELETE & UPDATE
    // ========================================================================

    /**
     * Soft-deletes a vector by marking it as a tombstone.
     * The node is skipped during search but remains in the graph for connectivity.
     * Call [compact] periodically to physically remove deleted nodes.
     *
     * @param id The ID of the vector to delete.
     * @return true if the vector existed and was deleted.
     */
    fun delete(id: String): Boolean {
        if (!nodes.containsKey(id)) return false
        tombstones.add(id)
        metadata.remove(id)
        return true
    }

    /**
     * Updates a vector by deleting the old one and inserting the new one.
     *
     * @param id The ID of the vector to update.
     * @param newVector The new vector data.
     * @param newMeta Optional new metadata (replaces old metadata entirely).
     */
    fun update(id: String, newVector: FloatArray, newMeta: Map<String, Any>? = null) {
        delete(id)
        insert(id, newVector, VectorMath.getMagnitude(newVector), newMeta)
    }

    /**
     * Updates only the metadata for a vector without changing the vector itself.
     */
    fun updateMetadata(id: String, meta: Map<String, Any>) {
        if (nodes.containsKey(id) && id !in tombstones) {
            metadata[id] = meta
        }
    }

    /**
     * Compacts the index by physically removing tombstoned nodes and
     * repairing broken neighbor links. Call periodically during idle time.
     *
     * @return Number of nodes physically removed.
     */
    fun compact(): Int {
        val removed = tombstones.size
        for (deadId in tombstones) {
            val deadNode = nodes.remove(deadId) ?: continue
            for (l in 0..deadNode.nodeLevel) {
                for (neighborId in deadNode.neighbors[l]) {
                    val neighbor = nodes[neighborId] ?: continue
                    if (l <= neighbor.nodeLevel) {
                        neighbor.neighbors[l].remove(deadId)
                    }
                }
            }
        }
        tombstones.clear()

        // Repair entry point if it was deleted
        if (entryNode.get()?.let { !nodes.containsKey(it.id) } == true) {
            entryNode.set(nodes.values.maxByOrNull { it.nodeLevel })
            maxLevel.set(entryNode.get()?.nodeLevel ?: -1)
        }

        return removed
    }

    // ========================================================================
    // METADATA
    // ========================================================================

    /**
     * Retrieves the metadata for a given vector ID.
     */
    fun getMetadata(id: String): Map<String, Any>? = metadata[id]

    /**
     * Sets metadata for a vector.
     */
    fun setMetadata(id: String, meta: Map<String, Any>) {
        metadata[id] = meta
    }

    // ========================================================================
    // INTERNAL ALGORITHMS
    // ========================================================================

    private fun searchLayer(
        query: FloatArray, queryMag: Float,
        entryPoint: HNSWNode, ef: Int, level: Int,
        filter: VectorFilter
    ): List<Pair<String, Float>> {
        val visited = mutableSetOf<String>()
        val candidates = PriorityQueue<Pair<String, Float>>(compareByDescending { it.second })
        val results = PriorityQueue<Pair<String, Float>>(compareBy { it.second })

        val initialDist = distance(query, queryMag, entryPoint)
        val entryPassesFilter = entryPoint.id !in tombstones && filter.matches(metadata[entryPoint.id])
        
        candidates.add(entryPoint.id to initialDist)
        if (entryPassesFilter) {
            results.add(entryPoint.id to initialDist)
        }
        visited.add(entryPoint.id)

        while (candidates.isNotEmpty()) {
            val (currId, currDist) = candidates.poll()!!
            
            if (results.size >= ef && currDist < results.peek()!!.second) break
            
            val currNode = nodes[currId] ?: continue
            
            if (level > currNode.nodeLevel) continue

            for (neighborId in currNode.neighbors[level]) {
                if (neighborId in visited) continue
                visited.add(neighborId)
                if (neighborId in tombstones) continue

                val neighborNode = nodes[neighborId] ?: continue
                val dist = distance(query, queryMag, neighborNode)

                val worstResult = if (results.size >= ef) results.peek()!!.second else Float.MIN_VALUE

                if (results.size < ef || dist > worstResult) {
                    candidates.add(neighborId to dist)
                    
                    if (filter.matches(metadata[neighborId])) {
                        results.add(neighborId to dist)
                        if (results.size > ef) results.poll()
                    }
                }
            }
        }

        return results.toList().sortedByDescending { it.second }
    }

    /**
     * RNG (Relative Neighborhood Graph) heuristic for neighbor selection.
     * Produces better graph quality than simple closest-N selection.
     */
    private fun selectNeighborsHeuristic(
        query: FloatArray, queryMag: Float,
        candidates: List<Pair<String, Float>>,
        maxCount: Int
    ): List<Pair<String, Float>> {
        if (candidates.size <= maxCount) return candidates

        val selected = mutableListOf<Pair<String, Float>>()
        val remaining = candidates.sortedByDescending { it.second }.toMutableList()

        while (selected.size < maxCount && remaining.isNotEmpty()) {
            val best = remaining.removeFirst()
            selected.add(best)

            val bestNode = nodes[best.first] ?: continue
            remaining.removeAll { candidate ->
                val candidateNode = nodes[candidate.first] ?: return@removeAll true
                val distToSelected = distanceBetweenNodes(bestNode, candidateNode)
                distToSelected > candidate.second
            }
        }

        return selected
    }

    private fun connect(source: HNSWNode, target: HNSWNode, level: Int) {
        if (level <= source.nodeLevel) {
            synchronized(source) {
                source.neighbors[level].add(target.id)
            }
        }
    }

    private fun pruneConnections(node: HNSWNode, level: Int) {
        if (level > node.nodeLevel) return
        val connections = node.neighbors[level]
        if (connections.size <= maxNeighbors) return

        synchronized(node) {
            if (connections.size <= maxNeighbors) return
            val sorted = connections.mapNotNull { id ->
                if (id in tombstones) return@mapNotNull null
                val neighbor = nodes[id] ?: return@mapNotNull null
                id to distanceBetweenNodes(node, neighbor)
            }.sortedByDescending { it.second }

            val toKeep = sorted.take(maxNeighbors).map { it.first }.toSet()
            val toRemove = connections.filter { it !in toKeep }
            for (id in toRemove) {
                connections.remove(id)
            }
        }
    }

    private fun distance(v: FloatArray, mag: Float, node: HNSWNode): Float {
        val qVec = node.quantizedVector
        val rawVec = node.vector
        return if (qVec != null && quantizer != null) {
            quantizer.computeDistance(v, qVec, metric)
        } else if (rawVec != null) {
            metric.computeWithMagnitudes(v, mag, rawVec, node.magnitude)
        } else {
            val decompressed = quantizer?.dequantize(qVec!!) ?: FloatArray(0)
            metric.compute(v, decompressed)
        }
    }

    private fun distanceBetweenNodes(n1: HNSWNode, n2: HNSWNode): Float {
        val v1 = n1.vector
        val v2 = n2.vector
        val q1 = n1.quantizedVector
        val q2 = n2.quantizedVector
        val quant = quantizer

        return when {
            v1 != null && v2 != null -> metric.computeWithMagnitudes(v1, n1.magnitude, v2, n2.magnitude)
            v1 != null && q2 != null && quant != null -> quant.computeDistance(v1, q2, metric)
            v2 != null && q1 != null && quant != null -> quant.computeDistance(v2, q1, metric)
            q1 != null && q2 != null && quant != null -> {
                val decompressed = quant.dequantize(q1)
                quant.computeDistance(decompressed, q2, metric)
            }
            else -> {
                val raw1 = v1 ?: quant?.dequantize(q1!!) ?: FloatArray(0)
                val raw2 = v2 ?: quant?.dequantize(q2!!) ?: FloatArray(0)
                metric.compute(raw1, raw2)
            }
        }
    }

    private fun randomLevel(): Int {
        return (-ln(Random.nextDouble()) * levelMult).toInt()
    }

    // ========================================================================
    // STATS & UTILITIES
    // ========================================================================

    fun size() = nodes.size - tombstones.size
    fun totalNodes() = nodes.size
    fun deletedCount() = tombstones.size
    fun contains(id: String): Boolean = nodes.containsKey(id) && id !in tombstones

    /**
     * Returns index health statistics.
     */
    fun stats(): IndexStats {
        var totalEdges = 0L
        var maxEdges = 0
        for (node in nodes.values) {
            if (node.id in tombstones) continue
            val edges = node.neighbors.sumOf { it.size }
            totalEdges += edges
            if (edges > maxEdges) maxEdges = edges
        }
        val activeNodes = size()
        return IndexStats(
            totalNodes = activeNodes,
            deletedNodes = tombstones.size,
            totalEdges = totalEdges,
            avgEdgesPerNode = if (activeNodes > 0) totalEdges.toFloat() / activeNodes else 0f,
            maxEdgesOnNode = maxEdges,
            levels = maxLevel.get() + 1,
            metric = metric,
            quantized = quantizer != null
        )
    }

    data class IndexStats(
        val totalNodes: Int,
        val deletedNodes: Int,
        val totalEdges: Long,
        val avgEdgesPerNode: Float,
        val maxEdgesOnNode: Int,
        val levels: Int,
        val metric: DistanceMetric,
        val quantized: Boolean
    )

    // ========================================================================
    // PERSISTENCE
    // ========================================================================

    private class CountingOutputStream(private val out: java.io.OutputStream) : java.io.OutputStream() {
        var bytesWritten: Long = 0L
            private set

        override fun write(b: Int) {
            out.write(b)
            bytesWritten++
        }

        override fun write(b: ByteArray) {
            out.write(b)
            bytesWritten += b.size
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            out.write(b, off, len)
            bytesWritten += len
        }

        override fun flush() {
            out.flush()
        }

        override fun close() {
            out.close()
        }
    }

    fun saveToDisk(file: java.io.File) {
        val activeNodes = nodes.entries.filter { it.key !in tombstones }.map { it.value }
        val nodeCount = activeNodes.size
        val nodeIdToIdx = activeNodes.mapIndexed { idx, node -> node.id to idx }.toMap()

        val rawOut = java.io.FileOutputStream(file)
        val bufferedOut = java.io.BufferedOutputStream(rawOut, 256 * 1024)
        val countingOut = CountingOutputStream(bufferedOut)
        val out = java.io.DataOutputStream(countingOut)

        out.use {
            out.writeInt(0x4D484E53) // "MHNS" magic
            out.writeInt(3)          // Format version 3
            out.writeInt(maxLevel.get())
            out.writeUTF(metric.name)

            val entryId = entryNode.get()?.id
            out.writeByte(if (entryId != null) 1 else 0)
            if (entryId != null) out.writeUTF(entryId)

            // Write quantizer calibration if present
            out.writeByte(if (quantizer != null) 1 else 0)
            if (quantizer != null) {
                val (min, max) = quantizer.getCalibration()
                out.writeInt(min.size)
                for (f in min) out.writeFloat(f)
                for (f in max) out.writeFloat(f)
            }

            out.writeInt(nodeCount)

            // 1. Write String ID Index block
            for (node in activeNodes) {
                out.writeUTF(node.id)
            }

            // 2. Write Node Records Block
            val offsets = LongArray(nodeCount)
            for (i in 0 until nodeCount) {
                val node = activeNodes[i]
                out.flush()
                offsets[i] = countingOut.bytesWritten

                out.writeUTF(node.id)
                val vector = node.vector ?: quantizer?.dequantize(node.quantizedVector!!) ?: FloatArray(0)
                out.writeFloat(node.magnitude)
                out.writeInt(node.nodeLevel)
                out.writeInt(vector.size)
                for (f in vector) out.writeFloat(f)

                // Write neighbors
                for (l in 0..node.nodeLevel) {
                    val neighbors = node.neighbors[l].filter { it !in tombstones }
                    out.writeInt(neighbors.size)
                    for (nId in neighbors) {
                        val nIdx = nodeIdToIdx[nId] ?: -1
                        out.writeInt(nIdx)
                    }
                }

                // Write metadata
                val meta = metadata[node.id]
                out.writeByte(if (meta != null) 1 else 0)
                if (meta != null) {
                    out.writeInt(meta.size)
                    for ((key, value) in meta) {
                        out.writeUTF(key)
                        when (value) {
                            is String  -> { out.writeByte(1); out.writeUTF(value) }
                            is Int     -> { out.writeByte(2); out.writeInt(value) }
                            is Long    -> { out.writeByte(3); out.writeLong(value) }
                            is Float   -> { out.writeByte(4); out.writeFloat(value) }
                            is Double  -> { out.writeByte(5); out.writeDouble(value) }
                            is Boolean -> { out.writeByte(6); out.writeByte(if (value) 1 else 0) }
                            else       -> { out.writeByte(1); out.writeUTF(value.toString()) }
                        }
                    }
                }
            }

            // 3. Write Node Offset Table at the end
            out.flush()
            val offsetTableStart = countingOut.bytesWritten
            for (offset in offsets) {
                out.writeLong(offset)
            }

            // Write offsetTableStart pointer and footer magic
            out.writeLong(offsetTableStart)
            out.writeInt(0x4D484E53) // "MHNS" footer magic
        }
    }

    fun loadFromDisk(file: java.io.File) {
        if (!file.exists()) return

        java.io.DataInputStream(java.io.BufferedInputStream(java.io.FileInputStream(file), 256 * 1024)).use { input ->
            val magic = input.readInt()
            if (magic == 0x484E5357) {
                // Handle legacy version 2
                val version = input.readInt()
                maxLevel.set(input.readInt())
                
                if (version >= 2) {
                    val savedMetric = input.readUTF()
                }

                val hasEntry = input.readBoolean()
                val entryId = if (hasEntry) input.readUTF() else null

                if (version >= 2) {
                    val hasQuantizer = input.readBoolean()
                    if (hasQuantizer && quantizer != null) {
                        val dims = input.readInt()
                        val min = FloatArray(dims) { input.readFloat() }
                        val max = FloatArray(dims) { input.readFloat() }
                        quantizer.loadCalibration(min, max)
                    } else if (hasQuantizer) {
                        val dims = input.readInt()
                        repeat(dims * 2) { input.readFloat() }
                    }
                }

                val nodeCount = input.readInt()
                nodes.clear()
                metadata.clear()
                tombstones.clear()

                for (i in 0 until nodeCount) {
                    val id = input.readUTF()
                    val vecSize = input.readInt()
                    val vector = FloatArray(vecSize) { input.readFloat() }
                    val magnitude = input.readFloat()
                    val level = input.readInt()

                    val quantized = quantizer?.quantize(vector)
                    val storedVector = if (quantizer != null && quantizer.isTrained()) null else vector
                    val node = HNSWNode(id, storedVector, magnitude, level, quantized)
                    
                    for (l in 0..level) {
                        val neighborCount = input.readInt()
                        repeat(neighborCount) { node.neighbors[l].add(input.readUTF()) }
                    }

                    val hasMeta = input.readBoolean()
                    if (hasMeta) {
                        val metaSize = input.readInt()
                        val meta = HashMap<String, Any>(metaSize)
                        repeat(metaSize) {
                            val key = input.readUTF()
                            val type = input.readByte().toInt()
                            val value: Any = when (type) {
                                1 -> input.readUTF()
                                2 -> input.readInt()
                                3 -> input.readLong()
                                4 -> input.readFloat()
                                5 -> input.readDouble()
                                6 -> input.readBoolean()
                                else -> input.readUTF()
                            }
                            meta[key] = value
                        }
                        metadata[id] = meta
                    }

                    nodes[id] = node
                }

                if (entryId != null) {
                    entryNode.set(nodes[entryId])
                }
                return
            }

            if (magic != 0x4D484E53) return // Unknown magic

            val version = input.readInt()
            if (version != 3) return
            maxLevel.set(input.readInt())
            
            val savedMetric = input.readUTF()

            val hasEntry = input.readByte() == 1.toByte()
            val entryId = if (hasEntry) input.readUTF() else null

            val hasQuantizer = input.readByte() == 1.toByte()
            if (hasQuantizer && quantizer != null) {
                val dims = input.readInt()
                val min = FloatArray(dims) { input.readFloat() }
                val max = FloatArray(dims) { input.readFloat() }
                quantizer.loadCalibration(min, max)
            } else if (hasQuantizer) {
                val dims = input.readInt()
                repeat(dims * 2) { input.readFloat() }
            }

            val nodeCount = input.readInt()
            nodes.clear()
            metadata.clear()
            tombstones.clear()

            // 1. Read String ID Index
            val idxToNodeId = Array(nodeCount) { "" }
            for (i in 0 until nodeCount) {
                idxToNodeId[i] = input.readUTF()
            }

            // 2. Read Node Records Block
            val tempNeighbors = Array(nodeCount) { ArrayList<IntArray>() }

            for (i in 0 until nodeCount) {
                val id = input.readUTF()
                val magnitude = input.readFloat()
                val level = input.readInt()
                val vecSize = input.readInt()
                val vector = FloatArray(vecSize) { input.readFloat() }

                val quantized = quantizer?.quantize(vector)
                val storedVector = if (quantizer != null && quantizer.isTrained()) null else vector
                val node = HNSWNode(id, storedVector, magnitude, level, quantized)

                for (l in 0..level) {
                    val neighborCount = input.readInt()
                    val neighborIndices = IntArray(neighborCount) { input.readInt() }
                    tempNeighbors[i].add(neighborIndices)
                }

                val hasMeta = input.readByte() == 1.toByte()
                if (hasMeta) {
                    val metaSize = input.readInt()
                    val meta = HashMap<String, Any>(metaSize)
                    repeat(metaSize) {
                        val key = input.readUTF()
                        val type = input.readByte().toInt()
                        val value: Any = when (type) {
                            1 -> input.readUTF()
                            2 -> input.readInt()
                            3 -> input.readLong()
                            4 -> input.readFloat()
                            5 -> input.readDouble()
                            6 -> input.readByte() == 1.toByte()
                            else -> input.readUTF()
                        }
                        meta[key] = value
                    }
                    metadata[id] = meta
                }

                nodes[id] = node
            }

            // Link neighbors
            for (i in 0 until nodeCount) {
                val nodeId = idxToNodeId[i]
                val node = nodes[nodeId] ?: continue
                for (l in 0..node.nodeLevel) {
                    val neighborsList = tempNeighbors[i][l]
                    for (nIdx in neighborsList) {
                        if (nIdx >= 0 && nIdx < nodeCount) {
                            node.neighbors[l].add(idxToNodeId[nIdx])
                        }
                    }
                }
            }

            if (entryId != null) {
                entryNode.set(nodes[entryId])
            }
        }
    }
}

