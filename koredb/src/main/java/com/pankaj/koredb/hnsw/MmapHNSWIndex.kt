package com.pankaj.koredb.hnsw

import com.pankaj.koredb.core.VectorMath
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.util.PriorityQueue

/**
 * A read-only, memory-mapped implementation of the HNSW index.
 * Maps the flat binary file into virtual memory and runs search queries off-heap.
 */
class MmapHNSWIndex(file: File) {

    private var buffer: ByteBuffer
    private val channel: FileChannel
    private val fileLength: Long

    val maxLevel: Int
    val metric: DistanceMetric
    val entryNodeId: String?
    val entryNodeIdx: Int
    val nodeCount: Int

    private val nodeIdToIdx = HashMap<String, Int>()
    private val idxToNodeId: Array<String>
    private val nodeOffsets: LongArray
    private val metadata = HashMap<Int, Map<String, Any>>()

    init {
        val raf = RandomAccessFile(file, "r")
        fileLength = raf.length()
        channel = raf.channel
        buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength)
        buffer.order(ByteOrder.BIG_ENDIAN)

        // Read Header
        val magic = buffer.getInt()
        if (magic != 0x4D484E53) { // "MHNS" magic
            throw IllegalArgumentException("Invalid memory-mapped HNSW file format.")
        }
        val version = buffer.getInt()
        if (version != 3) {
            throw IllegalArgumentException("Unsupported HNSW version: $version. Expected version 3.")
        }
        maxLevel = buffer.getInt()
        val metricName = readUTFFromBuffer(buffer)
        metric = DistanceMetric.valueOf(metricName)

        val hasEntry = buffer.get() == 1.toByte()
        entryNodeId = if (hasEntry) readUTFFromBuffer(buffer) else null

        val hasQuantizer = buffer.get() == 1.toByte()
        if (hasQuantizer) {
            val dims = buffer.getInt()
            // Skip calibration arrays
            buffer.position(buffer.position() + dims * 8)
        }

        nodeCount = buffer.getInt()

        // Read String ID Index block
        idxToNodeId = Array(nodeCount) { "" }
        for (i in 0 until nodeCount) {
            val id = readUTFFromBuffer(buffer)
            idxToNodeId[i] = id
            nodeIdToIdx[id] = i
        }

        entryNodeIdx = entryNodeId?.let { nodeIdToIdx[it] } ?: -1

        // Read offset table from the end of the file
        val footerMagic = buffer.getInt((fileLength - 4).toInt())
        if (footerMagic != 0x4D484E53) {
            throw IllegalArgumentException("HNSW file corrupted: invalid footer magic.")
        }
        val offsetTableStart = buffer.getLong((fileLength - 12).toInt())

        nodeOffsets = LongArray(nodeCount)
        buffer.position(offsetTableStart.toInt())
        for (i in 0 until nodeCount) {
            nodeOffsets[i] = buffer.getLong()
        }

        // Read Metadata for all nodes eagerly
        for (i in 0 until nodeCount) {
            val offset = nodeOffsets[i]
            val localBuf = buffer.duplicate()
            localBuf.position(offset.toInt())

            // Skip: ID (readUTF), Magnitude (Float), Level (Int), Vector size (Int)
            val id = readUTFFromBuffer(localBuf)
            localBuf.getFloat() // magnitude
            val level = localBuf.getInt()
            val vecSize = localBuf.getInt()
            localBuf.position(localBuf.position() + vecSize * 4)

            // Skip adjacency lists
            for (l in 0..level) {
                val neighborsCount = localBuf.getInt()
                localBuf.position(localBuf.position() + neighborsCount * 4)
            }

            // Read Metadata
            val hasMeta = localBuf.get() == 1.toByte()
            if (hasMeta) {
                val metaSize = localBuf.getInt()
                val meta = HashMap<String, Any>(metaSize)
                repeat(metaSize) {
                    val key = readUTFFromBuffer(localBuf)
                    val type = localBuf.get().toInt()
                    val value: Any = when (type) {
                        1 -> readUTFFromBuffer(localBuf)
                        2 -> localBuf.getInt()
                        3 -> localBuf.getLong()
                        4 -> localBuf.getFloat()
                        5 -> localBuf.getDouble()
                        6 -> localBuf.get() == 1.toByte()
                        else -> readUTFFromBuffer(localBuf)
                    }
                    meta[key] = value
                }
                metadata[i] = meta
            }
        }
    }

    fun size(): Int = nodeCount

    fun contains(id: String): Boolean = nodeIdToIdx.containsKey(id)

    fun search(
        query: FloatArray,
        limit: Int,
        filter: VectorFilter = VectorFilter.EMPTY
    ): List<Pair<String, Float>> {
        if (nodeCount == 0 || entryNodeIdx == -1) return emptyList()

        val queryMag = VectorMath.getMagnitude(query)
        val localBuffer = buffer.duplicate()
        localBuffer.order(ByteOrder.BIG_ENDIAN)

        var currIdx = entryNodeIdx
        var currDist = distance(query, queryMag, currIdx, localBuffer)

        if (maxLevel >= 1) {
            for (l in maxLevel downTo 1) {
                var changed = true
                while (changed) {
                    changed = false
                    val nodeLevel = getNodeLevel(currIdx, localBuffer)
                    if (l <= nodeLevel) {
                        val neighbors = getNeighbors(currIdx, l, localBuffer)
                        for (neighborIdx in neighbors) {
                            val d = distance(query, queryMag, neighborIdx, localBuffer)
                            if (d > currDist) {
                                currDist = d
                                currIdx = neighborIdx
                                changed = true
                            }
                        }
                    }
                }
            }
        }

        val searchEf = if (filter.isEmpty()) {
            maxOf(50, limit)
        } else {
            maxOf(50, limit * 4)
        }

        return searchLayer(query, queryMag, currIdx, searchEf, 0, filter, localBuffer)
    }

    private fun searchLayer(
        query: FloatArray,
        queryMag: Float,
        entryIdx: Int,
        ef: Int,
        level: Int,
        filter: VectorFilter,
        localBuffer: ByteBuffer
    ): List<Pair<String, Float>> {
        val visited = HashSet<Int>()
        val candidates = PriorityQueue<Pair<Int, Float>>(compareByDescending { it.second })
        val results = PriorityQueue<Pair<Int, Float>>(compareBy { it.second })

        val initialDist = distance(query, queryMag, entryIdx, localBuffer)
        val entryPassesFilter = filter.matches(metadata[entryIdx])

        candidates.add(entryIdx to initialDist)
        if (entryPassesFilter) {
            results.add(entryIdx to initialDist)
        }
        visited.add(entryIdx)

        while (candidates.isNotEmpty()) {
            val (currIdx, currDist) = candidates.poll()!!

            if (results.size >= ef && currDist < results.peek()!!.second) break

            val nodeLevel = getNodeLevel(currIdx, localBuffer)
            if (level > nodeLevel) continue

            val neighbors = getNeighbors(currIdx, level, localBuffer)
            for (neighborIdx in neighbors) {
                if (neighborIdx in visited) continue
                visited.add(neighborIdx)

                val dist = distance(query, queryMag, neighborIdx, localBuffer)
                val worstResult = if (results.size >= ef) results.peek()!!.second else Float.MIN_VALUE

                if (results.size < ef || dist > worstResult) {
                    candidates.add(neighborIdx to dist)

                    if (filter.matches(metadata[neighborIdx])) {
                        results.add(neighborIdx to dist)
                        if (results.size > ef) results.poll()
                    }
                }
            }
        }

        return results.toList()
            .sortedByDescending { it.second }
            .map { idxToNodeId[it.first] to it.second }
    }

    private fun distance(query: FloatArray, queryMag: Float, nodeIdx: Int, localBuffer: ByteBuffer): Float {
        val offset = nodeOffsets[nodeIdx]
        localBuffer.position(offset.toInt())

        val len = localBuffer.getShort().toInt() and 0xFFFF
        localBuffer.position(localBuffer.position() + len)

        val magnitude = localBuffer.getFloat()
        localBuffer.getInt() // level
        val vecSize = localBuffer.getInt()

        val vectorStartOffset = localBuffer.position()

        return when (metric) {
            DistanceMetric.COSINE -> {
                VectorMath.cosineSimilarity(query, queryMag, localBuffer, vectorStartOffset, vecSize)
            }
            DistanceMetric.EUCLIDEAN -> {
                var sum = 0f
                for (i in 0 until vecSize) {
                    val diff = query[i] - localBuffer.getFloat(vectorStartOffset + i * 4)
                    sum += diff * diff
                }
                1.0f / (1.0f + sum)
            }
            DistanceMetric.INNER_PRODUCT -> {
                VectorMath.dotProduct(query, localBuffer, vectorStartOffset, vecSize)
            }
            DistanceMetric.MANHATTAN -> {
                var sum = 0f
                for (i in 0 until vecSize) {
                    val diff = kotlin.math.abs(query[i] - localBuffer.getFloat(vectorStartOffset + i * 4))
                    sum += diff
                }
                1.0f / (1.0f + sum)
            }
        }
    }

    private fun getNodeLevel(nodeIdx: Int, localBuffer: ByteBuffer): Int {
        val offset = nodeOffsets[nodeIdx]
        localBuffer.position(offset.toInt())
        val len = localBuffer.getShort().toInt() and 0xFFFF
        localBuffer.position(localBuffer.position() + len + 4)
        return localBuffer.getInt()
    }

    private fun getNeighbors(nodeIdx: Int, targetLevel: Int, localBuffer: ByteBuffer): IntArray {
        val offset = nodeOffsets[nodeIdx]
        localBuffer.position(offset.toInt())

        val len = localBuffer.getShort().toInt() and 0xFFFF
        localBuffer.position(localBuffer.position() + len)

        localBuffer.getFloat()
        val nodeLevel = localBuffer.getInt()
        val vecSize = localBuffer.getInt()

        localBuffer.position(localBuffer.position() + vecSize * 4)

        for (l in 0 until targetLevel) {
            val count = localBuffer.getInt()
            localBuffer.position(localBuffer.position() + count * 4)
        }

        val count = localBuffer.getInt()
        val result = IntArray(count)
        for (i in 0 until count) {
            result[i] = localBuffer.getInt()
        }
        return result
    }

    fun close() {
        try {
            channel.close()
        } catch (_: Exception) {}
    }

    companion object {
        private fun readUTFFromBuffer(buf: ByteBuffer): String {
            val utflen = buf.getShort().toInt() and 0xFFFF
            val bytearr = ByteArray(utflen)
            buf.get(bytearr)
            return String(bytearr, Charsets.UTF_8)
        }
    }
}
