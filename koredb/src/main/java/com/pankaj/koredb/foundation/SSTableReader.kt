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

package com.pankaj.koredb.foundation

import com.pankaj.koredb.core.VectorMath
import java.io.File
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.util.PriorityQueue

/**
 * A high-performance reader for Sorted String Tables (SSTables).
 *
 * This reader utilizes memory-mapped files ([MappedByteBuffer]) to provide zero-copy 
 * access to data persisted on disk. It is designed for low-latency point lookups, 
 * efficient prefix scans, and high-throughput vector similarity searches.
 */
class SSTableReader(val file: File) {
    
    var level: Int = 0

    private val buffer: MappedByteBuffer
    private val bloomFilter: BloomFilter

    /**
     * The byte offset identifying where the data section ends and the metadata begins.
     */
    val dataEndOffset: Long

    var minKey: ByteArray? = null
        private set
    var maxKey: ByteArray? = null
        private set

    // Sparse index structures for accelerated block-level lookups.
    private val blockKeys = mutableListOf<ByteArray>()
    private val blockOffsets = mutableListOf<Int>()
    val compressionCodec: com.pankaj.koredb.compression.CompressionCodec

    init {
        val channel = RandomAccessFile(file, "r").channel
        buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, file.length())
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)

        if (buffer.capacity() < 16) {
            throw IllegalStateException("Corrupt SSTable: File header/footer missing in ${file.name}")
        }

        // Read the metadata footer (last 16 bytes of the file).
        buffer.position(buffer.capacity() - 16)
        val bloomFilterOffset = buffer.long
        val versionAndCodec = buffer.int
        val magicNumber = buffer.int

        if (magicNumber != SSTable.MAGIC_NUMBER) {
            throw IllegalStateException("Corrupt SSTable: Invalid Magic Number in ${file.name}")
        }

        val version = versionAndCodec and 0x00FFFFFF
        if (version != SSTable.VERSION_V1) {
            throw UnsupportedOperationException("Unsupported SSTable version: $version. Please upgrade KoreDB.")
        }

        val codecType = ((versionAndCodec ushr 24) and 0xFF).toByte()
        compressionCodec = com.pankaj.koredb.compression.CompressionCodec.fromType(codecType)

        // Initialize the Bloom Filter from its serialized representation.
        buffer.position(bloomFilterOffset.toInt())
        val bitSize = buffer.int
        val hashFunctions = buffer.int
        val bfByteSize = buffer.capacity() - 16 - buffer.position()
        val bfBytes = ByteArray(bfByteSize)
        buffer.get(bfBytes)

        bloomFilter = BloomFilter.fromByteArray(bitSize, hashFunctions, bfBytes)
        dataEndOffset = bloomFilterOffset

        buildSparseIndex()
    }

    fun decompressValue(bytes: ByteArray): ByteArray {
        return if (bytes.isEmpty()) bytes else compressionCodec.decompress(bytes)
    }

    private fun buildSparseIndex() {
        buffer.position(0)
        var count = 0
        var bytesSinceLastIndex = 0

        while (buffer.position() < dataEndOffset) {
            val currentPos = buffer.position()
            val keySize = buffer.getInt()
            val valueSize = buffer.getInt()
            
            val isFirst = currentPos == 0

            if (isFirst || count % 128 == 0 || bytesSinceLastIndex >= 256 * 1024) { 
                val keyBytes = ByteArray(keySize)
                buffer.get(keyBytes)
                blockKeys.add(keyBytes)
                blockOffsets.add(currentPos)
                
                if (isFirst) minKey = keyBytes
                
                buffer.position(currentPos + 8 + keySize + valueSize)
                bytesSinceLastIndex = 0
            } else {
                buffer.position(currentPos + 8 + keySize + valueSize)
                bytesSinceLastIndex += 8 + keySize + valueSize
            }
            
            // To find the exact maxKey, we just read the key of the last record
            if (buffer.position() == dataEndOffset.toInt()) {
                buffer.position(currentPos + 8)
                val keyBytes = ByteArray(keySize)
                buffer.get(keyBytes)
                maxKey = keyBytes
                buffer.position(dataEndOffset.toInt())
            }
            count++
        }
    }

    fun findBlockStartOffset(target: ByteArray): Int {
        if (blockKeys.isEmpty()) return 0
        var low = 0
        var high = blockKeys.size - 1
        var result = 0
        while (low <= high) {
            val mid = (low + high) ushr 1
            val cmp = ByteArrayComparator.compare(blockKeys[mid], target)
            if (cmp <= 0) {
                result = mid
                low = mid + 1
            } else {
                high = mid - 1
            }
        }
        return blockOffsets[result]
    }

    fun find(targetKey: ByteArray): ByteArray? {
        if (!bloomFilter.mightContain(targetKey)) return null
        val localBuffer = getBufferSnapshot()
        val startOffset = findBlockStartOffset(targetKey)
        localBuffer.position(startOffset)

        while (localBuffer.position() < dataEndOffset) {
            val recordPos = localBuffer.position()
            val keySize = localBuffer.getInt()
            val valueSize = localBuffer.getInt()
            val keyOffset = recordPos + 8

            val cmp = compareBufferWithKey(localBuffer, keyOffset, keySize, targetKey)
            if (cmp == 0) {
                localBuffer.position(keyOffset + keySize)
                val valueBytes = ByteArray(valueSize)
                localBuffer.get(valueBytes)
                return decompressValue(valueBytes)
            } else if (cmp > 0) {
                break
            } else {
                localBuffer.position(keyOffset + keySize + valueSize)
            }
        }
        return null
    }

    /**
     * Compares two sections of different buffers without allocations.
     */
    fun compareBuffers(b1: java.nio.ByteBuffer, off1: Int, len1: Int, b2: java.nio.ByteBuffer, off2: Int, len2: Int): Int {
        val minLen = minOf(len1, len2)
        for (i in 0 until minLen) {
            val v1 = b1.get(off1 + i).toInt() and 0xFF
            val v2 = b2.get(off2 + i).toInt() and 0xFF
            if (v1 != v2) return v1 - v2
        }
        return len1 - len2
    }

    /**
     * Compares a section of the buffer with a given key without allocations.
     */
    fun compareBufferWithKey(buffer: java.nio.ByteBuffer, offset: Int, size: Int, key: ByteArray): Int {
        val minLen = minOf(size, key.size)
        for (i in 0 until minLen) {
            val a = buffer.get(offset + i).toInt() and 0xFF
            val b = key[i].toInt() and 0xFF
            if (a != b) return a.compareTo(b)
        }
        return size.compareTo(key.size)
    }

    fun getBufferSnapshot(): java.nio.ByteBuffer {
        return buffer.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN).position(0) as java.nio.ByteBuffer
    }

    fun mightContain(key: ByteArray): Boolean {
        return bloomFilter.mightContain(key)
    }

    fun findTopVectors(prefix: ByteArray, query: FloatArray, limit: Int): List<Pair<ByteArray, Float>> {
        val topKHeap = PriorityQueue<Pair<Int, Float>>(compareBy { it.second })
        val queryMag = VectorMath.getMagnitude(query)
        val prefixLen = prefix.size

        val localBuffer = getBufferSnapshot()
        val startOffset = findBlockStartOffset(prefix)
        localBuffer.position(startOffset)

        while (localBuffer.position() < dataEndOffset) {
            val startPos = localBuffer.position()
            val keySize = localBuffer.getInt()
            val valueSize = localBuffer.getInt()
            
            var match = keySize >= prefixLen
            if (match) {
                for (i in 0 until prefixLen) {
                    val bFile = localBuffer.get()
                    if (bFile != prefix[i]) {
                        match = false
                        if ((bFile.toInt() and 0xFF) > (prefix[i].toInt() and 0xFF)) {
                             return finalizeResults(topKHeap, localBuffer)
                        }
                        break
                    }
                }
            }
            if (match) {
                val valueOffset = startPos + 8 + keySize
                val storedMag = localBuffer.getFloat(valueOffset)
                val vectorLength = (valueSize - 4) / 4
                if (query.size == vectorLength) {
                    val dot = VectorMath.dotProduct(query, localBuffer, valueOffset + 4, vectorLength)
                    val score = if (queryMag == 0f || storedMag == 0f) 0f else dot / (queryMag * storedMag)

                    if (score > -1.5f) {
                        if (topKHeap.size < limit) {
                            topKHeap.add(Pair(startPos, score))
                        } else if (score > topKHeap.peek()!!.second) {
                            topKHeap.poll()
                            topKHeap.add(Pair(startPos, score))
                        }
                    }
                }
            }
            localBuffer.position(startPos + 8 + keySize + valueSize)
        }
        return finalizeResults(topKHeap, localBuffer)
    }

    private fun finalizeResults(
        topKHeap: PriorityQueue<Pair<Int, Float>>, 
        localBuffer: java.nio.ByteBuffer
    ): List<Pair<ByteArray, Float>> {
        val finalResults = mutableListOf<Pair<ByteArray, Float>>()
        while (topKHeap.isNotEmpty()) {
            val winner = topKHeap.poll()!!
            localBuffer.position(winner.first)
            val kSize = localBuffer.getInt()
            localBuffer.getInt()
            val keyBytes = ByteArray(kSize)
            localBuffer.get(keyBytes)
            finalResults.add(Pair(keyBytes, winner.second))
        }
        return finalResults.reversed()
    }
}




