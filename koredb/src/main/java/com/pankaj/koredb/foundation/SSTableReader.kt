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
 *
 * ### Performance Optimizations:
 * 1. **Bloom Filter**: Probabilistic membership test to bypass disk access for keys 
 *    definitely not present in the segment.
 * 2. **Sparse Index**: An in-memory sampling of keys (every 512 entries) that allows 
 *    the reader to perform a binary search and jump directly to the relevant data block.
 * 3. **Memory Mapping**: Leverages OS-level page caching for ultra-fast data access 
 *    without the overhead of traditional file I/O.
 * 4. **Early Termination**: Exploits the lexicographical sorting of keys to stop 
 *    scans immediately once the target range is exceeded.
 *
 * @property file The SSTable segment file to read.
 */
class SSTableReader(val file: File) {

    private val buffer: MappedByteBuffer
    private val bloomFilter: BloomFilter

    /**
     * The byte offset identifying where the data section ends and the metadata begins.
     */
    val dataEndOffset: Long

    // Sparse index structures for accelerated block-level lookups.
    private val blockKeys = mutableListOf<ByteArray>()
    private val blockOffsets = mutableListOf<Int>()

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
        val version = buffer.int
        val magicNumber = buffer.int

        if (magicNumber != SSTable.MAGIC_NUMBER) {
            throw IllegalStateException("Corrupt SSTable: Invalid Magic Number in ${file.name}")
        }

        if (version != SSTable.VERSION_V1) {
            throw UnsupportedOperationException("Unsupported SSTable version: $version. Please upgrade KoreDB.")
        }

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

    /**
     * Scans the data section to populate the sparse index.
     * Sampling keys at regular intervals allows the reader to jump to specific 
     * file offsets instead of performing a full linear scan.
     */
    private fun buildSparseIndex() {
        buffer.position(0)
        var count = 0

        while (buffer.position() < dataEndOffset) {
            val currentPos = buffer.position()
            val keySize = buffer.getInt()
            val valueSize = buffer.getInt()

            // Sample every 512th key to balance memory usage and lookup speed.
            if (count % 512 == 0) { 
                val keyBytes = ByteArray(keySize)
                buffer.get(keyBytes)
                blockKeys.add(keyBytes)
                blockOffsets.add(currentPos)
                // Skip the value to continue scanning keys.
                buffer.position(currentPos + 8 + keySize + valueSize)
            } else {
                // Skip both key and value for non-indexed entries.
                buffer.position(currentPos + 8 + keySize + valueSize)
            }

            count++
        }
    }

    /**
     * Performs a binary search over the sparse index to find the starting offset
     * of the block that *might* contain the target key.
     */
    private fun findBlockFloorOffset(target: ByteArray): Int {
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

    /**
     * Similar to [findBlockFloorOffset], but optimized for finding the start
     * of a prefix range.
     */
    fun findBlockStartOffset(prefix: ByteArray): Int {
        var low = 0
        var high = blockKeys.size - 1
        var result = 0

        while (low <= high) {
            val mid = (low + high) ushr 1
            val cmp = ByteArrayComparator.compare(blockKeys[mid], prefix)

            if (cmp < 0) {
                low = mid + 1
            } else {
                result = mid
                high = mid - 1
            }
        }

        return if (blockOffsets.isEmpty()) 0 else blockOffsets[result]
    }

    /**
     * Retrieves the value associated with [targetKey].
     *
     * @return The associated value, or null if the key is not present.
     */
    fun find(targetKey: ByteArray): ByteArray? {
        // Fast path: Check Bloom Filter to avoid unnecessary disk paging.
        if (!bloomFilter.mightContain(targetKey)) {
            return null
        }

        val localBuffer = getBufferSnapshot()

        // Jump to the closest sampled offset.
        val startOffset = findBlockFloorOffset(targetKey)
        localBuffer.position(startOffset)

        while (localBuffer.position() < dataEndOffset) {
            val keySize = localBuffer.getInt()
            val valueSize = localBuffer.getInt()

            val keyBytes = ByteArray(keySize)
            localBuffer.get(keyBytes)

            val cmp = ByteArrayComparator.compare(keyBytes, targetKey)
            if (cmp == 0) {
                val valueBytes = ByteArray(valueSize)
                localBuffer.get(valueBytes)
                return valueBytes
            } else if (cmp > 0) {
                // Early termination: Keys are sorted, so we can stop if current > target.
                break
            } else {
                localBuffer.position(localBuffer.position() + valueSize)
            }
        }
        return null
    }

    /**
     * Scans the SSTable for entries matching the provided [prefix].
     * 
     * This implementation uses a zero-allocation prefix check to minimize 
     * GC pressure during large scans.
     *
     * @param prefix The byte array prefix to match.
     * @param consumer Lambda invoked for each matching key-value pair.
     */
    inline fun scanByPrefix(
        prefix: ByteArray,
        crossinline consumer: (ByteArray, ByteArray) -> Unit
    ) {
        val localBuffer = getBufferSnapshot()

        val startOffset = findBlockStartOffset(prefix)
        localBuffer.position(startOffset)

        while (localBuffer.position() < dataEndOffset) {
            val entryStart = localBuffer.position()
            val keySize = localBuffer.getInt()
            val valueSize = localBuffer.getInt()
            val keyOffset = localBuffer.position()

            // 🚀 ZERO-ALLOCATION PREFIX MATCH
            if (keySize < prefix.size) {
                localBuffer.position(entryStart + 8 + keySize + valueSize)
                continue
            }

            var matches = true
            for (i in prefix.indices) {
                if (localBuffer.get(keyOffset + i) != prefix[i]) {
                    matches = false
                    break
                }
            }

            if (!matches) {
                // Since keys are sorted, if the first byte is already greater than 
                // the prefix's first byte, we've moved past the entire range.
                val firstByte = localBuffer.get(keyOffset)
                if (firstByte > prefix[0]) break

                localBuffer.position(entryStart + 8 + keySize + valueSize)
                continue
            }

            // Allocation only happens for matching entries.
            val keyBytes = ByteArray(keySize)
            localBuffer.position(keyOffset)
            localBuffer.get(keyBytes)

            val valueBytes = ByteArray(valueSize)
            localBuffer.get(valueBytes)

            consumer(keyBytes, valueBytes)
        }
    }

    /**
     * Returns a lightweight snapshot of the memory-mapped buffer.
     * This allows for thread-safe concurrent reads without modifying the main buffer's position.
     */
    fun getBufferSnapshot(): java.nio.ByteBuffer {
        // Essential: Duplicate copies the position (which might be at end after indexing).
        // We must reset it to 0 for new readers.
        return buffer.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN).position(0) as java.nio.ByteBuffer
    }

    /**
     * Performs a vector similarity search within a prefixed collection.
     *
     * Uses a min-heap (PriorityQueue) to track the top [limit] results based 
     * on Cosine Similarity.
     *
     * @param prefix The prefix scoping the search.
     * @param query The query vector.
     * @param limit The maximum number of results to return.
     * @return A list of matching keys and their similarity scores, sorted descending.
     */
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
            
            // 🏎️ OPTIMIZED PREFIX MATCHING & EARLY EXIT
            // Use sequential get() to check prefix (ART is very fast at this)
            var match = keySize >= prefixLen
            if (match) {
                for (i in 0 until prefixLen) {
                    val bFile = localBuffer.get()
                    if (bFile != prefix[i]) {
                        match = false
                        // Check if we passed the entire range (lexicographical sorting)
                        if ((bFile.toInt() and 0xFF) > (prefix[i].toInt() and 0xFF)) {
                             // EXIT ENTIRE DISK SCAN: Range is exceeded.
                             return finalizeResults(topKHeap, localBuffer)
                        }
                        break
                    }
                }
            }
            
            if (match) {
                val valueOffset = startPos + 8 + keySize
                
                // Read magnitude and compute dot product
                val storedMag = localBuffer.getFloat(valueOffset)
                val vectorLength = (valueSize - 4) / 4
                
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

            // Move to the next record
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
            localBuffer.getInt() // Skip valueSize

            val keyBytes = ByteArray(kSize)
            localBuffer.get(keyBytes)
            finalResults.add(Pair(keyBytes, winner.second))
        }
        return finalResults.reversed()
    }
}
