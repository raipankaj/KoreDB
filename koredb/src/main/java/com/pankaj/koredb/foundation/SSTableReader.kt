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
import java.util.TreeMap

/**
 * A high-performance reader for Sorted String Tables (SSTables).
 *
 * This reader utilizes memory-mapped files ([MappedByteBuffer]) for zero-copy access
 * to on-disk data. Performance is optimized using:
 * 1. Bloom Filter: O(1) probabilistic check to skip unnecessary disk reads.
 * 2. Sparse Index: A [TreeMap] in RAM that stores offsets for every 10th key,
 *    allowing the reader to jump directly to the closest data block.
 * 3. Early Termination: Leveraging the sorted nature of SSTables to stop
 *    scanning as soon as the target key or prefix range is passed.
 *
 * @property file The SSTable file to read.
 */
class SSTableReader(val file: File) {

    private val buffer: MappedByteBuffer
    private val bloomFilter: BloomFilter

    /**
     * The byte offset where the data section ends and the metadata begins.
     */
    val dataEndOffset: Long

    // Sparse index for fast lookups and range jumps.
    // Maps a sample key to its byte position in the file.
    private val sparseIndex = TreeMap<ByteArray, Int>(ByteArrayComparator)

    init {
        val channel = RandomAccessFile(file, "r").channel
        buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, file.length())

        if (buffer.capacity() < 12) {
            throw IllegalStateException("Corrupt SSTable: File is too small.")
        }

        // Read metadata from the footer (last 12 bytes)
        buffer.position(buffer.capacity() - 12)
        val bloomFilterOffset = buffer.long
        val magicNumber = buffer.int

        if (magicNumber != SSTable.MAGIC_NUMBER) {
            throw IllegalStateException("Corrupt SSTable: Invalid Magic Number in ${file.name}")
        }

        // Load and initialize the Bloom Filter
        buffer.position(bloomFilterOffset.toInt())
        val bitSize = buffer.int
        val hashFunctions = buffer.int
        val bfByteSize = buffer.capacity() - 12 - buffer.position()
        val bfBytes = ByteArray(bfByteSize)
        buffer.get(bfBytes)

        bloomFilter = BloomFilter.fromByteArray(bitSize, hashFunctions, bfBytes)
        dataEndOffset = bloomFilterOffset

        buildSparseIndex()
    }

    /**
     * Builds a sparse index in memory by sampling keys from the file.
     * This allows subsequent searches to jump to a specific offset instead of
     * scanning from the beginning.
     */
    private fun buildSparseIndex() {
        buffer.position(0)
        var count = 0
        while (buffer.position() < dataEndOffset) {
            val currentPos = buffer.position()
            val keySize = buffer.getInt()
            val valueSize = buffer.getInt()

            if (count % 10 == 0) {
                val keyBytes = ByteArray(keySize)
                buffer.get(keyBytes)
                sparseIndex[keyBytes] = currentPos
                buffer.position(currentPos + 8 + keySize + valueSize)
            } else {
                buffer.position(currentPos + 8 + keySize + valueSize)
            }
            count++
        }
    }

    /**
     * Searches for a value associated with the given [targetKey].
     *
     * @param targetKey The key to look for.
     * @return The value associated with the key, or null if not found.
     */
    fun find(targetKey: ByteArray): ByteArray? {
        // Fast path: check Bloom Filter first
        if (!bloomFilter.mightContain(targetKey)) {
            return null
        }

        val localBuffer = getBufferSnapshot()

        // Jump to the closest known offset using the sparse index
        val floorEntry = sparseIndex.floorEntry(targetKey)
        val startOffset = floorEntry?.value ?: 0
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
                // Since keys are sorted, if current key > target, it doesn't exist
                break
            } else {
                localBuffer.position(localBuffer.position() + valueSize)
            }
        }
        return null
    }

    /**
     * Scans the SSTable for all entries matching the given [prefix].
     *
     * @param prefix The byte array prefix to match.
     * @return A list of matching key-value pairs.
     */
    fun scanByPrefix(prefix: ByteArray): List<Pair<ByteArray, ByteArray>> {
        val results = mutableListOf<Pair<ByteArray, ByteArray>>()

        val localBuffer = getBufferSnapshot()
        // Jump to the start of the possible prefix section
        val floorEntry = sparseIndex.floorEntry(prefix)
        val startOffset = floorEntry?.value ?: 0
        localBuffer.position(startOffset)

        while (localBuffer.position() < dataEndOffset) {
            val startPos = localBuffer.position()
            val keySize = localBuffer.getInt()
            val valueSize = localBuffer.getInt()

            var isMatch = true
            var passedPrefix = false
            val compareLen = minOf(keySize, prefix.size)

            // Perform zero-allocation byte comparison directly on the buffer
            for (i in 0 until compareLen) {
                val bFile = localBuffer.get(startPos + 8 + i).toInt() and 0xFF
                val bPref = prefix[i].toInt() and 0xFF
                if (bFile != bPref) {
                    isMatch = false
                    if (bFile > bPref) {
                        passedPrefix = true
                    }
                    break
                }
            }

            if (isMatch && keySize < prefix.size) {
                isMatch = false
            }

            // Early termination: stop if we've alphabetically passed the prefix range
            if (passedPrefix) {
                break
            }

            if (isMatch) {
                val keyBytes = ByteArray(keySize)
                localBuffer.get(keyBytes)
                val valueBytes = ByteArray(valueSize)
                localBuffer.get(valueBytes)
                results.add(Pair(keyBytes, valueBytes))
            } else {
                localBuffer.position(startPos + 8 + keySize + valueSize)
            }
        }
        return results
    }

    /**
     * Returns a thread-safe snapshot of the underlying buffer for iteration.
     */
    fun getBufferSnapshot(): java.nio.ByteBuffer {
        return buffer.duplicate().order(java.nio.ByteOrder.BIG_ENDIAN)
    }

    /**
     * Performs a vector similarity search within a specific prefix range.
     *
     * This method uses a min-heap to track the top [limit] most similar vectors
     * using Cosine Similarity.
     *
     * @param prefix The prefix identifying the collection of vectors.
     * @param query The query vector.
     * @param limit The maximum number of results to return.
     * @return A sorted list of matching keys and their similarity scores.
     */
    fun findTopVectors(prefix: ByteArray, query: FloatArray, limit: Int): List<Pair<ByteArray, Float>> {
        val topKHeap = PriorityQueue<Pair<Int, Float>>(compareBy { it.second })
        val queryMag = VectorMath.getMagnitude(query)

        val localBuffer = getBufferSnapshot()

        // Jump to the start of the vector collection
        val floorEntry = sparseIndex.floorEntry(prefix)
        val startOffset = floorEntry?.value ?: 0
        localBuffer.position(startOffset)

        while (localBuffer.position() < dataEndOffset) {
            val startPos = localBuffer.position()
            val keySize = localBuffer.getInt()
            val valueSize = localBuffer.getInt()

            var isMatch = true
            var passedPrefix = false
            val compareLen = minOf(keySize, prefix.size)

            for (i in 0 until compareLen) {
                val bFile = localBuffer.get(startPos + 8 + i).toInt() and 0xFF
                val bPref = prefix[i].toInt() and 0xFF
                if (bFile != bPref) {
                    isMatch = false
                    if (bFile > bPref) {
                        passedPrefix = true
                    }
                    break
                }
            }

            if (isMatch && keySize < prefix.size) isMatch = false

            if (passedPrefix) {
                break
            }

            if (isMatch) {
                val valueOffset = startPos + 8 + keySize
                val score = VectorMath.cosineSimilarity(query, queryMag, localBuffer, valueOffset)

                if (topKHeap.size < limit) {
                    topKHeap.add(Pair(startPos, score))
                } else if (score > topKHeap.peek()!!.second) {
                    topKHeap.poll()
                    topKHeap.add(Pair(startPos, score))
                }
            }

            localBuffer.position(startPos + 8 + keySize + valueSize)
        }

        val finalResults = mutableListOf<Pair<ByteArray, Float>>()
        while (topKHeap.isNotEmpty()) {
            val winner = topKHeap.poll()!!
            localBuffer.position(winner.first)
            val kSize = localBuffer.getInt()
            localBuffer.getInt() // skip valueSize

            val keyBytes = ByteArray(kSize)
            localBuffer.get(keyBytes)
            finalResults.add(Pair(keyBytes, winner.second))
        }

        return finalResults.reversed()
    }
}
