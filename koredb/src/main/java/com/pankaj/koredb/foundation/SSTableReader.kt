/*
 * Copyright 2024 KoreDB Authors
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
 * A high-performance reader for Static Sorted Tables (SSTables).
 *
 * This reader uses memory-mapped files ([MappedByteBuffer]) for fast, zero-copy access
 * to on-disk data. It utilizes a Bloom Filter for O(1) existence checks and a sparse
 * index for faster range queries.
 *
 * @property file The SSTable file to read.
 */
class SSTableReader(val file: File) {

    private val buffer: MappedByteBuffer
    private val bloomFilter: BloomFilter
    
    /**
     * The byte offset where the data section ends and the Bloom Filter section begins.
     */
    val dataEndOffset: Long
    private val sparseIndex = mutableMapOf<String, Int>()

    init {
        val channel = RandomAccessFile(file, "r").channel
        buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, file.length())

        if (buffer.capacity() < 12) {
            throw IllegalStateException("Corrupt SSTable: File is too small.")
        }

        // 1. Read Footer (Last 12 bytes)
        buffer.position(buffer.capacity() - 12)
        val bloomFilterOffset = buffer.long
        val magicNumber = buffer.int

        // 2. Verify Data Integrity
        if (magicNumber != SSTable.MAGIC_NUMBER) {
            throw IllegalStateException("Corrupt SSTable: Invalid Magic Number in ${file.name}")
        }

        // 3. Load Bloom Filter
        buffer.position(bloomFilterOffset.toInt())

        val bitSize = buffer.int
        val hashFunctions = buffer.int

        val bfByteSize = buffer.capacity() - 12 - buffer.position()
        val bfBytes = ByteArray(bfByteSize)
        buffer.get(bfBytes)

        bloomFilter = BloomFilter.fromByteArray(bitSize, hashFunctions, bfBytes)
        dataEndOffset = bloomFilterOffset

        // Build Sparse Index for faster navigation
        buildSparseIndex()
    }

    private fun buildSparseIndex() {
        buffer.position(0)
        var count = 0
        while (buffer.position() < dataEndOffset) {
            val currentPos = buffer.position()
            val keySize = buffer.getInt()
            val valueSize = buffer.getInt()

            // Every 10th key, we save its position in RAM to create a sparse index
            if (count % 10 == 0) {
                val keyBytes = ByteArray(keySize)
                buffer.get(keyBytes)
                sparseIndex[String(keyBytes, Charsets.UTF_8)] = currentPos
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
        if (!bloomFilter.mightContain(targetKey)) {
            return null
        }

        buffer.position(0)

        while (buffer.position() < dataEndOffset) {
            val keySize = buffer.getInt()
            val valueSize = buffer.getInt()

            val keyBytes = ByteArray(keySize)
            buffer.get(keyBytes)

            if (ByteArrayComparator.compare(keyBytes, targetKey) == 0) {
                val valueBytes = ByteArray(valueSize)
                buffer.get(valueBytes)
                return valueBytes
            } else {
                buffer.position(buffer.position() + valueSize)
            }
        }
        return null
    }

    /**
     * Scans the file for all keys that start with the given [prefix].
     *
     * @param prefix The byte array prefix to match.
     * @return A list of key-value pairs that match the prefix.
     */
    fun scanByPrefix(prefix: ByteArray): List<Pair<ByteArray, ByteArray>> {
        val results = mutableListOf<Pair<ByteArray, ByteArray>>()
        buffer.position(0)

        while (buffer.position() < dataEndOffset) {
            val startPos = buffer.position()
            val keySize = buffer.getInt()
            val valueSize = buffer.getInt()

            // Compare bytes directly in the buffer to avoid unnecessary allocations
            var isMatch = keySize >= prefix.size
            if (isMatch) {
                for (i in prefix.indices) {
                    if (buffer.get(startPos + 8 + i) != prefix[i]) {
                        isMatch = false
                        break
                    }
                }
            }

            if (isMatch) {
                val keyBytes = ByteArray(keySize)
                buffer.get(keyBytes)
                val valueBytes = ByteArray(valueSize)
                buffer.get(valueBytes)
                results.add(Pair(keyBytes, valueBytes))
            } else {
                buffer.position(startPos + 8 + keySize + valueSize)
            }
        }
        return results
    }

    /**
     * Returns a thread-safe snapshot of the underlying buffer.
     *
     * @return A [java.nio.ByteBuffer] pointing to the same data but with independent markers.
     */
    fun getBufferSnapshot(): java.nio.ByteBuffer {
        return buffer.duplicate().order(java.nio.ByteOrder.BIG_ENDIAN)
    }

    /**
     * Searches the SSTable for the most similar vectors based on cosine similarity.
     *
     * This method uses a bounded min-heap to efficiently find the top results without
     * allocating memory for all candidate keys.
     *
     * @param prefix Prefix to filter relevant records.
     * @param query The query vector.
     * @param limit The maximum number of results to return.
     * @return A sorted list of the most similar keys and their scores.
     */
    fun findTopVectors(prefix: ByteArray, query: FloatArray, limit: Int): List<Pair<ByteArray, Float>> {
        val topKHeap = PriorityQueue<Pair<Int, Float>>(compareBy { it.second })
        val queryMag = VectorMath.getMagnitude(query)
        buffer.position(0)

        while (buffer.position() < dataEndOffset) {
            val startPos = buffer.position()
            val keySize = buffer.getInt()
            val valueSize = buffer.getInt()

            var isMatch = keySize >= prefix.size
            if (isMatch) {
                for (i in prefix.indices) {
                    if (buffer.get(startPos + 8 + i) != prefix[i]) {
                        isMatch = false
                        break
                    }
                }
            }

            if (isMatch) {
                val valueOffset = startPos + 8 + keySize
                val score = VectorMath.cosineSimilarity(query, queryMag, buffer, valueOffset)

                if (topKHeap.size < limit) {
                    topKHeap.add(Pair(startPos, score))
                } else if (score > topKHeap.peek()!!.second) {
                    topKHeap.poll()
                    topKHeap.add(Pair(startPos, score))
                }
            }

            buffer.position(startPos + 8 + keySize + valueSize)
        }

        val finalResults = mutableListOf<Pair<ByteArray, Float>>()
        while (topKHeap.isNotEmpty()) {
            val winner = topKHeap.poll()!!
            buffer.position(winner.first)
            val kSize = buffer.getInt()
            buffer.getInt() // skip valueSize

            val keyBytes = ByteArray(kSize)
            buffer.get(keyBytes)
            finalResults.add(Pair(keyBytes, winner.second))
        }

        return finalResults.reversed()
    }
}
