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

import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * Handles the creation and formatting of Sorted String Tables (SSTables).
 *
 * An SSTable is an immutable disk-resident file containing sorted key-value pairs. 
 * It is a fundamental component of the Log-Structured Merge-tree (LSM-tree), 
 * providing high-throughput sequential writes and efficient range scans.
 *
 * ### File Format Specification (V1):
 *
 * 1. **Data Blocks**: 
 *    Sequential records of key-value pairs. Each record follows the format:
 *    - Key Size (4 bytes, Int)
 *    - Value Size (4 bytes, Int)
 *    - Key (Variable length bytes)
 *    - Value (Variable length bytes)
 *
 * 2. **Bloom Filter**: 
 *    A probabilistic data structure used to quickly determine if a key *might* exist 
 *    in this segment, avoiding unnecessary disk I/O for negative lookups.
 *
 * 3. **Footer**: 
 *    Fixed-size metadata at the end of the file (16 bytes) containing:
 *    - Bloom Filter Offset (8 bytes, Long): Byte position where the filter begins.
 *    - Version (4 bytes, Int): Format version for forward compatibility.
 *    - Magic Number (4 bytes, Int): Constant identifying the file as a KoreDB SSTable.
 */
class SSTable {

    companion object {
        /**
         * Magic number used to identify valid KoreDB SSTable files ("KORE").
         */
        const val MAGIC_NUMBER = 0x4B4F5245

        /**
         * Current version of the SSTable file format.
         */
        const val VERSION_V1 = 1

        /**
         * Persists the contents of a [MemTable] to a file in SSTable format.
         *
         * This method is heavily optimized for throughput:
         * - A single reusable [DirectByteBuffer] is used for all record writes,
         *   eliminating per-record heap allocations and GC pressure.
         * - Bloom filter prefix entries are capped at [MAX_PREFIX_DEPTH] levels
         *   and use zero-copy hashing via [BloomFilter.addRange].
         *
         * @param memTable The source in-memory table to flush.
         * @param outputFile The destination file where the SSTable will be written.
         */
        fun writeFromMemTable(
            memTable: MemTable,
            outputFile: File,
            compressionCodec: com.pankaj.koredb.compression.CompressionCodec = com.pankaj.koredb.compression.NoOpCompressionCodec
        ) {
            writeSortedEntries(
                memTable.getSortedEntries().map { it.key to it.value },
                outputFile,
                compressionCodec
            )
        }

        /**
         * Persists a sequence of sorted key-value pairs directly to an SSTable file.
         * Used for streaming compaction to eliminate intermediate in-memory MemTable accumulation.
         *
         * @param entries Sorted key-value pairs to write.
         * @param outputFile The destination SSTable file.
         * @param compressionCodec Codec for payload compression.
         */
        fun writeSortedEntries(
            entries: Sequence<Pair<ByteArray, ByteArray>>,
            outputFile: File,
            compressionCodec: com.pankaj.koredb.compression.CompressionCodec = com.pankaj.koredb.compression.NoOpCompressionCodec
        ) {
            val fileOutputStream = FileOutputStream(outputFile)
            val channel: FileChannel = fileOutputStream.channel

            // Initialize a Bloom Filter to build a membership index during the write pass.
            // Parameters are tuned for 100k entries with a low false-positive rate.
            val bloomFilter = BloomFilter(100_000, 3)

            val bufferCapacity = 256 * 1024 // 256KB write buffer
            var buffer = ByteBuffer.allocateDirect(bufferCapacity)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN)

            // 1. Write the Data Blocks: Iterate through sorted entries and append to file.
            for (entry in entries) {
                val key = entry.first
                val rawValue = entry.second
                val value = if (rawValue.isNotEmpty() && !isVectorKey(key)) compressionCodec.compress(rawValue) else rawValue

                bloomFilter.add(key)

                // Capped prefix bloom with zero-copy hashing
                var colonCount = 0
                for (i in key.indices) {
                    if (key[i] == ':'.code.toByte()) {
                        colonCount++
                        bloomFilter.addRange(key, 0, i + 1)
                        if (colonCount >= MAX_PREFIX_DEPTH) break
                    }
                }

                val recordSize = 8 + key.size + value.size

                // Auto-flush: if the current record doesn't fit, write buffer to disk
                if (buffer.remaining() < recordSize) {
                    buffer.flip()
                    while (buffer.hasRemaining()) {
                        channel.write(buffer)
                    }
                    buffer.clear()

                    // Handle oversized records that exceed the buffer capacity
                    if (recordSize > buffer.capacity()) {
                        buffer = ByteBuffer.allocateDirect(recordSize)
                            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    }
                }

                buffer.putInt(key.size)
                buffer.putInt(value.size)
                buffer.put(key)
                buffer.put(value)
            }

            // Flush any remaining data in the buffer
            buffer.flip()
            while (buffer.hasRemaining()) {
                channel.write(buffer)
            }

            // 2. Capture the exact byte offset where the Bloom Filter starts.
            val bloomFilterOffset = channel.position()

            // 3. Serialize and write the Bloom Filter.
            val bfBytes = bloomFilter.toByteArray()
            val bfBuffer = ByteBuffer.allocate(bfBytes.size + 8).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            bfBuffer.putInt(bloomFilter.bitSize)
            bfBuffer.putInt(bloomFilter.hashFunctions)
            bfBuffer.put(bfBytes)

            bfBuffer.flip()
            while (bfBuffer.hasRemaining()) {
                channel.write(bfBuffer)
            }

            // 4. Write the Footer: Fixed-length metadata (16 bytes) with encoded codec identifier
            val footerBuffer = ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            footerBuffer.putLong(bloomFilterOffset)
            val versionAndCodec = ((compressionCodec.type.toInt() and 0xFF) shl 24) or (VERSION_V1 and 0x00FFFFFF)
            footerBuffer.putInt(versionAndCodec)
            footerBuffer.putInt(MAGIC_NUMBER)

            footerBuffer.flip()
            while (footerBuffer.hasRemaining()) {
                channel.write(footerBuffer)
            }

            // Ensure all data is physically flushed to the storage device.
            channel.force(true)
            channel.close()
            fileOutputStream.close()
        }

        /**
         * Maximum number of colon-delimited prefix segments to index in the Bloom Filter.
         * Capping this at 3 covers the structural prefixes (e.g., "doc:collection:" or
         * "idx:collection:field:") while avoiding the O(K) explosion from deeply nested keys.
         */
        private const val MAX_PREFIX_DEPTH = 3

        val VEC_PREFIX = "vec:".toByteArray(Charsets.UTF_8)

        fun isVectorKey(key: ByteArray): Boolean {
            if (key.size < VEC_PREFIX.size) return false
            for (i in VEC_PREFIX.indices) {
                if (key[i] != VEC_PREFIX[i]) return false
            }
            return true
        }
    }
}
