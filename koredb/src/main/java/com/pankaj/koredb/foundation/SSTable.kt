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
         * The [MemTable] must be sorted prior to calling this method to maintain 
         * the invariant that SSTable keys are stored in lexicographical order.
         *
         * @param memTable The source in-memory table to flush.
         * @param outputFile The destination file where the SSTable will be written.
         */
        fun writeFromMemTable(memTable: MemTable, outputFile: File) {
            val fileOutputStream = FileOutputStream(outputFile)
            val channel: FileChannel = fileOutputStream.channel

            // Initialize a Bloom Filter to build a membership index during the write pass.
            // Parameters are tuned for 100k entries with a low false-positive rate.
            val bloomFilter = BloomFilter(100_000, 3)

            // 1. Write the Data Blocks: Iterate through sorted entries and append to file.
            for (entry in memTable.getSortedEntries()) {
                val key = entry.key
                val value = entry.value

                bloomFilter.add(key)

                val recordSize = 8 + key.size + value.size
                val buffer = ByteBuffer.allocate(recordSize).order(java.nio.ByteOrder.LITTLE_ENDIAN)

                buffer.putInt(key.size)
                buffer.putInt(value.size)
                buffer.put(key)
                buffer.put(value)

                buffer.flip()
                while (buffer.hasRemaining()) {
                    channel.write(buffer)
                }
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

            // 4. Write the Footer: Fixed-length metadata for file verification and indexing.
            val footerBuffer = ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            footerBuffer.putLong(bloomFilterOffset)
            footerBuffer.putInt(VERSION_V1)
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
    }
}
