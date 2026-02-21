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

import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * Handles the creation and formatting of Static Sorted Tables (SSTables).
 *
 * An SSTable is an immutable disk-resident file containing sorted key-value pairs.
 * The format includes:
 * 1. Data Blocks: Sequential key-value pairs.
 * 2. Bloom Filter: Probabilistic index for fast membership testing.
 * 3. Footer: Metadata containing the Bloom Filter offset and a magic number for verification.
 */
class SSTable {

    companion object {
        /**
         * Magic number used to identify valid KoreDB SSTable files ("KORE").
         */
        const val MAGIC_NUMBER = 0x4B4F5245

        /**
         * Persists the contents of a [MemTable] to a file in SSTable format.
         *
         * @param memTable The source in-memory table to flush.
         * @param outputFile The destination file for the SSTable.
         */
        fun writeFromMemTable(memTable: MemTable, outputFile: File) {
            val fileOutputStream = FileOutputStream(outputFile)
            val channel: FileChannel = fileOutputStream.channel

            // Prepare a Bloom Filter to build as we write
            val bloomFilter = BloomFilter(100_000, 3)

            // 1. Write the Data Blocks
            for (entry in memTable.getSortedEntries()) {
                val key = entry.key
                val value = entry.value

                bloomFilter.add(key)

                val recordSize = 8 + key.size + value.size
                val buffer = ByteBuffer.allocate(recordSize)

                buffer.putInt(key.size)
                buffer.putInt(value.size)
                buffer.put(key)
                buffer.put(value)

                buffer.flip()
                while (buffer.hasRemaining()) {
                    channel.write(buffer)
                }
            }

            // 2. Record the position where the Bloom Filter begins
            val bloomFilterOffset = channel.position()

            // 3. Write the serialized Bloom Filter
            val bfBytes = bloomFilter.toByteArray()
            val bfBuffer = ByteBuffer.allocate(bfBytes.size + 8)
            bfBuffer.putInt(bloomFilter.bitSize)
            bfBuffer.putInt(bloomFilter.hashFunctions)
            bfBuffer.put(bfBytes)

            bfBuffer.flip()
            while (bfBuffer.hasRemaining()) {
                channel.write(bfBuffer)
            }

            // 4. Write the Footer (12 Bytes)
            // [Offset (8 Bytes)] [Magic Number (4 Bytes)]
            val footerBuffer = ByteBuffer.allocate(12)
            footerBuffer.putLong(bloomFilterOffset)
            footerBuffer.putInt(MAGIC_NUMBER)

            footerBuffer.flip()
            while (footerBuffer.hasRemaining()) {
                channel.write(footerBuffer)
            }

            channel.force(true)
            channel.close()
            fileOutputStream.close()
        }
    }
}
