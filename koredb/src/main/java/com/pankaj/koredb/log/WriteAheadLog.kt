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

package com.pankaj.koredb.log

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.zip.CRC32

/**
 * A high-performance, append-only Write-Ahead Log (WAL) ensuring data durability.
 *
 * The WAL is a critical component of the storage engine, recording every mutation 
 * to disk before it is applied to the in-memory MemTable. This ensures that in the 
 * event of a system crash or power loss, the database can recover its state by 
 * replaying the log.
 *
 * ### WAL Protocol Specification:
 * 1. **Batch Transaction**: All mutations in a single `appendBatch` call are 
 *    wrapped in a transaction block.
 *    - `RECORD_BEGIN` (4 bytes): Marks the start of a batch.
 *    - `RECORD_PUT` (Multiple): Individual mutations.
 *      - Type (4 bytes)
 *      - Key Size (4 bytes)
 *      - Value Size (4 bytes)
 *      - CRC32 Checksum (8 bytes): Computed over Key and Value.
 *      - Key (Variable bytes)
 *      - Value (Variable bytes)
 *    - `RECORD_COMMIT` (4 bytes): Marks the batch as successfully persisted.
 *
 * ### Recovery Mechanism:
 * During initialization, the WAL is scanned sequentially. Only batches that 
 * conclude with a `RECORD_COMMIT` and pass all CRC32 checksum validations are 
 * replayed into the MemTable. Partial or corrupted batches are ignored to 
 * maintain atomicity.
 *
 * @property logFile The file where the log is persisted.
 */
class WriteAheadLog(private val logFile: File) {

    private val channel: FileChannel

    init {
        // Ensure the directory for the log file exists.
        if (logFile.parentFile?.exists() == false) logFile.parentFile?.mkdirs()

        // Open the file in read-write mode and position the channel at the end for appending.
        channel = RandomAccessFile(logFile, "rw").channel
        channel.position(channel.size())
    }

    /**
     * Appends a batch of key-value pairs to the log atomically.
     *
     * This method pre-allocates a single direct [ByteBuffer] to perform a single 
     * sequential write, maximizing I/O throughput.
     *
     * @param batch A list of mutations to persist.
     */
    @Synchronized
    fun appendBatch(batch: List<Pair<ByteArray, ByteArray>>) {
        // Calculate the exact size needed for the buffer to avoid reallocations.
        // Record Begin (4) + Commit (4) = 8 bytes.
        // Each PUT: Tag(4) + KeySize(4) + ValSize(4) + CRC(8) = 20 bytes + data.
        val estimatedSize = batch.sumOf { 20 + it.first.size + it.second.size } + 8

        val buffer = ByteBuffer.allocateDirect(estimatedSize)

        buffer.putInt(RECORD_BEGIN)

        val crc = CRC32()

        for ((key, value) in batch) {
            crc.reset()
            crc.update(key)
            crc.update(value)
            val checksum = crc.value

            buffer.putInt(RECORD_PUT)
            buffer.putInt(key.size)
            buffer.putInt(value.size)
            buffer.putLong(checksum)
            buffer.put(key)
            buffer.put(value)
        }

        buffer.putInt(RECORD_COMMIT)

        // Switch to read mode for writing to the file channel.
        buffer.flip()

        while (buffer.hasRemaining()) {
            channel.write(buffer)
        }
    }

    /**
     * Forces all buffered data to be written to the underlying storage device.
     */
    fun flush() {
        channel.force(true)
    }

    /**
     * Closes the log file channel.
     */
    fun close() {
        channel.close()
    }

    /**
     * Replays the log from the beginning, invoking [consumer] for each committed record.
     *
     * This method provides an internal mechanism for data recovery using the 
     * already opened channel.
     */
    fun replay(consumer: (ByteArray, ByteArray) -> Unit) {
        channel.position(0)
        val tempBatch = mutableListOf<Pair<ByteArray, ByteArray>>()

        try {
            while (channel.position() < channel.size()) {
                val typeBuf = ByteBuffer.allocate(4)
                if (channel.read(typeBuf) < 4) break 
                typeBuf.flip()

                when (typeBuf.int) {
                    RECORD_BEGIN -> tempBatch.clear()

                    RECORD_PUT -> {
                        // Read metadata: KeySize(4) + ValueSize(4) + Checksum(8).
                        val meta = ByteBuffer.allocate(16)
                        if (channel.read(meta) < 16) break
                        meta.flip()

                        val keySize = meta.int
                        val valueSize = meta.int
                        val storedChecksum = meta.long

                        // Corruption Guard: Prevent OOM from malformed or garbage sizes.
                        if (keySize < 0 || valueSize < 0 || keySize > MAX_RECORD_SIZE || valueSize > MAX_RECORD_SIZE) {
                            println("⚠️ WAL Corrupted: Invalid record size. Halting replay.")
                            break
                        }

                        // Bounds Guard: Ensure we don't attempt to read beyond the file boundary.
                        if (channel.position() + keySize + valueSize > channel.size()) {
                            break
                        }

                        val key = ByteArray(keySize)
                        val value = ByteArray(valueSize)

                        // Sequential read for key data.
                        val keyBuffer = ByteBuffer.wrap(key)
                        while (keyBuffer.hasRemaining()) {
                            if (channel.read(keyBuffer) <= 0) break
                        }
                        if (keyBuffer.hasRemaining()) break

                        // Sequential read for value data.
                        val valueBuffer = ByteBuffer.wrap(value)
                        while (valueBuffer.hasRemaining()) {
                            if (channel.read(valueBuffer) <= 0) break
                        }
                        if (valueBuffer.hasRemaining()) break

                        // Verification Guard: Ensure data integrity via CRC32.
                        val crc = CRC32()
                        crc.update(key)
                        crc.update(value)

                        if (crc.value != storedChecksum) {
                            println("⚠️ WAL CRC Checksum Failed! Halting replay.")
                            break
                        }

                        tempBatch.add(key to value)
                    }

                    RECORD_COMMIT -> {
                        // Atomically apply the entire batch now that it is verified as complete.
                        tempBatch.forEach { consumer(it.first, it.second) }
                        tempBatch.clear()
                    }
                    else -> break
                }
            }
        } catch (e: Exception) {
            println("⚠️ WAL recovery encountered an error; safely ignoring trailing bytes.")
        }
    }

    companion object {
        private const val RECORD_BEGIN = 1
        private const val RECORD_PUT = 2
        private const val RECORD_COMMIT = 3

        /**
         * Safety limit for record sizes (50MB) to prevent memory exhaustion 
         * during recovery from a corrupted log.
         */
        private const val MAX_RECORD_SIZE = 50_000_000

        /**
         * Static recovery method that opens an isolated, read-only view of the log file.
         *
         * @param logFile The WAL file to replay.
         * @param consumer Callback for each recovered key-value pair.
         */
        fun replay(logFile: File, consumer: (ByteArray, ByteArray) -> Unit) {
            if (!logFile.exists()) return

            RandomAccessFile(logFile, "r").use { raf ->
                val channel = raf.channel
                val tempBatch = mutableListOf<Pair<ByteArray, ByteArray>>()

                try {
                    while (channel.position() < channel.size()) {
                        val typeBuf = ByteBuffer.allocate(4)
                        if (channel.read(typeBuf) < 4) break
                        typeBuf.flip()

                        when (typeBuf.int) {
                            RECORD_BEGIN -> tempBatch.clear()
                            RECORD_PUT -> {
                                val meta = ByteBuffer.allocate(16)
                                if (channel.read(meta) < 16) break
                                meta.flip()

                                val keySize = meta.int
                                val valueSize = meta.int
                                val storedChecksum = meta.long

                                if (keySize < 0 || valueSize < 0 ||
                                    keySize > MAX_RECORD_SIZE || valueSize > MAX_RECORD_SIZE) break

                                if (channel.position() + keySize + valueSize > channel.size()) break

                                val key = ByteArray(keySize)
                                val value = ByteArray(valueSize)

                                val keyBuffer = ByteBuffer.wrap(key)
                                while (keyBuffer.hasRemaining()) {
                                    if (channel.read(keyBuffer) <= 0) break
                                }
                                if (keyBuffer.hasRemaining()) break

                                val valueBuffer = ByteBuffer.wrap(value)
                                while (valueBuffer.hasRemaining()) {
                                    if (channel.read(valueBuffer) <= 0) break
                                }
                                if (valueBuffer.hasRemaining()) break

                                val crc = CRC32()
                                crc.update(key)
                                crc.update(value)

                                if (crc.value != storedChecksum) {
                                    println("⚠️ WAL CRC mismatch. Halting replay.")
                                    break
                                }

                                tempBatch.add(key to value)
                            }
                            RECORD_COMMIT -> {
                                tempBatch.forEach { consumer(it.first, it.second) }
                                tempBatch.clear()
                            }
                            else -> break
                        }
                    }
                } catch (e: Exception) {
                    println("⚠️ WAL trailing bytes ignored.")
                }
            }
        }
    }
}
