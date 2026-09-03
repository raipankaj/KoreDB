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
 *      - Key (Variable bytes)
 *      - Value (Variable bytes)
 *    - `BATCH_CRC` (4 bytes): Marker for the checksum.
 *    - Checksum (8 bytes): Computed over all keys and values in the batch.
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
     * Pooled DirectByteBuffer to avoid massive allocations during batch writes.
     * Pre-sized to 4MB to handle typical bulk operations without re-allocation.
     */
    private var sharedBuffer: ByteBuffer? = null

    /**
     * Appends a batch of key-value pairs to the log atomically.
     *
     * This method utilizes a reusable DirectByteBuffer to maximize throughput.
     * The buffer is grown geometrically if needed and never shrunk, ensuring
     * that repeated batch operations converge to a stable allocation.
     *
     * @param batch A list of mutations to persist.
     */
    @Synchronized
    fun appendBatch(batch: List<Pair<ByteArray, ByteArray>>) {
        val bufferCapacity = 4 * 1024 * 1024 // Fixed 4MB streaming buffer
        var buffer = sharedBuffer
        if (buffer == null) {
            buffer = ByteBuffer.allocateDirect(bufferCapacity).order(java.nio.ByteOrder.LITTLE_ENDIAN)
            sharedBuffer = buffer
        }
        buffer.clear()

        val crc = CRC32()

        fun flushBuffer() {
            buffer.flip()
            while (buffer.hasRemaining()) {
                channel.write(buffer)
            }
            buffer.clear()
        }

        fun ensureSpace(needed: Int) {
            if (buffer.remaining() < needed) {
                flushBuffer()
            }
        }

        ensureSpace(4)
        buffer.putInt(RECORD_BEGIN)

        for (i in batch.indices) {
            val pair = batch[i]
            val key = pair.first
            val value = pair.second

            crc.update(key)
            crc.update(value)

            val recordHeaderSize = 12 // RECORD_PUT (4) + key.size (4) + value.size (4)
            val recordTotalSize = recordHeaderSize + key.size + value.size

            if (recordTotalSize <= buffer.capacity()) {
                ensureSpace(recordTotalSize)
                buffer.putInt(RECORD_PUT)
                buffer.putInt(key.size)
                buffer.putInt(value.size)
                buffer.put(key)
                buffer.put(value)
            } else {
                // Large record exceeding 4MB: write header via buffer, stream payloads directly
                ensureSpace(recordHeaderSize)
                buffer.putInt(RECORD_PUT)
                buffer.putInt(key.size)
                buffer.putInt(value.size)
                flushBuffer()

                val keyBuf = ByteBuffer.wrap(key)
                while (keyBuf.hasRemaining()) {
                    channel.write(keyBuf)
                }
                val valBuf = ByteBuffer.wrap(value)
                while (valBuf.hasRemaining()) {
                    channel.write(valBuf)
                }
            }
        }

        ensureSpace(16) // BATCH_CRC (4) + crc.value (8) + RECORD_COMMIT (4)
        buffer.putInt(BATCH_CRC)
        buffer.putLong(crc.value)
        buffer.putInt(RECORD_COMMIT)
        flushBuffer()
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


    companion object {
        private const val RECORD_BEGIN = 1
        private const val RECORD_PUT = 2
        private const val RECORD_COMMIT = 3
        private const val BATCH_CRC = 4

        /**
         * Safety limit for record sizes (50MB) to prevent memory exhaustion 
         * during recovery from a corrupted log.
         */
        private const val MAX_RECORD_SIZE = 50_000_000

        /**
         * Static recovery method that opens an isolated, read-write view of the log file.
         * Replays valid commits and truncates any corrupted or incomplete trailing transactions.
         *
         * @param logFile The WAL file to replay.
         * @param consumer Callback for each recovered key-value pair.
         */
        fun replay(logFile: File, consumer: (ByteArray, ByteArray) -> Unit) {
            if (!logFile.exists()) return

            var lastValidPosition = 0L
            RandomAccessFile(logFile, "rw").use { raf ->
                val channel = raf.channel
                val tempBatch = mutableListOf<Pair<ByteArray, ByteArray>>()
                val batchCrc = CRC32()

                try {
                    val headerBuf = ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN)

                    while (channel.position() < channel.size()) {
                        headerBuf.clear().limit(4)
                        if (channel.read(headerBuf) < 4) break
                        headerBuf.flip()

                        when (headerBuf.int) {
                            RECORD_BEGIN -> {
                                tempBatch.clear()
                                batchCrc.reset()
                            }
                            RECORD_PUT -> {
                                headerBuf.clear().limit(8)
                                if (channel.read(headerBuf) < 8) break
                                headerBuf.flip()

                                val keySize = headerBuf.int
                                val valueSize = headerBuf.int

                                if (keySize < 0 || valueSize < 0 ||
                                    keySize > MAX_RECORD_SIZE || valueSize > MAX_RECORD_SIZE) break

                                if (channel.position() + keySize + valueSize > channel.size()) break

                                val key = ByteArray(keySize)
                                val value = ByteArray(valueSize)

                                val keyBuffer = ByteBuffer.wrap(key)
                                while (keyBuffer.hasRemaining()) { if (channel.read(keyBuffer) <= 0) break }
                                if (keyBuffer.hasRemaining()) break

                                val valueBuffer = ByteBuffer.wrap(value)
                                while (valueBuffer.hasRemaining()) { if (channel.read(valueBuffer) <= 0) break }
                                if (valueBuffer.hasRemaining()) break

                                batchCrc.update(key)
                                batchCrc.update(value)
                                tempBatch.add(key to value)
                            }
                            BATCH_CRC -> {
                                headerBuf.clear().limit(8)
                                if (channel.read(headerBuf) < 8) break
                                headerBuf.flip()
                                if (headerBuf.long != batchCrc.value) break
                            }
                            RECORD_COMMIT -> {
                                tempBatch.forEach { consumer(it.first, it.second) }
                                tempBatch.clear()
                                lastValidPosition = channel.position()
                            }
                            else -> break
                        }
                    }
                } catch (e: Exception) {
                    println("⚠️ WAL trailing bytes ignored.")
                }

                if (lastValidPosition < channel.size()) {
                    try {
                        channel.truncate(lastValidPosition)
                        channel.force(true)
                    } catch (e: Exception) {
                        println("⚠️ Failed to truncate WAL file: ${e.message}")
                    }
                }
            }
        }
    }
}
