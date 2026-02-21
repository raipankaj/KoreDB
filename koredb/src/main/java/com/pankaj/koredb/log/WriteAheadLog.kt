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

package com.pankaj.koredb.log

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * A Write-Ahead Log (WAL) that provides durability for database transactions.
 *
 * The WAL appends every write operation sequentially to a log file before it is
 * applied to the in-memory data structures. This ensures that in the event of a crash,
 * the data can be recovered by replaying the log.
 *
 * Sequential I/O is used for maximum performance on both SSDs and HDDs.
 *
 * @param logFile The file to use for logging transactions.
 */
class WriteAheadLog(private val logFile: File) {

    private val channel: FileChannel

    init {
        if (logFile.parentFile?.exists() == false) {
            logFile.parentFile?.mkdirs()
        }

        channel = RandomAccessFile(logFile, "rw").channel

        // Ensure we append to the end of the file if it already exists
        channel.position(channel.size())
    }

    /**
     * Appends a new record to the log.
     *
     * The binary format for each entry is:
     * [KeySize (4 bytes)] [ValueSize (4 bytes)] [KeyBytes] [ValueBytes]
     *
     * @param key The key of the record.
     * @param value The value of the record.
     */
    fun append(key: ByteArray, value: ByteArray) {
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

    /**
     * Forces all buffered output to be written to the underlying storage device.
     *
     * This ensures that the data is physically stored on disk and will survive
     * a power loss or system crash.
     */
    fun flush() {
        channel.force(false)
    }

    /**
     * Closes the log file and releases the underlying file channel.
     */
    fun close() {
        channel.close()
    }
}
