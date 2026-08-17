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

import java.nio.ByteBuffer

/**
 * An iterator for traversing the key-value pairs within a single SSTable.
 */
class SSTableIterator(
    private val reader: SSTableReader,
    override val priority: Int,
    startOffset: Int = 0,
    private var startKey: ByteArray? = null,
    private val endKey: ByteArray? = null
) : KoreIterator {

    private val buffer: ByteBuffer = reader.getBufferSnapshot()
    private val dataEndOffset = reader.dataEndOffset

    override var currentKey: ByteArray? = null
        private set

    // Store metadata for lazy and sequential value loading
    private var valueOffset: Int = -1
    private var valueSize: Int = -1
    private var valueRead: Boolean = false

    init {
        buffer.position(startOffset)
        advance()
    }

    override fun value(): ByteArray? {
        if (valueOffset == -1) return null
        if (valueSize == 0) return EMPTY_BYTE_ARRAY

        // Optimization: If we are already at the valueOffset, this is a sequential read.
        // If not (e.g., value() called multiple times or out of order), we must jump.
        val currentPos = buffer.position()
        if (currentPos != valueOffset) {
            buffer.position(valueOffset)
        }

        val valueBytes = ByteArray(valueSize)
        buffer.get(valueBytes)
        valueRead = true
        
        // No need to restore position if we are doing a range scan, 
        // as advance() will handle the skip if valueRead is false.
        return reader.decompressValue(valueBytes)
    }

    /**
     * Advances the iterator to the next key-value pair.
     */
    override fun advance(): Boolean {
        // If the previous value was NOT read, we must skip it now to maintain position.
        if (valueOffset != -1 && !valueRead) {
            buffer.position(valueOffset + valueSize)
        }

        while (buffer.position() < dataEndOffset) {
            val keyOffset = buffer.position()
            val keySize = buffer.getInt()
            val vSize = buffer.getInt()

            // 1. Skip keys before startKey (optimized with zero-allocation comparison)
            if (startKey != null) {
                val cmp = reader.compareBufferWithKey(buffer, keyOffset + 8, keySize, startKey!!)
                if (cmp < 0) {
                    buffer.position(keyOffset + 8 + keySize + vSize) // Skip key and value bytes
                    continue
                }
                // Target range reached
                startKey = null
            }

            // 2. Early termination if we've passed the endKey
            // endKey is exclusive, so if currentKey >= endKey, we terminate.
            if (endKey != null) {
                val cmp = reader.compareBufferWithKey(buffer, keyOffset + 8, keySize, endKey)
                if (cmp >= 0) {
                    break
                }
            }

            // Valid key found. Now allocate it so it can be used in the PriorityQueue.
            val keyBytes = ByteArray(keySize)
            buffer.get(keyBytes)

            currentKey = keyBytes
            valueOffset = buffer.position()
            valueSize = vSize
            valueRead = false // Value is now pending
            return true
        }

        currentKey = null
        valueOffset = -1
        valueSize = -1
        valueRead = false
        buffer.position(dataEndOffset.toInt()) 
        return false
    }

    companion object {
        private val EMPTY_BYTE_ARRAY = ByteArray(0)
    }
}
