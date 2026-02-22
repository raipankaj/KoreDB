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
 *
 * This iterator uses a snapshot of the SSTable's memory-mapped buffer to provide
 * thread-safe access. It is designed to be used with a [java.util.PriorityQueue]
 * for multi-way merging during compaction or range scans.
 *
 * @property fileIndex The sequence index of the SSTable file, used to determine
 *                     precedence when keys are identical (newer files win).
 */
class SSTableIterator(
    reader: SSTableReader,
    val fileIndex: Int
) : Comparable<SSTableIterator> {

    private val buffer: ByteBuffer = reader.getBufferSnapshot()
    private val dataEndOffset = reader.dataEndOffset

    /**
     * The key at the current iterator position.
     */
    var currentKey: ByteArray? = null
        private set

    /**
     * The value at the current iterator position.
     */
    var currentValue: ByteArray? = null
        private set

    init {
        advance()
    }

    /**
     * Advances the iterator to the next key-value pair.
     *
     * @return true if a record was successfully read, false if the end of the data section is reached.
     */
    fun advance(): Boolean {
        if (buffer.position() >= dataEndOffset) {
            currentKey = null
            currentValue = null
            return false
        }

        val keySize = buffer.getInt()
        val valueSize = buffer.getInt()

        val keyBytes = ByteArray(keySize)
        buffer.get(keyBytes)

        val valueBytes = ByteArray(valueSize)
        buffer.get(valueBytes)

        currentKey = keyBytes
        currentValue = valueBytes
        return true
    }

    /**
     * Compares this iterator with another based on the [currentKey].
     *
     * Sorting logic:
     * 1. Smallest key first (lexicographical).
     * 2. If keys are equal, the newest file (highest [fileIndex]) comes first.
     */
    override fun compareTo(other: SSTableIterator): Int {
        val myKey = currentKey ?: return 1
        val otherKey = other.currentKey ?: return -1

        val cmp = ByteArrayComparator.compare(myKey, otherKey)
        if (cmp != 0) return cmp

        return other.fileIndex.compareTo(this.fileIndex)
    }
}
