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

/**
 * Base interface for all iterators used in K-Way merging.
 */
interface KoreIterator : Comparable<KoreIterator> {
    val currentKey: ByteArray?
    val priority: Int // Higher priority means newer data
    
    /**
     * Lazily retrieves the value for the current key.
     */
    fun value(): ByteArray?

    fun advance(): Boolean

    override fun compareTo(other: KoreIterator): Int {
        val thisKey = currentKey ?: return 1
        val otherKey = other.currentKey ?: return -1
        
        val cmp = ByteArrayComparator.compare(thisKey, otherKey)
        if (cmp != 0) return cmp
        
        // If keys are equal, higher priority (newer) comes first
        return other.priority.compareTo(this.priority)
    }
}

/**
 * Iterator for the in-memory MemTable.
 */
class MemTableIterator(
    private val iterator: Iterator<Map.Entry<ByteArray, ByteArray>>,
    override val priority: Int,
    private val endKey: ByteArray? = null
) : KoreIterator {
    override var currentKey: ByteArray? = null
    private var internalValue: ByteArray? = null

    init {
        advance()
    }

    override fun value(): ByteArray? = internalValue

    override fun advance(): Boolean {
        if (iterator.hasNext()) {
            val entry = iterator.next()
            if (endKey != null && ByteArrayComparator.compare(entry.key, endKey) >= 0) {
                currentKey = null
                internalValue = null
                return false
            }
            currentKey = entry.key
            internalValue = entry.value
            return true
        }
        currentKey = null
        internalValue = null
        return false
    }
}
