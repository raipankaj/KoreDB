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

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The in-memory buffer for KoreDB.
 *
 * The [MemTable] acts as the first tier of storage in the LSM-Tree architecture.
 * All write operations are first applied here. When the [MemTable] reaches a 
 * certain size threshold, it is flushed to disk as an immutable SSTable.
 *
 * This implementation uses a [ConcurrentSkipListMap] to provide thread-safe, 
 * concurrent access while maintaining keys in sorted order.
 */
class MemTable {
    // Lock-free highly concurrent tree, sorted by our custom byte comparator
    private val table = ConcurrentSkipListMap<ByteArray, ByteArray>(ByteArrayComparator)

    // Tracks the memory footprint to know when to flush to disk
    private val currentSizeBytes = AtomicInteger(0)

    /**
     * Inserts or updates a key-value pair in the table.
     *
     * @param key The key to insert.
     * @param value The value associated with the key.
     */
    fun put(key: ByteArray, value: ByteArray) {
        val previousValue = table.put(key, value)

        // Calculate memory added (Key + Value). If we updated an existing key, subtract old value size
        var sizeDelta = key.size + value.size
        if (previousValue != null) {
            sizeDelta -= previousValue.size
        }
        currentSizeBytes.addAndGet(sizeDelta)
    }

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key The key to look up.
     * @return The associated value, or null if not found.
     */
    fun get(key: ByteArray): ByteArray? {
        return table[key]
    }

    /**
     * Returns the approximate current memory usage of the table in bytes.
     */
    fun sizeInBytes(): Int = currentSizeBytes.get()

    /**
     * Returns a snapshot of the sorted entries currently in the table.
     *
     * @return A [Sequence] of sorted map entries.
     */
    fun getSortedEntries(): Sequence<Map.Entry<ByteArray, ByteArray>> {
        return table.entries.asSequence()
    }

    /**
     * Clears all entries from the table and resets the size counter.
     */
    fun clear() {
        table.clear()
        currentSizeBytes.set(0)
    }
}
