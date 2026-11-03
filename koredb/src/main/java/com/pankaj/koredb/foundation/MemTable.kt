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

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

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
    private val currentSizeBytes = AtomicLong(0L)

    /**
     * Inserts or updates a key-value pair in the table.
     *
     * @param key The key to insert.
     * @param value The value associated with the key.
     */
    fun put(key: ByteArray, value: ByteArray) {
        val previousValue = table.put(key, value)

        // Calculate memory added (Key + Value). If updating an existing key, adjust only for value difference
        val sizeDelta = if (previousValue == null) {
            (key.size + value.size).toLong()
        } else {
            (value.size - previousValue.size).toLong()
        }
        currentSizeBytes.addAndGet(sizeDelta)
    }

    /**
     * High-throughput bulk insertion of multiple key-value pairs.
     *
     * This method is significantly faster than calling [put] in a loop because:
     * - Size tracking is accumulated locally and applied in a single atomic update.
     * - Reduces the overhead of individual [AtomicLong.addAndGet] calls.
     *
     * @param entries The list of key-value pairs to insert.
     */
    fun putAll(entries: List<Pair<ByteArray, ByteArray>>) {
        var totalDelta = 0L
        for (i in entries.indices) {
            val entry = entries[i]
            val previousValue = table.put(entry.first, entry.second)
            if (previousValue == null) {
                totalDelta += (entry.first.size + entry.second.size).toLong()
            } else {
                totalDelta += (entry.second.size - previousValue.size).toLong()
            }
        }
        currentSizeBytes.addAndGet(totalDelta)
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
    fun sizeInBytes(): Int = currentSizeBytes.get().coerceIn(0L, Int.MAX_VALUE.toLong()).toInt()

    /**
     * Returns a snapshot of the sorted entries currently in the table.
     *
     * @return A [Sequence] of sorted map entries.
     */
    fun getSortedEntries(): Sequence<Map.Entry<ByteArray, ByteArray>> {
        return table.entries.asSequence()
    }

    /**
     * Jumps directly to the closest matching key in the SkipList.
     * $O(\log N)$ time complexity.
     */
    fun getTailEntries(prefix: ByteArray): Sequence<Map.Entry<ByteArray, ByteArray>> {
        return table.tailMap(prefix).entries.asSequence()
    }

    /**
     * Returns the total number of entries stored in the MemTable.
     */
    fun size(): Int = table.size

    /**
     * Clears all entries from the table and resets the size counter.
     */
    fun clear() {
        table.clear()
        currentSizeBytes.set(0)
    }
}
