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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * High-performance Striped 2-Tier Block and Record Cache for KoreDB.
 *
 * Caches decompressed SSTable records and uncompressed data blocks in memory,
 * bypassing repeated disk I/O and decompression for frequently accessed entries.
 *
 * Employs lock striping and opportunistic LRU head updates to eliminate multi-threaded
 * lock contention on cache hits while preserving strict bounded capacity eviction.
 */
class BlockCache(
    private val maxSizeBytes: Long = DEFAULT_CACHE_SIZE_BYTES
) {

    private class CacheEntry(
        val key: String,
        val data: ByteArray,
        val sizeBytes: Long,
        var prev: CacheEntry? = null,
        var next: CacheEntry? = null
    )

    private val numStripes: Int = if (maxSizeBytes < 64 * 1024) 1 else 16
    private val stripeCapacity: Long = maxSizeBytes / numStripes

    private class Stripe(val maxSizeBytes: Long) {
        val lock = ReentrantLock()
        val map = ConcurrentHashMap<String, CacheEntry>()
        var head: CacheEntry? = null
        var tail: CacheEntry? = null
        val currentSizeBytes = AtomicLong(0)

        fun evictUntilBudget() {
            while (currentSizeBytes.get() > maxSizeBytes && tail != null) {
                val toEvict = tail ?: break
                map.remove(toEvict.key)
                removeNode(toEvict)
                currentSizeBytes.addAndGet(-toEvict.sizeBytes)
            }
        }

        fun addToHead(node: CacheEntry) {
            node.next = head
            node.prev = null
            head?.prev = node
            head = node
            if (tail == null) {
                tail = node
            }
        }

        fun removeNode(node: CacheEntry) {
            node.prev?.next = node.next
            node.next?.prev = node.prev
            if (node === head) {
                head = node.next
            }
            if (node === tail) {
                tail = node.prev
            }
            node.prev = null
            node.next = null
        }

        fun moveToHead(node: CacheEntry) {
            if (node === head) return
            if (map[node.key] !== node) return
            removeNode(node)
            addToHead(node)
        }
    }

    private val stripes: Array<Stripe> = Array(numStripes) { Stripe(stripeCapacity) }

    private fun getStripe(cacheKey: String): Stripe {
        if (numStripes == 1) return stripes[0]
        val hash = cacheKey.hashCode()
        val index = (hash xor (hash ushr 16)) and (numStripes - 1)
        return stripes[index]
    }

    val hits = AtomicLong(0)
    val misses = AtomicLong(0)

    /**
     * Looks up an uncompressed block or record from the cache.
     *
     * Uses lock striping and non-blocking tryLock on hits to eliminate thread serialization.
     */
    fun get(cacheKey: String): ByteArray? {
        val stripe = getStripe(cacheKey)
        val entry = stripe.map[cacheKey]
        if (entry != null) {
            hits.incrementAndGet()
            if (stripe.lock.tryLock()) {
                try {
                    stripe.moveToHead(entry)
                } finally {
                    stripe.lock.unlock()
                }
            }
            return entry.data
        }
        misses.incrementAndGet()
        return null
    }

    /**
     * Stores an uncompressed block or record in the cache.
     */
    fun put(cacheKey: String, data: ByteArray) {
        val entrySize = 32L + cacheKey.length * 2 + data.size
        val stripe = getStripe(cacheKey)
        if (entrySize > stripe.maxSizeBytes) return // Item too large to cache in stripe

        stripe.lock.withLock {
            val existing = stripe.map[cacheKey]
            if (existing != null) {
                stripe.currentSizeBytes.addAndGet(-existing.sizeBytes)
                stripe.removeNode(existing)
            }

            val newEntry = CacheEntry(cacheKey, data, entrySize)
            stripe.addToHead(newEntry)
            stripe.map[cacheKey] = newEntry
            val newTotal = stripe.currentSizeBytes.addAndGet(entrySize)

            if (newTotal > stripe.maxSizeBytes) {
                stripe.evictUntilBudget()
            }
        }
    }

    /**
     * Removes a specific entry by cacheKey.
     */
    fun remove(cacheKey: String) {
        val stripe = getStripe(cacheKey)
        stripe.lock.withLock {
            val entry = stripe.map.remove(cacheKey)
            if (entry != null) {
                stripe.currentSizeBytes.addAndGet(-entry.sizeBytes)
                stripe.removeNode(entry)
            }
        }
    }

    /**
     * Clears all cached blocks and resets counters.
     */
    fun clear() {
        for (i in 0 until numStripes) {
            val stripe = stripes[i]
            stripe.lock.withLock {
                stripe.map.clear()
                stripe.head = null
                stripe.tail = null
                stripe.currentSizeBytes.set(0)
            }
        }
        hits.set(0)
        misses.set(0)
    }

    /**
     * Removes all entries whose cacheKey starts with [prefix].
     */
    fun removePrefix(prefix: String) {
        for (i in 0 until numStripes) {
            val stripe = stripes[i]
            stripe.lock.withLock {
                val iter = stripe.map.entries.iterator()
                while (iter.hasNext()) {
                    val entry = iter.next()
                    if (entry.key.startsWith(prefix)) {
                        iter.remove()
                        stripe.currentSizeBytes.addAndGet(-entry.value.sizeBytes)
                        stripe.removeNode(entry.value)
                    }
                }
            }
        }
    }

    /**
     * Invalidates all cached blocks and records for the given SSTable name.
     */
    fun invalidateSSTable(sstableName: String) {
        val prefix = if (sstableName.endsWith(":")) sstableName else "$sstableName:"
        removePrefix(prefix)
    }

    val sizeBytes: Long
        get() {
            var total = 0L
            for (i in 0 until numStripes) {
                total += stripes[i].currentSizeBytes.get()
            }
            return total
        }

    val entryCount: Int
        get() {
            var count = 0
            for (i in 0 until numStripes) {
                count += stripes[i].map.size
            }
            return count
        }

    companion object {
        const val DEFAULT_CACHE_SIZE_BYTES: Long = 16L * 1024L * 1024L // 16 MB default
    }
}
