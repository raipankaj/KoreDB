package com.pankaj.koredb.foundation

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * High-performance 2-Tier Block and Record Cache for KoreDB.
 *
 * Caches decompressed SSTable records and uncompressed data blocks in memory,
 * bypassing repeated disk I/O and decompression for frequently accessed entries.
 *
 * Implements an O(1) concurrent LRU eviction strategy bounded by a configurable byte size.
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

    private val map = ConcurrentHashMap<String, CacheEntry>()
    private val lock = ReentrantLock()
    private var head: CacheEntry? = null
    private var tail: CacheEntry? = null
    private val currentSizeBytes = AtomicLong(0)

    val hits = AtomicLong(0)
    val misses = AtomicLong(0)

    /**
     * Looks up an uncompressed block or record from the cache.
     */
    fun get(cacheKey: String): ByteArray? {
        val entry = map[cacheKey]
        if (entry != null) {
            hits.incrementAndGet()
            lock.withLock {
                moveToHead(entry)
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
        if (entrySize > maxSizeBytes) return // Item too large to cache

        lock.withLock {
            val existing = map[cacheKey]
            if (existing != null) {
                currentSizeBytes.addAndGet(-existing.sizeBytes)
                removeNode(existing)
            }

            val newEntry = CacheEntry(cacheKey, data, entrySize)
            addToHead(newEntry)
            map[cacheKey] = newEntry
            val newTotal = currentSizeBytes.addAndGet(entrySize)

            // Evict until within memory limit
            if (newTotal > maxSizeBytes) {
                evictUntilBudget()
            }
        }
    }

    fun remove(cacheKey: String) {
        lock.withLock {
            val entry = map.remove(cacheKey)
            if (entry != null) {
                currentSizeBytes.addAndGet(-entry.sizeBytes)
                removeNode(entry)
            }
        }
    }

    fun clear() {
        lock.withLock {
            map.clear()
            head = null
            tail = null
            currentSizeBytes.set(0)
            hits.set(0)
            misses.set(0)
        }
    }

    val sizeBytes: Long get() = currentSizeBytes.get()
    val entryCount: Int get() = map.size

    private fun evictUntilBudget() {
        while (currentSizeBytes.get() > maxSizeBytes && tail != null) {
            val toEvict = tail ?: break
            map.remove(toEvict.key)
            removeNode(toEvict)
            currentSizeBytes.addAndGet(-toEvict.sizeBytes)
        }
    }

    private fun addToHead(node: CacheEntry) {
        node.next = head
        node.prev = null
        head?.prev = node
        head = node
        if (tail == null) {
            tail = node
        }
    }

    private fun removeNode(node: CacheEntry) {
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

    private fun moveToHead(node: CacheEntry) {
        if (node === head) return
        removeNode(node)
        addToHead(node)
    }

    companion object {
        const val DEFAULT_CACHE_SIZE_BYTES: Long = 16L * 1024L * 1024L // 16 MB default
    }
}
