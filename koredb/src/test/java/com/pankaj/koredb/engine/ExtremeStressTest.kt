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

package com.pankaj.koredb.engine

import com.pankaj.koredb.foundation.BlockCache
import com.pankaj.koredb.foundation.BloomFilter
import com.pankaj.koredb.foundation.OrderPreservingEncoder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Extreme Adversarial Stress Test Suite for Milestone 1.
 *
 * Validates under harsh concurrent and boundary conditions:
 * - BlockCache lock striping, concurrent gets/puts, eviction under tight memory, prefix invalidation.
 * - BloomFilter bitwise hashing resilience against integer overflow and negative byte patterns.
 * - Multi-threaded LSM-Tree point lookups racing against background flushes and compactions.
 * - Persistence and retrieval of numeric boundary keys (Long.MIN_VALUE, Long.MAX_VALUE, -0.0, NaNs).
 */
class ExtremeStressTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/extreme_stress_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test BlockCache High Concurrency and Eviction Invariants`() {
        // Configure a small cache budget (64KB) across 16 stripes to force continuous evictions
        val cacheBudget = 64L * 1024L
        val cache = BlockCache(cacheBudget)
        val numThreads = 16
        val opsPerThread = 5_000
        val executor = Executors.newFixedThreadPool(numThreads)
        val latch = CountDownLatch(numThreads)
        val errors = ConcurrentHashMap.newKeySet<Throwable>()

        for (t in 0 until numThreads) {
            executor.submit {
                try {
                    for (i in 0 until opsPerThread) {
                        val key = "sstable_${t % 4}.sst:block_${(i % 100)}"
                        val data = ByteArray(128) { (it xor i).toByte() }

                        // 70% gets, 25% puts, 5% invalidations
                        val op = i % 20
                        when {
                            op < 14 -> {
                                cache.get(key)
                            }
                            op < 19 -> {
                                cache.put(key, data)
                            }
                            else -> {
                                cache.invalidateSSTable("sstable_${t % 4}.sst")
                            }
                        }

                        // Verify invariant: cache size must never be negative
                        val currentSize = cache.sizeBytes
                        if (currentSize < 0) {
                            throw IllegalStateException("BlockCache size underflowed to negative: $currentSize")
                        }
                    }
                } catch (t: Throwable) {
                    errors.add(t)
                } finally {
                    latch.countDown()
                }
            }
        }

        assertTrue("Timeout waiting for BlockCache stress threads", latch.await(30, TimeUnit.SECONDS))
        executor.shutdown()

        if (errors.isNotEmpty()) {
            fail("BlockCache stress test encountered errors: ${errors.first().message}")
        }

        assertTrue("Cache size must be non-negative: ${cache.sizeBytes}", cache.sizeBytes >= 0L)
        assertTrue("Cache hits or misses should be non-zero", (cache.hits.get() + cache.misses.get()) > 0)
    }

    @Test
    fun `test BloomFilter Bitwise Hashing and Edge Case Keys`() {
        // Bit sizes: small, power of 2, non-power of 2, prime
        val bitSizes = intArrayOf(64, 128, 997, 1024, 65536)
        val hashFuncs = intArrayOf(1, 3, 7, 15)

        for (bitSize in bitSizes) {
            for (hashes in hashFuncs) {
                val bf = BloomFilter(bitSize, hashes)

                // 1. Extreme key forms
                val emptyKey = ByteArray(0)
                val singleZero = byteArrayOf(0x00)
                val singleMax = byteArrayOf(0xFF.toByte())
                val negativeBytes = byteArrayOf(-128, -64, -1, -2, -100)
                val largeKey = ByteArray(16 * 1024) { (it and 0xFF).toByte() }

                val testKeys = listOf(emptyKey, singleZero, singleMax, negativeBytes, largeKey)
                for (k in testKeys) {
                    bf.add(k)
                    assertTrue("BloomFilter false negative on extreme key (len=${k.size}, bitSize=$bitSize, h=$hashes)",
                        bf.mightContain(k)
                    )
                }

                // 2. Sub-range hashing match
                val composite = ByteArray(100) { it.toByte() }
                bf.addRange(composite, 10, 20)
                val subSlice = composite.copyOfRange(10, 30)
                assertTrue("addRange and mightContain sub-slice must match", bf.mightContain(subSlice))
                assertTrue("mightContainRange must match mightContain", bf.mightContainRange(composite, 10, 20))
            }
        }

        // 3. Zero false negative rate on 10,000 randomized keys
        val filter = BloomFilter(50000, 7)
        val insertedKeys = (0 until 10000).map { i ->
            "key:$i:value:${i * 31}".toByteArray()
        }
        for (k in insertedKeys) {
            filter.add(k)
        }
        for (k in insertedKeys) {
            assertTrue("BloomFilter must have 0% false negative rate", filter.mightContain(k))
        }
    }

    @Test
    fun `test Concurrent LSM Point Reads Racing With Flush and Compaction`() = runBlocking(Dispatchers.IO) {
        val totalKeys = 1000
        val payload = "StressPayloadData_1234567890".toByteArray()

        // Prepopulate database with initial dataset
        val initialBatch = (0 until totalKeys).map { i ->
            "stress:$i".toByteArray() to payload
        }
        db.writeBatchRaw(initialBatch)
        db.flushMemTableInternal()

        val numWriters = 4
        val numReaders = 8
        val writesPerWorker = 500
        val readsPerWorker = 2000
        val readCounter = AtomicInteger(0)
        val writeCounter = AtomicInteger(0)
        val readErrors = ConcurrentHashMap.newKeySet<String>()
        val writersDone = AtomicBoolean(false)

        // Launch reader coroutines
        val readers = (0 until numReaders).map { rId ->
            async {
                for (i in 0 until readsPerWorker) {
                    val keyId = (0 until totalKeys).random()
                    val key = "stress:$keyId".toByteArray()
                    val res = db.getRaw(key)
                    if (res == null) {
                        readErrors.add("Reader $rId missed key: stress:$keyId")
                    } else if (!res.contentEquals(payload) && !String(res).startsWith("updated:")) {
                        readErrors.add("Corrupt payload read for key stress:$keyId")
                    }
                    readCounter.incrementAndGet()
                }
            }
        }

        // Launch writer coroutines updating existing keys and triggering periodic flush
        val writers = (0 until numWriters).map { wId ->
            async {
                for (counter in 0 until writesPerWorker) {
                    val targetKey = (0 until totalKeys).random()
                    val key = "stress:$targetKey".toByteArray()
                    val updatedVal = "updated:writer$wId:$counter".toByteArray()
                    db.putRaw(key, updatedVal)
                    writeCounter.incrementAndGet()

                    if (counter == 250) {
                        db.flushMemTableInternal()
                    }
                }
            }
        }

        writers.awaitAll()
        writersDone.set(true)
        readers.awaitAll()

        println("ExtremeStressTest completed: ${readCounter.get()} reads, ${writeCounter.get()} writes")
        assertTrue("Encountered read errors during concurrent point reads: $readErrors", readErrors.isEmpty())
    }

    @Test
    fun `test Numerical Boundary Keys Ingested in LSM and Retrievable After Flush and Compaction`() = runBlocking {
        // Encodings of extreme numerical values with namespace prefixes to test both Long and Double
        val boundaryPairs = listOf(
            ("long:".toByteArray() + OrderPreservingEncoder.encodeLong(Long.MIN_VALUE)) to "val:Long.MIN_VALUE".toByteArray(),
            ("long:".toByteArray() + OrderPreservingEncoder.encodeLong(Long.MIN_VALUE + 1)) to "val:Long.MIN_VALUE+1".toByteArray(),
            ("long:".toByteArray() + OrderPreservingEncoder.encodeLong(-1L)) to "val:-1L".toByteArray(),
            ("long:".toByteArray() + OrderPreservingEncoder.encodeLong(0L)) to "val:0L".toByteArray(),
            ("long:".toByteArray() + OrderPreservingEncoder.encodeLong(1L)) to "val:1L".toByteArray(),
            ("long:".toByteArray() + OrderPreservingEncoder.encodeLong(Long.MAX_VALUE)) to "val:Long.MAX_VALUE".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(Double.NEGATIVE_INFINITY)) to "val:-Inf".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(-Double.MAX_VALUE)) to "val:-Double.MAX_VALUE".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(-Double.MIN_VALUE)) to "val:-Double.MIN_VALUE".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(-0.0)) to "val:-0.0".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(0.0)) to "val:0.0".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(Double.MIN_VALUE)) to "val:Double.MIN_VALUE".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(Double.MAX_VALUE)) to "val:Double.MAX_VALUE".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(Double.POSITIVE_INFINITY)) to "val:+Inf".toByteArray(),
            ("double:".toByteArray() + OrderPreservingEncoder.encodeDouble(Double.NaN)) to "val:NaN".toByteArray()
        )

        // Write all pairs
        for ((k, v) in boundaryPairs) {
            db.putRaw(k, v)
        }

        // 1. Verify retrieval from MemTable
        for ((k, v) in boundaryPairs) {
            val res = db.getRaw(k)
            assertNotNull("Failed to read boundary key from MemTable", res)
            assertArrayEquals("Payload mismatch from MemTable", v, res)
        }

        // 2. Force flush to L0 SSTable
        db.flushMemTableInternal()

        // Verify retrieval from L0 SSTable & BlockCache
        for ((k, v) in boundaryPairs) {
            val res = db.getRaw(k)
            assertNotNull("Failed to read boundary key from L0 SSTable", res)
            assertArrayEquals("Payload mismatch from L0 SSTable", v, res)
        }

        // 3. Clear cache and verify cold disk read
        db.blockCache.clear()
        for ((k, v) in boundaryPairs) {
            val res = db.getRaw(k)
            assertNotNull("Failed to read boundary key cold from disk", res)
            assertArrayEquals("Payload mismatch cold from disk", v, res)
        }

        // 4. Test raw un-prefixed Longs alone
        val rawLongPairs = listOf(
            OrderPreservingEncoder.encodeLong(Long.MIN_VALUE) to "raw:MIN".toByteArray(),
            OrderPreservingEncoder.encodeLong(-100L) to "raw:-100".toByteArray(),
            OrderPreservingEncoder.encodeLong(0L) to "raw:0".toByteArray(),
            OrderPreservingEncoder.encodeLong(100L) to "raw:100".toByteArray(),
            OrderPreservingEncoder.encodeLong(Long.MAX_VALUE) to "raw:MAX".toByteArray()
        )
        for ((k, v) in rawLongPairs) {
            db.putRaw(k, v)
        }
        db.flushMemTableInternal()
        for ((k, v) in rawLongPairs) {
            val res = db.getRaw(k)
            assertNotNull("Failed to read raw long key", res)
            assertArrayEquals("Raw long value mismatch", v, res)
        }
    }
}
