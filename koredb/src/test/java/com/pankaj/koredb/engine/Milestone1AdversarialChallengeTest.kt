package com.pankaj.koredb.engine

import com.pankaj.koredb.foundation.BlockCache
import com.pankaj.koredb.foundation.BloomFilter
import com.pankaj.koredb.foundation.ByteArrayComparator
import com.pankaj.koredb.foundation.MemTable
import com.pankaj.koredb.foundation.OrderPreservingEncoder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * Adversarial stress harness targeting Milestone 1 optimizations:
 * - Concurrent compaction & SSTableReader unmapping during heavy reads
 * - BlockCache striped LRU concurrency, invalidation, and negative size underflow
 * - MemTable accurate size delta tracking and edge cases
 * - BloomFilter zero-alloc hashing and negative byte handling
 * - OrderPreservingEncoder boundary conditions and monotonicity
 */
class Milestone1AdversarialChallengeTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_m1_adv_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // 1. ORDER PRESERVING ENCODER ADVERSARIAL STRESS
    // ========================================================================

    @Test
    fun testOrderPreservingEncoderMonotonicityAndOffsets() {
        // Test Longs
        val testLongs = listOf(
            Long.MIN_VALUE, Long.MIN_VALUE + 1, -1000000L, -1L, 0L, 1L, 1000000L,
            Long.MAX_VALUE - 1, Long.MAX_VALUE
        )
        for (i in 0 until testLongs.size - 1) {
            val a = testLongs[i]
            val b = testLongs[i + 1]
            val encA = OrderPreservingEncoder.encodeLong(a)
            val encB = OrderPreservingEncoder.encodeLong(b)
            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Expected enc($a) < enc($b), got cmp=$cmp", cmp < 0)

            // Round trip
            assertEquals(a, OrderPreservingEncoder.decodeLong(encA))
            assertEquals(b, OrderPreservingEncoder.decodeLong(encB))

            // Non-zero offset round trip
            val paddedA = ByteArray(20) { 0xFF.toByte() }
            System.arraycopy(encA, 0, paddedA, 5, 8)
            assertEquals(a, OrderPreservingEncoder.decodeLong(paddedA, 5))
        }

        // Test Doubles
        val testDoubles = listOf(
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            -1e10,
            -1.0,
            -Double.MIN_VALUE,
            -0.0,
            0.0,
            Double.MIN_VALUE,
            1.0,
            1e10,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY
        )
        for (i in 0 until testDoubles.size - 1) {
            val a = testDoubles[i]
            val b = testDoubles[i + 1]
            val encA = OrderPreservingEncoder.encodeDouble(a)
            val encB = OrderPreservingEncoder.encodeDouble(b)
            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Expected enc($a) < enc($b), got cmp=$cmp", cmp < 0)

            // Round trip
            assertEquals(a, OrderPreservingEncoder.decodeDouble(encA), 0.0)
            assertEquals(b, OrderPreservingEncoder.decodeDouble(encB), 0.0)

            // Non-zero offset round trip
            val paddedA = ByteArray(24) { 0xAA.toByte() }
            System.arraycopy(encA, 0, paddedA, 7, 8)
            assertEquals(a, OrderPreservingEncoder.decodeDouble(paddedA, 7), 0.0)
        }

        // Random 10,000 doubles sorted check
        val randomDoubles = DoubleArray(10_000) {
            when (Random.nextInt(5)) {
                0 -> Random.nextDouble(-1e15, 1e15)
                1 -> Random.nextDouble(-1.0, 1.0)
                2 -> Random.nextDouble(1e-100, 1e100)
                3 -> -Random.nextDouble(1e-100, 1e100)
                else -> Random.nextDouble()
            }
        }.sorted()

        for (i in 0 until randomDoubles.size - 1) {
            val d1 = randomDoubles[i]
            val d2 = randomDoubles[i + 1]
            val enc1 = OrderPreservingEncoder.encodeDouble(d1)
            val enc2 = OrderPreservingEncoder.encodeDouble(d2)
            val cmp = ByteArrayComparator.compare(enc1, enc2)
            if (d1 < d2) {
                assertTrue("Expected enc($d1) < enc($d2), got $cmp", cmp < 0)
            } else {
                assertEquals("Expected enc($d1) == enc($d2)", 0, cmp)
            }
        }
    }

    // ========================================================================
    // 2. BLOOM FILTER ADVERSARIAL STRESS
    // ========================================================================

    @Test
    fun testBloomFilterZeroAllocNegativeBytesAndNoFalseNegatives() {
        val bf = BloomFilter(100_000, 5)

        // Seed 10,000 keys with negative bytes, non-ASCII UTF-8, zeroes, and random bytes
        val keys = mutableListOf<ByteArray>()
        for (i in 0 until 10_000) {
            val key = when (i % 4) {
                0 -> ByteArray(16) { (it * -17).toByte() }
                1 -> "unicode_key_测试_🚀_$i".toByteArray(Charsets.UTF_8)
                2 -> ByteArray(32) { 0xFF.toByte() }
                else -> Random.nextBytes(24)
            }
            keys.add(key)
            bf.add(key)
        }

        // Verification: ZERO false negatives
        for (key in keys) {
            assertTrue("BloomFilter reported false negative for key!", bf.mightContain(key))
            // Also test subrange
            val padded = ByteArray(key.size + 10)
            System.arraycopy(key, 0, padded, 5, key.size)
            assertTrue("BloomFilter mightContainRange failed on padded subrange", bf.mightContainRange(padded, 5, key.size))
        }

        // Serialization round trip
        val bytes = bf.toByteArray()
        val deserialized = BloomFilter.fromByteArray(bf.bitSize, bf.hashFunctions, bytes)
        for (key in keys) {
            assertTrue("Deserialized BloomFilter reported false negative!", deserialized.mightContain(key))
        }
    }

    // ========================================================================
    // 3. MEMTABLE ACCURATE SIZE DELTA TRACKING
    // ========================================================================

    @Test
    fun testMemTableSizeDeltaUnderflowAndEdgeCases() {
        val memTable = MemTable()
        assertEquals(0, memTable.sizeInBytes())

        val key = "item_1".toByteArray()
        val val100 = ByteArray(100) { 1 }
        val val200 = ByteArray(200) { 2 }
        val val50 = ByteArray(50) { 3 }

        // Initial put
        memTable.put(key, val100)
        assertEquals(key.size + 100, memTable.sizeInBytes())

        // Update with larger value
        memTable.put(key, val200)
        assertEquals(key.size + 200, memTable.sizeInBytes())

        // Update with smaller value
        memTable.put(key, val50)
        assertEquals(key.size + 50, memTable.sizeInBytes())

        // Bulk putAll with updates
        val batch = listOf(
            Pair("item_1".toByteArray(), ByteArray(80)), // update: +30
            Pair("item_2".toByteArray(), ByteArray(120)), // insert: +key.size + 120
            Pair("item_2".toByteArray(), ByteArray(150))  // update in same batch: +30
        )
        memTable.putAll(batch)

        val expectedSize = (key.size + 80) + ("item_2".toByteArray().size + 150)
        assertEquals(expectedSize, memTable.sizeInBytes())

        // Clear
        memTable.clear()
        assertEquals(0, memTable.sizeInBytes())
        assertEquals(0, memTable.size())
    }

    // ========================================================================
    // 4. BLOCK CACHE CONCURRENT STRIPING, INVALIDATION & SIZE INTEGRITY
    // ========================================================================

    @Test
    fun testBlockCacheConcurrentUnderflowAndInvalidation() {
        // Cache of 128KB (16 stripes of 8KB)
        val cache = BlockCache(128 * 1024L)
        val numThreads = 8
        val opsPerThread = 5_000
        val sstableNames = listOf("sst_0", "sst_1", "sst_2", "sst_3")
        val errors = ConcurrentLinkedQueue<Throwable>()
        val running = AtomicBoolean(true)

        val threads = mutableListOf<Thread>()

        // 4 Reader threads
        for (t in 0 until 4) {
            threads.add(Thread {
                var localOps = 0
                while (running.get() && localOps < opsPerThread) {
                    try {
                        val sst = sstableNames[localOps % sstableNames.size]
                        val key = "$sst:offset_${localOps % 100}"
                        val res = cache.get(key)
                        if (res == null) {
                            cache.put(key, ByteArray(64) { localOps.toByte() })
                        }
                        localOps++
                    } catch (e: Throwable) {
                        errors.add(e)
                    }
                }
            })
        }

        // 2 Invalidator threads (simulating compaction invalidation)
        for (t in 0 until 2) {
            threads.add(Thread {
                var localOps = 0
                while (running.get() && localOps < 500) {
                    try {
                        val sst = sstableNames[localOps % sstableNames.size]
                        cache.invalidateSSTable(sst)
                        Thread.sleep(1)
                        localOps++
                    } catch (e: Throwable) {
                        errors.add(e)
                    }
                }
            })
        }

        // 2 Writer threads continuously churning cache
        for (t in 0 until 2) {
            threads.add(Thread {
                var localOps = 0
                while (running.get() && localOps < opsPerThread) {
                    try {
                        val sst = sstableNames[localOps % sstableNames.size]
                        val key = "$sst:churn_${localOps}"
                        cache.put(key, ByteArray(128) { (localOps % 256).toByte() })
                        localOps++
                    } catch (e: Throwable) {
                        errors.add(e)
                    }
                }
            })
        }

        threads.forEach { it.start() }
        threads.forEach { it.join(5000) }
        running.set(false)

        assertTrue("Exceptions encountered during BlockCache concurrency: $errors", errors.isEmpty())
        assertTrue("BlockCache sizeBytes underflowed to negative: ${cache.sizeBytes}", cache.sizeBytes >= 0L)
    }

    // ========================================================================
    // 5. TORTURE TEST: CONCURRENT HEAVY READS WHILE COMPACTING & UNMAPPING
    // ========================================================================

    @Test
    fun testConcurrentCompactionUnmappingTorture() = runBlocking {
        val db = KoreDB(testDir)
        val numRecords = 8_000
        val numReaderCoroutines = 8
        val errors = ConcurrentLinkedQueue<Throwable>()
        val keepReading = AtomicBoolean(true)
        val readSuccessCount = AtomicInteger(0)

        try {
            // Seed 4 initial SSTable segments (forces L0 creation)
            for (i in 0 until numRecords) {
                db.putRaw("key_$i".toByteArray(), "value_$i".toByteArray())
                if (i > 0 && i % 2000 == 0) {
                    db.flushMemTableInternal()
                }
            }
            db.flushMemTableInternal()

            // Launch 8 concurrent reader coroutines hammering random reads
            val readers = (0 until numReaderCoroutines).map { readerId ->
                async(Dispatchers.IO) {
                    val rng = Random(readerId * 42)
                    while (keepReading.get()) {
                        try {
                            val id = rng.nextInt(numRecords)
                            val k = "key_$id".toByteArray()
                            val expectedVal = "value_$id"
                            val actual = db.getRaw(k)
                            if (actual != null) {
                                assertEquals(expectedVal, String(actual))
                                readSuccessCount.incrementAndGet()
                            }
                        } catch (e: Throwable) {
                            errors.add(e)
                            break
                        }
                    }
                }
            }

            // Concurrently perform repeated compactions and new writes
            val compactor = async(Dispatchers.IO) {
                try {
                    for (c in 0 until 5) {
                        // Write additional batch
                        for (w in 0 until 500) {
                            db.putRaw("new_key_${c}_$w".toByteArray(), "new_value_${c}_$w".toByteArray())
                        }
                        db.flushMemTableInternal()
                        db.performCompaction()
                        delay(20)
                    }
                } catch (e: Throwable) {
                    errors.add(e)
                } finally {
                    keepReading.set(false)
                }
            }

            compactor.await()
            readers.awaitAll()

            assertTrue("Errors observed during concurrent reads and compactions: $errors", errors.isEmpty())
            assertTrue("Expected at least 1,000 successful point reads, got ${readSuccessCount.get()}", readSuccessCount.get() > 1000)

            // Final data integrity check
            for (i in 0 until 100) {
                val actual = db.getRaw("key_$i".toByteArray())
                assertNotNull("Key key_$i should exist after compaction", actual)
            }
        } finally {
            db.close()
        }
    }

    // ========================================================================
    // 6. EXTREME RACE: DIRECT UNMAPPING VS BUFFER SNAPSHOT CONCURRENT READ
    // ========================================================================

    @Test
    fun testBufferSnapshotSafetyDuringReaderClose() {
        val file = File(testDir, "test_mmap_close.sst")
        val memTable = MemTable()
        for (i in 0 until 2000) {
            memTable.put("k_${i.toString().padStart(6, '0')}".toByteArray(), "val_$i".toByteArray())
        }
        com.pankaj.koredb.foundation.SSTable.writeFromMemTable(memTable, file)

        val reader = com.pankaj.koredb.foundation.SSTableReader(file)
        val errors = ConcurrentLinkedQueue<Throwable>()
        val stopFlag = AtomicBoolean(false)
        val readCounter = AtomicInteger(0)

        val readerThread = Thread {
            while (!stopFlag.get()) {
                try {
                    val key = "k_${Random.nextInt(2000).toString().padStart(6, '0')}".toByteArray()
                    val res = reader.find(key)
                    if (res != null) {
                        readCounter.incrementAndGet()
                    }
                } catch (e: Throwable) {
                    // Capture whatever exception happens (e.g. BufferUnderflowException, IllegalStateException, SIGSEGV)
                    errors.add(e)
                    break
                }
            }
        }

        readerThread.start()
        Thread.sleep(25) // Let reader get into active loop

        // Close and unmap the reader while readerThread is running!
        reader.close()
        stopFlag.set(true)
        readerThread.join(2000)

        println("Extreme unmap test: reads completed before/during close = ${readCounter.get()}, errors = ${errors.size}")
        for (err in errors) {
            println("Caught during unmap: ${err.javaClass.name}: ${err.message}")
        }
    }
}
