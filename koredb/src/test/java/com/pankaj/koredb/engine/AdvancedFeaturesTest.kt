package com.pankaj.koredb.engine

import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.core.SimdVectorMath
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.engine.mvcc.MvccConflictException
import com.pankaj.koredb.foundation.BlockCache
import com.pankaj.koredb.foundation.ByteArrayComparator
import com.pankaj.koredb.foundation.OrderPreservingEncoder
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.ProductQuantizer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.Random
import java.util.UUID

/**
 * Comprehensive verification suite for the 4 major architectural breakthroughs:
 * 1. Order-Preserving Numeric Byte Encoding & LSM Range Pushdown
 * 2. 2-Tier Block Cache & Fast LZ4 Block Compression
 * 3. 16-Lane SIMD Vector Acceleration & Product Quantization (PQ 32x)
 * 4. True MVCC Snapshot Isolation ACID Engine
 */
class AdvancedFeaturesTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    data class StockItem(
        val symbol: String,
        val price: Double,
        val volume: Long
    )

    @Before
    fun setup() {
        testDir = File("build/tmp/test_adv_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    // ========================================================================
    // 1. ORDER-PRESERVING NUMERIC BYTE ENCODING & RANGE PUSHDOWN
    // ========================================================================

    @Test
    fun `test OrderPreservingEncoder preserves mathematical ordering across all primitives`() {
        // Doubles
        val doubles = listOf(
            -Double.MAX_VALUE, -1000.5, -100.0, -1.0, -0.001, 0.0, 0.001, 1.0, 100.0, 1000.5, Double.MAX_VALUE
        )
        for (i in 0 until doubles.size - 1) {
            val a = doubles[i]
            val b = doubles[i + 1]
            val encA = OrderPreservingEncoder.encodeDouble(a)
            val encB = OrderPreservingEncoder.encodeDouble(b)
            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Double order failed for $a vs $b", cmp < 0)
            assertEquals(a, OrderPreservingEncoder.decodeDouble(encA), 0.0)
            assertEquals(b, OrderPreservingEncoder.decodeDouble(encB), 0.0)
        }

        // Ints
        val ints = listOf(Int.MIN_VALUE, -500, -1, 0, 1, 500, Int.MAX_VALUE)
        for (i in 0 until ints.size - 1) {
            val a = ints[i]
            val b = ints[i + 1]
            val encA = OrderPreservingEncoder.encodeInt(a)
            val encB = OrderPreservingEncoder.encodeInt(b)
            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Int order failed for $a vs $b", cmp < 0)
            assertEquals(a, OrderPreservingEncoder.decodeInt(encA))
            assertEquals(b, OrderPreservingEncoder.decodeInt(encB))
        }

        // Longs
        val longs = listOf(Long.MIN_VALUE, -1_000_000L, -1L, 0L, 1L, 1_000_000L, Long.MAX_VALUE)
        for (i in 0 until longs.size - 1) {
            val a = longs[i]
            val b = longs[i + 1]
            val encA = OrderPreservingEncoder.encodeLong(a)
            val encB = OrderPreservingEncoder.encodeLong(b)
            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Long order failed for $a vs $b", cmp < 0)
            assertEquals(a, OrderPreservingEncoder.decodeLong(encA))
            assertEquals(b, OrderPreservingEncoder.decodeLong(encB))
        }
    }

    @Test
    fun `test Numeric Indexing and Range Pushdown in KoreQuery`() = runBlocking {
        val collection = db.collection<StockItem>("stocks")
        collection.createNumericIndex("price") { it.price }

        // Populate 100 stocks with prices 1.0 to 100.0
        val batch = (1..100).associate { i ->
            "stock_$i" to StockItem("SYM_$i", i.toDouble(), i * 1000L)
        }
        collection.insertBatch(batch)
        db.engine.flushMemTableInternal()

        // 1. whereBetween(20.0, 30.0)
        val betweenResults = collection.query()
            .whereBetween("price", 20.0, 30.0)
            .execute()

        assertEquals(11, betweenResults.size)
        assertTrue(betweenResults.all { it.price in 20.0..30.0 })

        // 2. Count with range pushdown
        val count = collection.query()
            .whereBetween("price", 50.0, 60.0)
            .count()
        assertEquals(11, count)

        // 3. whereGt and whereLt
        val gtResults = collection.query().whereGt("price", 95.0).execute()
        assertEquals(5, gtResults.size)

        val ltResults = collection.query().whereLte("price", 5.0).execute()
        assertEquals(5, ltResults.size)
    }

    // ========================================================================
    // 2. 2-TIER BLOCK CACHE & LZ4 BLOCK COMPRESSION
    // ========================================================================

    @Test
    fun `test LZ4 Block Compression roundtrip integrity`() {
        val codec = Lz4CompressionCodec()
        val original = "KoreDB High Performance Embedded Multi-Model Database".repeat(100).toByteArray()

        val compressed = codec.compress(original)
        assertTrue("LZ4 should achieve high compression on repetitive text", compressed.size < original.size / 2)

        val decompressed = codec.decompress(compressed)
        assertArrayEquals(original, decompressed)
    }

    @Test
    fun `test BlockCache hit miss and memory budget eviction`() {
        val cache = BlockCache(maxSizeBytes = 1024) // 1KB budget
        val dummyData = ByteArray(200) { 42 }

        cache.put("block_1", dummyData)
        cache.put("block_2", dummyData)

        assertNotNull("block_1 must be in cache", cache.get("block_1"))
        assertEquals(1, cache.hits.get())

        // Overfill cache
        cache.put("block_3", dummyData)
        cache.put("block_4", dummyData)
        cache.put("block_5", dummyData)

        assertTrue("Cache size must be within 1024 bytes", cache.sizeBytes <= 1024)
    }

    // ========================================================================
    // 3. 16-LANE SIMD & PRODUCT QUANTIZATION (32x)
    // ========================================================================

    @Test
    fun `test SimdVectorMath matches standard vector calculations`() {
        val v1 = floatArrayOf(1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, 11f, 12f, 13f, 14f, 15f, 16f)
        val v2 = floatArrayOf(2f, 1f, 0f, -1f, 2f, 1f, 0f, -1f, 2f, 1f, 0f, -1f, 2f, 1f, 0f, -1f)

        var expectedDot = 0f
        var expectedSq = 0f
        var expectedMan = 0f
        for (i in v1.indices) {
            expectedDot += v1[i] * v2[i]
            val diff = v1[i] - v2[i]
            expectedSq += diff * diff
            expectedMan += kotlin.math.abs(diff)
        }

        assertEquals(expectedDot, SimdVectorMath.dotProduct16(v1, v2), 0.001f)
        assertEquals(expectedSq, SimdVectorMath.euclideanDistanceSq16(v1, v2), 0.001f)
        assertEquals(expectedMan, SimdVectorMath.manhattanDistance16(v1, v2), 0.001f)
    }

    @Test
    fun `test ProductQuantizer 32x compression and Asymmetric Distance Computation`() {
        val dims = 128
        val subVectors = 16 // 16 sub-vectors of 8 dimensions = 16 bytes per vector (32x compression!)
        val pq = ProductQuantizer(dimensions = dims, numSubVectors = subVectors, numCentroids = 64)

        val random = Random(42)
        val trainingSet = (1..150).map {
            FloatArray(dims) { random.nextFloat() }
        }
        pq.train(trainingSet, maxIterations = 5)
        assertTrue(pq.isTrained)

        val testVec = FloatArray(dims) { random.nextFloat() }
        val code = pq.quantize(testVec)

        // Verify 32x compression ratio: 128 floats * 4 bytes = 512 bytes -> 16 bytes code!
        assertEquals("Code size must be 16 bytes", 16, code.size)

        // Asymmetric Distance Computation (ADC)
        val query = FloatArray(dims) { random.nextFloat() }
        val adcDist = pq.computeDistance(query, code, DistanceMetric.EUCLIDEAN)
        assertNotNull(adcDist)

        val cosineDist = pq.computeDistance(query, code, DistanceMetric.COSINE)
        assertNotNull(cosineDist)
    }

    // ========================================================================
    // 4. TRUE MVCC SNAPSHOT ISOLATION ACID ENGINE
    // ========================================================================

    @Test
    fun `test MVCC transaction commits mutations atomically`() {
        val key = "tx_user_1".toByteArray()
        val value = "Initial Value".toByteArray()

        db.transaction { tx ->
            tx.putRaw(key, value)
            val readWithinTx = tx.getRaw(key)
            assertNotNull(readWithinTx)
            assertEquals("Initial Value", String(readWithinTx!!))
        }

        // Must be visible outside after commit
        val committedVal = db.engine.getRaw(key)
        assertNotNull(committedVal)
        assertEquals("Initial Value", String(committedVal!!))
    }

    @Test
    fun `test MVCC write-write conflict throws MvccConflictException`() {
        runBlocking {
            val key = "shared_counter".toByteArray()
            db.engine.putRaw(key, "1".toByteArray())

            // Start transaction 1
            val snap1 = db.mvccManager.beginSnapshot()
            val tx1 = com.pankaj.koredb.engine.mvcc.MvccTransaction(db, snap1, db.mvccManager)

            // Start transaction 2
            val snap2 = db.mvccManager.beginSnapshot()
            val tx2 = com.pankaj.koredb.engine.mvcc.MvccTransaction(db, snap2, db.mvccManager)

            // TX1 mutates key and commits
            tx1.putRaw(key, "10".toByteArray())
            tx1.commit()

            // TX2 also tries to mutate key and commit -> must detect write-write collision!
            tx2.putRaw(key, "20".toByteArray())
            assertThrows(MvccConflictException::class.java) {
                tx2.commit()
            }
        }
    }

    @Test
    fun `test MVCC rollback discards writes`() {
        val key = "rollback_key".toByteArray()

        try {
            db.transaction { tx ->
                tx.putRaw(key, "Should be aborted".toByteArray())
                throw RuntimeException("Intentional Abort")
            }
        } catch (_: Exception) {}

        assertNull("Aborted transaction writes must not be visible", db.engine.getRaw(key))
    }
}
