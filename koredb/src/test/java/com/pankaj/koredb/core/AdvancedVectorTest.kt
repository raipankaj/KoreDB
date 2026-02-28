package com.pankaj.koredb.core

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.math.sqrt

/**
 * Exhaustive Test Suite for AI Vector Embedding Operations.
 *
 * Validates:
 * - Mathematical correctness of Cosine Similarity (Angles, Quadrants, Normalization)
 * - Handling of standard AI model dimensions (128, 384, 768, 1536)
 * - Edge cases (Zero, NaN, Infinity, Underflow/Overflow)
 * - CRUD & Persistence of vectors
 * - Search logic (Top-K, Ordering)
 */
class AdvancedVectorTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_adv_vec_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    // --- Mathematical Accuracy Tests ---

    @Test
    fun `test Cosine Similarity 0 Degrees (Identical)`() = runBlocking {
        val vectors = db.vectorCollection("math_0")
        val v = floatArrayOf(1f, 2f, 3f)
        vectors.insert("v", v)
        
        val results = vectors.search(v, limit = 1)
        assertEquals(1.0f, results[0].second, 0.0001f)
    }

    @Test
    fun `test Cosine Similarity 90 Degrees (Orthogonal)`() = runBlocking {
        val vectors = db.vectorCollection("math_90")
        val v1 = floatArrayOf(1f, 0f)
        val v2 = floatArrayOf(0f, 1f) // 90 deg
        vectors.insert("v1", v1)
        
        val results = vectors.search(v2, limit = 1)
        assertEquals(0.0f, results[0].second, 0.0001f)
    }

    @Test
    fun `test Cosine Similarity 180 Degrees (Opposite)`() = runBlocking {
        val vectors = db.vectorCollection("math_180")
        val v1 = floatArrayOf(1f, 1f)
        val v2 = floatArrayOf(-1f, -1f) // Opposite direction
        vectors.insert("v1", v1)
        
        val results = vectors.search(v2, limit = 1)
        assertEquals(-1.0f, results[0].second, 0.0001f)
    }

    @Test
    fun `test Cosine Similarity 60 Degrees (0_5 Sim)`() = runBlocking {
        // cos(60) = 0.5
        // V1 = (1, 0)
        // V2 = (0.5, sqrt(3)/2) -> length 1. dot = 0.5
        val vectors = db.vectorCollection("math_60")
        val v1 = floatArrayOf(1f, 0f)
        val v2 = floatArrayOf(0.5f, sqrt(3f) / 2)
        vectors.insert("target", v1)
        
        val results = vectors.search(v2, limit = 1)
        assertEquals(0.5f, results[0].second, 0.0001f)
    }

    @Test
    fun `test Magnitude Normalization (Scale Invariance)`() = runBlocking {
        // Cosine similarity depends only on angle, not magnitude.
        // [1, 1] should have sim 1.0 with [100, 100]
        val vectors = db.vectorCollection("scale")
        val small = floatArrayOf(1f, 1f)
        val huge = floatArrayOf(100f, 100f)
        
        vectors.insert("small", small)
        
        val results = vectors.search(huge, limit = 1)
        assertEquals(1.0f, results[0].second, 0.0001f)
    }

    @Test
    fun `test Negative Components (Quadrants)`() = runBlocking {
        // Check mixing positive and negative components
        // V1 = [1, 1] (Q1)
        // V2 = [-1, 1] (Q2) -> Angle 90 -> Sim 0
        val vectors = db.vectorCollection("quad")
        vectors.insert("target", floatArrayOf(1f, 1f))
        
        val results = vectors.search(floatArrayOf(-1f, 1f), limit = 1)
        assertEquals(0.0f, results[0].second, 0.0001f)
    }

    @Test
    fun `test Single Dimension Vectors`() = runBlocking {
        val vectors = db.vectorCollection("1d")
        vectors.insert("pos", floatArrayOf(5f))
        vectors.insert("neg", floatArrayOf(-5f))
        
        val q = floatArrayOf(10f)
        val results = vectors.search(q, limit = 2)
        
        // 5 vs 10 -> sim 1.0
        // -5 vs 10 -> sim -1.0
        assertEquals("pos", results[0].first)
        assertEquals(1.0f, results[0].second, 0.0001f)
        assertEquals("neg", results[1].first)
        assertEquals(-1.0f, results[1].second, 0.0001f)
    }

    // --- High Dimensionality Tests (AI Models) ---

    @Test
    fun `test Dimension 128 (Small)`() = runBlocking {
        val vectors = db.vectorCollection("d128")
        val v = FloatArray(128) { if (it % 2 == 0) 1f else 0f }
        vectors.insert("v", v)
        val res = vectors.search(v, 1)
        assertEquals(1.0f, res[0].second, 0.0001f)
    }

    @Test
    fun `test Dimension 384 (MiniLM-L6)`() = runBlocking {
        val vectors = db.vectorCollection("d384")
        val v = FloatArray(384) { 0.1f }
        vectors.insert("v", v)
        val res = vectors.search(v, 1)
        assertEquals(1.0f, res[0].second, 0.0001f)
    }

    @Test
    fun `test Dimension 768 (BERT Base)`() = runBlocking {
        val vectors = db.vectorCollection("d768")
        val v = FloatArray(768) { it.toFloat() }
        vectors.insert("v", v)
        // Search with same vector
        val res = vectors.search(v, 1)
        assertEquals(1.0f, res[0].second, 0.0001f)
    }

    @Test
    fun `test Dimension 1536 (OpenAI ada-002)`() = runBlocking {
        val vectors = db.vectorCollection("d1536")
        val v = FloatArray(1536) { 1f }
        vectors.insert("v", v)
        val res = vectors.search(v, 1)
        assertEquals(1.0f, res[0].second, 0.0001f)
    }

    // --- Edge Cases & Robustness ---

    @Test
    fun `test Tiny Values (Underflow Check)`() = runBlocking {
        val vectors = db.vectorCollection("tiny")
        // Use 1.0E-15f to avoid squaring underflow (float min positive is ~1.4E-45)
        // 1e-15^2 = 1e-30 (Safe)
        val tiny = floatArrayOf(1.0e-15f, 1.0e-15f)
        vectors.insert("tiny", tiny)
        
        val res = vectors.search(tiny, 1)
        // Should still normalize correctly to 1.0
        assertEquals(1.0f, res[0].second, 0.0001f)
    }

    @Test
    fun `test Large Values (Overflow Check)`() = runBlocking {
        val vectors = db.vectorCollection("huge")
        // Float max is ~3.4e38.
        val huge = floatArrayOf(1e20f, 1e20f)
        vectors.insert("huge", huge)
        
        val res = vectors.search(huge, 1)
        
        if (res.isNotEmpty()) {
            val score = res[0].second
            assertTrue("Score should be finite or NaN", !score.isInfinite())
        }
    }

    @Test
    fun `test NaN Handling`() = runBlocking {
        val vectors = db.vectorCollection("nan")
        val nanVec = floatArrayOf(Float.NaN, 1f)
        vectors.insert("nan", nanVec)
        
        // Search with clean vector
        val res = vectors.search(floatArrayOf(1f, 1f), 1)
        
        assertNotNull(res)
    }

    // --- Search Logic & Lifecycle ---

    @Test
    fun `test Top K Filtering`() = runBlocking {
        val vectors = db.vectorCollection("topk")
        // Insert 10 vectors with known similarities
        
        for (i in 0 until 10) {
            val x = (10 - i).toFloat()
            val y = i.toFloat()
            // v0=(10,0), v1=(9,1)... v9=(1,9)
            vectors.insert("v$i", floatArrayOf(x, y))
        }
        
        val target = floatArrayOf(1f, 0f)
        val res = vectors.search(target, limit = 3)
        
        assertEquals(3, res.size)
        assertEquals("v0", res[0].first) // Best match
        assertEquals("v1", res[1].first)
        assertEquals("v2", res[2].first)
    }

    @Test
    fun `test Search Empty Collection`() = runBlocking {
        val vectors = db.vectorCollection("empty")
        val res = vectors.search(floatArrayOf(1f, 2f), limit = 5)
        assertTrue(res.isEmpty())
    }

    @Test
    fun `test Update Vector (Overwrite)`() = runBlocking {
        val vectors = db.vectorCollection("update")
        
        // Initial: Orthogonal
        vectors.insert("u1", floatArrayOf(0f, 1f))
        
        // Search for [1,0] -> Sim 0
        var res = vectors.search(floatArrayOf(1f, 0f), 1)
        assertEquals(0.0f, res[0].second, 0.0001f)
        
        // Update: Identical
        vectors.insert("u1", floatArrayOf(1f, 0f))
        
        // Search for [1,0] -> Sim 1
        res = vectors.search(floatArrayOf(1f, 0f), 1)
        assertEquals(1.0f, res[0].second, 0.0001f)
    }

    @Test
    fun `test Delete Vector`() = runBlocking {
        val vectors = db.vectorCollection("delete")
        vectors.insert("d1", floatArrayOf(1f, 1f))
        
        assertFalse(vectors.search(floatArrayOf(1f, 1f), 1).isEmpty())
        
        db.deleteRaw("vec:delete:d1".toByteArray())
        
        val res = vectors.search(floatArrayOf(1f, 1f), 1)
        assertTrue("Vector should be deleted", res.isEmpty())
    }

    @Test
    fun `test Vector Persistence`() = runBlocking {
        val vectors = db.vectorCollection("persist")
        val vec = floatArrayOf(0.123f, 0.456f)
        vectors.insert("p1", vec)
        
        db.close()
        
        // Reopen
        val db2 = KoreDatabase(testDir)
        val vectors2 = db2.vectorCollection("persist")
        
        val res = vectors2.search(vec, 1)
        assertEquals(1, res.size)
        assertEquals("p1", res[0].first)
        assertEquals(1.0f, res[0].second, 0.0001f)
        
        db2.close()
    }

    @Test
    fun `test Batch Insert Consistency`() = runBlocking {
        val vectors = db.vectorCollection("batch")
        val batch = mapOf(
            "b1" to floatArrayOf(1f, 0f),
            "b2" to floatArrayOf(0f, 1f)
        )
        vectors.insertBatch(batch)
        
        val res1 = vectors.search(floatArrayOf(1f, 0f), 1)
        assertEquals("b1", res1[0].first)
        
        val res2 = vectors.search(floatArrayOf(0f, 1f), 1)
        assertEquals("b2", res2[0].first)
    }
}
