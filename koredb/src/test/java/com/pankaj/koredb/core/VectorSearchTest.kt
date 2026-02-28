package com.pankaj.koredb.core

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Test Suite for the Vector Similarity Search Engine.
 *
 * Covers:
 * - Exact Match retrieval
 * - Cosine Similarity logic
 * - Dimensionality handling
 * - Zero Vector behavior
 */
class VectorSearchTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_vec_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Verifies that inserting a vector and searching for it exactly returns the vector itself with score ~1.0.
     */
    @Test
    fun `test Exact Vector Match`() = runBlocking {
        val vectors = db.vectorCollection("embeddings")
        val vec1 = floatArrayOf(1.0f, 0.0f, 0.0f)
        val vec2 = floatArrayOf(0.0f, 1.0f, 0.0f)

        vectors.insert("v1", vec1)
        vectors.insert("v2", vec2)

        // Searching for vec1 exactly should return v1 with score ~1.0
        val results = vectors.search(vec1, limit = 1)
        assertEquals(1, results.size)
        assertEquals("v1", results[0].first)
        assertEquals(1.0f, results[0].second, 0.0001f)
    }

    /**
     * Validates Cosine Similarity calculations for known vectors:
     * - Identical (1.0)
     * - Orthogonal (0.0)
     * - Opposite (-1.0)
     */
    @Test
    fun `test Cosine Similarity logic`() = runBlocking {
        val vectors = db.vectorCollection("items")

        // A and B are orthogonal (sim = 0)
        // A and C are identical (sim = 1)
        // A and D are opposite (sim = -1)
        val A = floatArrayOf(1f, 0f)
        val B = floatArrayOf(0f, 1f)
        val C = floatArrayOf(1f, 0f)
        val D = floatArrayOf(-1f, 0f)

        vectors.insert("A", A)
        vectors.insert("B", B)
        vectors.insert("C", C)
        vectors.insert("D", D)

        val results = vectors.search(A, limit = 4)

        // Validate top 2 are A and C (identical vectors, score 1.0)
        // Order between identical scores is undefined, so check set membership
        val top2Ids = results.take(2).map { it.first }.toSet()
        val top2Scores = results.take(2).map { it.second }

        assertTrue("Top 2 must contain A", top2Ids.contains("A"))
        assertTrue("Top 2 must contain C", top2Ids.contains("C"))
        assertEquals("First score mismatch", 1.0f, top2Scores[0], 0.001f)
        assertEquals("Second score mismatch", 1.0f, top2Scores[1], 0.001f)

        // Validate 3rd is B (Orthogonal, score 0.0)
        assertEquals("Third element mismatch", "B", results[2].first)
        assertEquals("Orthogonal score mismatch", 0.0f, results[2].second, 0.001f)

        // Validate 4th is D (Opposite, score -1.0)
        assertEquals("Fourth element mismatch", "D", results[3].first)
        assertEquals("Opposite score mismatch", -1.0f, results[3].second, 0.001f)
    }

    /**
     * EDGE CASE: Dimensionality Mismatch
     * Verifies that the engine handles vectors of different lengths gracefully.
     * With the updated implementation, mismatched vectors return a score of -2.0
     * and are filtered out from the results.
     */
    @Test
    fun `test Dimensionality Mismatch`() = runBlocking {
        val vectors = db.vectorCollection("dim_test")
        
        val v2d = floatArrayOf(1f, 0f)
        val v3d = floatArrayOf(1f, 0f, 1f) // Extra dimension

        vectors.insert("v2d", v2d)
        
        // Search with a 3D vector against a 2D collection
        // Should NOT crash. Should return empty list because dimensions don't match.
        try {
            val results = vectors.search(v3d, limit = 1)
            assertTrue("Results should be empty for mismatched dimensions", results.isEmpty())
        } catch (e: Exception) {
            fail("Should not crash on dimensionality mismatch: ${e.message}")
        }
    }

    /**
     * EDGE CASE: Zero Vector
     * Verifies behavior when a vector has zero magnitude (division by zero risk).
     */
    @Test
    fun `test Zero Vector Handling`() = runBlocking {
        val vectors = db.vectorCollection("zero_test")
        val zeroVec = floatArrayOf(0f, 0f)
        val normalVec = floatArrayOf(1f, 1f)

        vectors.insert("zero", zeroVec)
        vectors.insert("normal", normalVec)

        // Searching WITH a zero vector
        // Cosine sim is undefined for zero vector.
        // Implementation should return 0.0 or handle gracefully.
        val results = vectors.search(zeroVec, limit = 2)
        
        // Ensure no crash and reasonable return
        results.forEach { 
             // Scores should be 0.0 or NaN, usually 0.0 in safe implementations
             assertTrue("Score should be valid (finite)", !it.second.isInfinite())
        }
    }
}
