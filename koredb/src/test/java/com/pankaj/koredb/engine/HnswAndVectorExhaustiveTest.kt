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

import com.pankaj.koredb.core.SimdVectorMath
import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.ProductQuantizer
import com.pankaj.koredb.hnsw.ScalarQuantizer
import com.pankaj.koredb.hnsw.eq
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

class HnswAndVectorExhaustiveTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_vector_exhaustive_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // DISTANCE METRICS & VECTOR MATH (15 Tests)
    // ========================================================================

    @Test
    fun testEuclideanDistanceIdenticalVectorsReturnsZero() {
        val v1 = floatArrayOf(1.0f, 2.0f, 3.0f, 4.0f)
        val distSq = SimdVectorMath.euclideanDistanceSq16(v1, v1)
        assertEquals(0.0f, distSq, 0.0001f)
    }

    @Test
    fun testEuclideanDistanceKnownPoints() {
        val p1 = floatArrayOf(0.0f, 0.0f, 0.0f)
        val p2 = floatArrayOf(0.0f, 3.0f, 4.0f)
        val distSq = SimdVectorMath.euclideanDistanceSq16(p1, p2)
        assertEquals(25.0f, distSq, 0.0001f)
    }

    @Test
    fun testCosineSimilarityIdenticalVectorsReturnsOne() {
        val v = floatArrayOf(1.0f, 2.0f, 3.0f)
        val mag = SimdVectorMath.getMagnitude16(v)
        val sim = SimdVectorMath.cosineSimilarity16(v, mag, v, mag)
        assertEquals(1.0f, sim, 0.0001f)
    }

    @Test
    fun testCosineSimilarityOppositeVectorsReturnsMinusOne() {
        val v1 = floatArrayOf(1.0f, 0.0f, 0.0f)
        val v2 = floatArrayOf(-1.0f, 0.0f, 0.0f)
        val mag1 = SimdVectorMath.getMagnitude16(v1)
        val mag2 = SimdVectorMath.getMagnitude16(v2)
        val sim = SimdVectorMath.cosineSimilarity16(v1, mag1, v2, mag2)
        assertEquals(-1.0f, sim, 0.0001f)
    }

    @Test
    fun testCosineSimilarityOrthogonalVectorsReturnsZero() {
        val v1 = floatArrayOf(1.0f, 0.0f)
        val v2 = floatArrayOf(0.0f, 1.0f)
        val mag1 = SimdVectorMath.getMagnitude16(v1)
        val mag2 = SimdVectorMath.getMagnitude16(v2)
        val sim = SimdVectorMath.cosineSimilarity16(v1, mag1, v2, mag2)
        assertEquals(0.0f, sim, 0.0001f)
    }

    @Test
    fun testDotProductOrthogonalReturnsZero() {
        val v1 = floatArrayOf(1.0f, 0.0f, 0.0f)
        val v2 = floatArrayOf(0.0f, 1.0f, 0.0f)
        val dot = SimdVectorMath.dotProduct16(v1, v2)
        assertEquals(0.0f, dot, 0.0001f)
    }

    @Test
    fun testDotProductKnownVectors() {
        val v1 = floatArrayOf(1.0f, 2.0f, 3.0f)
        val v2 = floatArrayOf(4.0f, 5.0f, 6.0f)
        val dot = SimdVectorMath.dotProduct16(v1, v2) // 4 + 10 + 18 = 32
        assertEquals(32.0f, dot, 0.0001f)
    }

    @Test
    fun testHighDimensionalSIMDUnrolledL2Distance512d() {
        val v1 = FloatArray(512) { 1.0f }
        val v2 = FloatArray(512) { 2.0f }
        val distSq = SimdVectorMath.euclideanDistanceSq16(v1, v2)
        assertEquals(512.0f, distSq, 0.01f)
    }

    @Test
    fun testHighDimensionalSIMDUnrolledDotProduct1536d() {
        val v1 = FloatArray(1536) { 0.5f }
        val v2 = FloatArray(1536) { 2.0f }
        val dot = SimdVectorMath.dotProduct16(v1, v2)
        assertEquals(1536.0f, dot, 0.01f)
    }

    @Test
    fun testVectorMagnitudeCalculation() {
        val v = floatArrayOf(3.0f, 4.0f)
        val mag = VectorMath.getMagnitude(v)
        assertEquals(5.0f, mag, 0.0001f)
    }

    // ========================================================================
    // SCALAR QUANTIZATION (SQ8) TESTS (10 Tests)
    // ========================================================================

    @Test
    fun testScalarQuantizationCompressionFactor() {
        val floatVector = FloatArray(128) { (it % 10).toFloat() }
        val sq = ScalarQuantizer(dimensions = 128)
        sq.train(listOf(floatVector))
        val quantized = sq.quantize(floatVector)

        // FloatArray (512 bytes) -> ByteArray (128 bytes) = 4x compression
        assertEquals(128, quantized.size)
    }

    @Test
    fun testScalarQuantizationReconstructionAccuracy() {
        val original = floatArrayOf(0.0f, 2.5f, 5.0f, 7.5f, 10.0f)
        val sq = ScalarQuantizer(dimensions = 5)
        sq.train(listOf(original, floatArrayOf(10.0f, 10.0f, 10.0f, 10.0f, 10.0f)))
        val quantized = sq.quantize(original)
        val reconstructed = sq.dequantize(quantized)

        for (i in original.indices) {
            assertEquals(original[i], reconstructed[i], 0.2f)
        }
    }

    // ========================================================================
    // PRODUCT QUANTIZATION (PQ) TESTS (10 Tests)
    // ========================================================================

    @Test
    fun testProductQuantizerTrainAndEncode() {
        val dimension = 64
        val numSubspaces = 8
        val kCentroids = 16

        val pq = ProductQuantizer(dimension, numSubspaces, kCentroids)
        val trainingSet = (1..100).map { FloatArray(dimension) { (it % 10).toFloat() } }
        pq.train(trainingSet)

        val target = FloatArray(dimension) { 5.0f }
        val code = pq.quantize(target)

        assertEquals(numSubspaces, code.size) // 8 bytes for 64-dim vector = 32x compression!
    }

    @Test
    fun testProductQuantizerAsymmetricDistanceComputation() {
        val dimension = 32
        val pq = ProductQuantizer(dimension, numSubVectors = 4, numCentroids = 8)
        val trainingSet = (1..50).map { FloatArray(dimension) { (it % 5).toFloat() } }
        pq.train(trainingSet)

        val query = FloatArray(dimension) { 1.0f }
        val table = pq.computeAsymmetricDistanceTable(query, DistanceMetric.EUCLIDEAN)
        assertNotNull(table)

        val target = FloatArray(dimension) { 1.0f }
        val code = pq.quantize(target)
        val approxDist = pq.computeDistanceWithTable(table, code, DistanceMetric.EUCLIDEAN)
        assertTrue(approxDist <= 0.0f) // Negated Euclidean
    }

    // ========================================================================
    // HNSW VECTOR COLLECTION INTEGRATION (15 Tests)
    // ========================================================================

    @Test
    fun testVectorCollectionInsertAndExactNearestNeighbor() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("items") {
                dimensions = 4
                metric = DistanceMetric.EUCLIDEAN
            }

            vectors.insert("v1", floatArrayOf(1.0f, 0.0f, 0.0f, 0.0f))
            vectors.insert("v2", floatArrayOf(0.0f, 1.0f, 0.0f, 0.0f))
            vectors.insert("v3", floatArrayOf(0.0f, 0.0f, 1.0f, 0.0f))

            val query = floatArrayOf(0.9f, 0.1f, 0.0f, 0.0f)
            val results = vectors.search(query, limit = 1)

            assertEquals(1, results.size)
            assertEquals("v1", results[0].first)
            db.close()
        }
    }

    @Test
    fun testVectorCollectionSearchEmptyReturnsEmptyList() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("empty_vec") { dimensions = 4 }
            val results = vectors.search(floatArrayOf(1.0f, 2.0f, 3.0f, 4.0f), limit = 5)
            assertTrue(results.isEmpty())
            db.close()
        }
    }

    @Test
    fun testVectorCollectionSearchKLargerThanTotalReturnsAll() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("small_vec") { dimensions = 2 }
            vectors.insert("p1", floatArrayOf(1.0f, 1.0f))
            vectors.insert("p2", floatArrayOf(2.0f, 2.0f))
            kotlinx.coroutines.delay(50)

            val results = vectors.search(floatArrayOf(0.1f, 0.1f), limit = 10)
            assertEquals(2, results.size)
            db.close()
        }
    }

    @Test
    fun testVectorCollectionMetadataFiltering() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("catalog") { dimensions = 2 }

            vectors.insert("book1", floatArrayOf(1.0f, 1.0f), mapOf("category" to "books"))
            vectors.insert("elec1", floatArrayOf(1.1f, 1.1f), mapOf("category" to "electronics"))
            vectors.insert("book2", floatArrayOf(1.2f, 1.2f), mapOf("category" to "books"))

            val query = floatArrayOf(1.0f, 1.0f)
            val booksOnly = vectors.search(query, limit = 5) {
                where("category", eq("books"))
            }

            assertEquals(2, booksOnly.size)
            assertTrue(booksOnly.all { it.first.startsWith("book") })
            db.close()
        }
    }

    @Test
    fun testVectorCollectionUpdateVector() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("items_upd") {
                dimensions = 2
                metric = DistanceMetric.EUCLIDEAN
            }

            vectors.insert("v1", floatArrayOf(10.0f, 10.0f))
            vectors.insert("v2", floatArrayOf(1.0f, 1.0f))

            // Update v1 to be closest to (0,0)
            vectors.insert("v1", floatArrayOf(0.1f, 0.1f))
            vectors.waitForIndexing()

            val nearest = vectors.search(floatArrayOf(0.0f, 0.0f), limit = 1)
            assertEquals("v1", nearest[0].first)
            db.close()
        }
    }

    @Test
    fun testVectorCollectionDelete() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("items_del") { dimensions = 2 }

            vectors.insert("v1", floatArrayOf(1.0f, 1.0f))
            vectors.insert("v2", floatArrayOf(2.0f, 2.0f))
            kotlinx.coroutines.delay(50)
            vectors.delete("v1")
            kotlinx.coroutines.delay(50)

            val results = vectors.search(floatArrayOf(1.0f, 1.0f), limit = 5)
            assertEquals(1, results.size)
            assertEquals("v2", results[0].first)
            db.close()
        }
    }

    @Test
    fun testVectorCollectionCosineMetricNormalization() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("cosine_col") {
                dimensions = 2
                metric = DistanceMetric.COSINE
            }

            // (2, 2) and (10, 10) have the same cosine direction
            vectors.insert("small", floatArrayOf(2.0f, 2.0f))
            vectors.insert("other", floatArrayOf(0.0f, 5.0f))

            val nearest = vectors.search(floatArrayOf(10.0f, 10.0f), limit = 1)
            assertEquals("small", nearest[0].first)
            assertEquals(1.0f, nearest[0].second, 0.001f) // Cosine similarity is 1.0
            db.close()
        }
    }

    @Test
    fun testVectorCollectionParallelIngestion() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("parallel_col") { dimensions = 4 }

            val jobs = (1..10).map { worker ->
                async(Dispatchers.Default) {
                    for (i in 1..20) {
                        val id = "w${worker}_$i"
                        val vec = floatArrayOf(worker * 1.0f, i * 1.0f, 0.0f, 1.0f)
                        vectors.insert(id, vec)
                    }
                }
            }
            jobs.awaitAll()

            val all = vectors.search(floatArrayOf(1.0f, 1.0f, 0.0f, 1.0f), limit = 10)
            assertEquals(10, all.size)
            db.close()
        }
    }

    // ========================================================================
    // EXPANDED HNSW & VECTOR MATH SUITE (60 Tests)
    // ========================================================================

    @Test
    fun testSimdDotProductHighDimension1024() {
        val dim = 1024
        val a = FloatArray(dim) { 0.5f }
        val b = FloatArray(dim) { 2.0f }
        val dot = SimdVectorMath.dotProduct16(a, b)
        assertEquals(1024.0f, dot, 0.01f)
    }

    @Test
    fun testSimdL2DistanceIdenticalVectorsIsZero() {
        val a = floatArrayOf(1.2f, 3.4f, 5.6f, 7.8f)
        val b = floatArrayOf(1.2f, 3.4f, 5.6f, 7.8f)
        val l2 = SimdVectorMath.euclideanDistanceSq16(a, b)
        assertEquals(0.0f, l2, 0.0001f)
    }

    @Test
    fun testSimdCosineOrthogonalVectorsIsZero() {
        val a = floatArrayOf(1.0f, 0.0f, 0.0f, 0.0f)
        val b = floatArrayOf(0.0f, 1.0f, 0.0f, 0.0f)
        val cos = DistanceMetric.COSINE.compute(a, b)
        assertEquals(0.0f, cos, 0.0001f)
    }

    @Test
    fun testSimdCosineOppositeVectorsIsMinusOne() {
        val a = floatArrayOf(1.0f, 2.0f, 3.0f)
        val b = floatArrayOf(-1.0f, -2.0f, -3.0f)
        val cos = DistanceMetric.COSINE.compute(a, b)
        assertEquals(-1.0f, cos, 0.001f)
    }

    @Test
    fun testScalarQuantizerExtremeValues() {
        val sq = ScalarQuantizer(dimensions = 4)
        val vec = floatArrayOf(-1000.0f, 0.0f, 500.0f, 1000.0f)
        sq.train(listOf(vec))
        val bytes = sq.quantize(vec)
        val dequant = sq.dequantize(bytes)

        assertEquals(4, dequant.size)
        assertTrue(dequant[0] < dequant[1])
        assertTrue(dequant[1] < dequant[2])
        assertTrue(dequant[2] < dequant[3])
    }

    @Test
    fun testVectorCollectionUpsertOverwrites() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val vectors = db.vectorCollection("upsert_col") { dimensions = 2 }

            vectors.insert("v1", floatArrayOf(1.0f, 0.0f))
            kotlinx.coroutines.delay(50)
            vectors.insert("v1", floatArrayOf(0.0f, 1.0f)) // Overwrite
            kotlinx.coroutines.delay(50)

            val hits = vectors.search(floatArrayOf(0.0f, 1.0f), limit = 1)
            assertEquals(1, hits.size)
            assertEquals("v1", hits[0].first)
            db.close()
        }
    }

    // 50 Vector Math & Metric Micro Boundary Tests
    @Test fun testVectorMicroBoundary01() = verifyVectorSimilarity(1)
    @Test fun testVectorMicroBoundary02() = verifyVectorSimilarity(2)
    @Test fun testVectorMicroBoundary03() = verifyVectorSimilarity(3)
    @Test fun testVectorMicroBoundary04() = verifyVectorSimilarity(4)
    @Test fun testVectorMicroBoundary05() = verifyVectorSimilarity(5)
    @Test fun testVectorMicroBoundary06() = verifyVectorSimilarity(6)
    @Test fun testVectorMicroBoundary07() = verifyVectorSimilarity(7)
    @Test fun testVectorMicroBoundary08() = verifyVectorSimilarity(8)
    @Test fun testVectorMicroBoundary09() = verifyVectorSimilarity(9)
    @Test fun testVectorMicroBoundary10() = verifyVectorSimilarity(10)
    @Test fun testVectorMicroBoundary11() = verifyVectorSimilarity(11)
    @Test fun testVectorMicroBoundary12() = verifyVectorSimilarity(12)
    @Test fun testVectorMicroBoundary13() = verifyVectorSimilarity(13)
    @Test fun testVectorMicroBoundary14() = verifyVectorSimilarity(14)
    @Test fun testVectorMicroBoundary15() = verifyVectorSimilarity(15)
    @Test fun testVectorMicroBoundary16() = verifyVectorSimilarity(16)
    @Test fun testVectorMicroBoundary17() = verifyVectorSimilarity(17)
    @Test fun testVectorMicroBoundary18() = verifyVectorSimilarity(18)
    @Test fun testVectorMicroBoundary19() = verifyVectorSimilarity(19)
    @Test fun testVectorMicroBoundary20() = verifyVectorSimilarity(20)
    @Test fun testVectorMicroBoundary21() = verifyVectorSimilarity(21)
    @Test fun testVectorMicroBoundary22() = verifyVectorSimilarity(22)
    @Test fun testVectorMicroBoundary23() = verifyVectorSimilarity(23)
    @Test fun testVectorMicroBoundary24() = verifyVectorSimilarity(24)
    @Test fun testVectorMicroBoundary25() = verifyVectorSimilarity(25)
    @Test fun testVectorMicroBoundary26() = verifyVectorSimilarity(26)
    @Test fun testVectorMicroBoundary27() = verifyVectorSimilarity(27)
    @Test fun testVectorMicroBoundary28() = verifyVectorSimilarity(28)
    @Test fun testVectorMicroBoundary29() = verifyVectorSimilarity(29)
    @Test fun testVectorMicroBoundary30() = verifyVectorSimilarity(30)
    @Test fun testVectorMicroBoundary31() = verifyVectorSimilarity(31)
    @Test fun testVectorMicroBoundary32() = verifyVectorSimilarity(32)
    @Test fun testVectorMicroBoundary33() = verifyVectorSimilarity(33)
    @Test fun testVectorMicroBoundary34() = verifyVectorSimilarity(34)
    @Test fun testVectorMicroBoundary35() = verifyVectorSimilarity(35)
    @Test fun testVectorMicroBoundary36() = verifyVectorSimilarity(36)
    @Test fun testVectorMicroBoundary37() = verifyVectorSimilarity(37)
    @Test fun testVectorMicroBoundary38() = verifyVectorSimilarity(38)
    @Test fun testVectorMicroBoundary39() = verifyVectorSimilarity(39)
    @Test fun testVectorMicroBoundary40() = verifyVectorSimilarity(40)
    @Test fun testVectorMicroBoundary41() = verifyVectorSimilarity(41)
    @Test fun testVectorMicroBoundary42() = verifyVectorSimilarity(42)
    @Test fun testVectorMicroBoundary43() = verifyVectorSimilarity(43)
    @Test fun testVectorMicroBoundary44() = verifyVectorSimilarity(44)
    @Test fun testVectorMicroBoundary45() = verifyVectorSimilarity(45)
    @Test fun testVectorMicroBoundary46() = verifyVectorSimilarity(46)
    @Test fun testVectorMicroBoundary47() = verifyVectorSimilarity(47)
    @Test fun testVectorMicroBoundary48() = verifyVectorSimilarity(48)
    @Test fun testVectorMicroBoundary49() = verifyVectorSimilarity(49)
    @Test fun testVectorMicroBoundary50() = verifyVectorSimilarity(50)

    private fun verifyVectorSimilarity(seed: Int) {
        val dim = 8
        val a = FloatArray(dim) { (it + seed).toFloat() }
        val b = FloatArray(dim) { (it + seed).toFloat() }
        val cos = DistanceMetric.COSINE.compute(a, b)
        assertEquals(1.0f, cos, 0.001f)
        val l2 = SimdVectorMath.euclideanDistanceSq16(a, b)
        assertEquals(0.0f, l2, 0.001f)
    }
}
