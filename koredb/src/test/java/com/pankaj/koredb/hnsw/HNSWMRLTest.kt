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

package com.pankaj.koredb.hnsw

import com.pankaj.koredb.core.SimdVectorMath
import com.pankaj.koredb.core.VectorMath
import org.junit.Assert.*
import org.junit.Test
import java.util.Random
import kotlin.math.sqrt

class HNSWMRLTest {

    private fun generateMRLVector(dim: Int, rng: Random): FloatArray {
        // MRL vectors concentrate higher energy/variance in the leading dimensions
        val vec = FloatArray(dim) { idx ->
            val scale = 1.0f / (1.0f + idx * 0.05f)
            rng.nextGaussian().toFloat() * scale
        }
        var norm = 0f
        for (v in vec) norm += v * v
        norm = sqrt(norm.coerceAtLeast(1e-12f))
        for (i in 0 until dim) vec[i] /= norm
        return vec
    }

    @Test
    fun testMagnitude64CachedOnHNSWNode() {
        val dim = 128
        val rng = Random(42L)
        val vec = generateMRLVector(dim, rng)
        val expectedMag64 = SimdVectorMath.getMagnitude16(vec, 64)

        val fullMag = VectorMath.getMagnitude(vec)
        val node = HNSWIndex.HNSWNode("test_node", vec, fullMag, 2)

        assertEquals("Cached magnitude64 must match 64-dim prefix magnitude",
            expectedMag64, node.magnitude64, 1e-5f)
        assertTrue("magnitude64 must be strictly positive", node.magnitude64 > 0f)
    }

    @Test
    fun testUpperLayerShortCircuitingPreservesRecall() {
        val dim = 128
        val numVectors = 200
        val rng = Random(12345L)

        val index = HNSWIndex(
            dimensions = dim,
            metric = DistanceMetric.COSINE,
            m = 16,
            efConstruction = 64,
            efSearch = 40
        )

        val data = ArrayList<Pair<String, FloatArray>>(numVectors)
        for (i in 0 until numVectors) {
            val id = "vec_$i"
            val vec = generateMRLVector(dim, rng)
            data.add(id to vec)
            index.insert(id, vec)
        }

        assertEquals(numVectors, index.size)

        // Evaluate recall across 20 test queries
        var totalMatches = 0
        val topK = 5
        val numQueries = 20

        for (q in 0 until numQueries) {
            val query = generateMRLVector(dim, Random(q.toLong() * 997))

            // 1. MRL short-circuit search (evaluating first 64 dimensions in upper layers)
            val mrlResults = index.search(query, limit = topK, mrlCoarseDims = 64)
            val mrlIds = mrlResults.map { it.first }.toSet()

            // 2. Full dimensional upper-layer search (mrlCoarseDims = dim disables short-circuiting)
            val fullResults = index.search(query, limit = topK, mrlCoarseDims = dim)
            val fullIds = fullResults.map { it.first }.toSet()

            val overlap = mrlIds.intersect(fullIds).size
            totalMatches += overlap
        }

        val totalExpected = topK * numQueries
        val recall = totalMatches.toDouble() / totalExpected
        assertTrue("MRL short-circuit search recall against full search must be >= 90%, was $recall",
            recall >= 0.90)
    }

    @Test
    fun testLayer0RescoreGuaranteesExactSimilarity() {
        val dim = 256
        val rng = Random(999L)
        val index = HNSWIndex(dimensions = dim, metric = DistanceMetric.COSINE)

        val vectors = HashMap<String, FloatArray>()
        for (i in 0 until 50) {
            val id = "doc_$i"
            val vec = generateMRLVector(dim, rng)
            vectors[id] = vec
            index.insert(id, vec)
        }

        val query = generateMRLVector(dim, Random(555L))
        val results = index.search(query, limit = 5, mrlCoarseDims = 64)

        assertEquals(5, results.size)
        for (res in results) {
            val originalVec = vectors[res.id]!!
            val exactSim = DistanceMetric.COSINE.compute(query, originalVec)
            assertEquals("Reported score must equal full 256-dim exact similarity",
                exactSim, res.distance, 1e-4f)
        }
    }

    @Test
    fun testSmallDimensionBoundaryCondition() {
        // When dimensions <= 64, MRL short-circuiting should seamlessly evaluate all available dimensions
        val dim = 32
        val rng = Random(777L)
        val index = HNSWIndex(dimensions = dim, metric = DistanceMetric.COSINE)

        for (i in 0 until 30) {
            val vec = generateMRLVector(dim, rng)
            index.insert("item_$i", vec)
        }

        val query = generateMRLVector(dim, Random(888L))
        // mrlCoarseDims = 64 on a 32-dim index
        val results = index.search(query, limit = 5, mrlCoarseDims = 64)

        assertEquals(5, results.size)
        // Verify results are monotonic descending by similarity
        for (i in 0 until results.size - 1) {
            assertTrue("Results must be sorted descending",
                results[i].distance >= results[i + 1].distance - 1e-5f)
        }
    }

    @Test
    fun testMatryoshkaTwoStageSearchMethod() {
        val dim = 128
        val rng = Random(333L)
        val index = HNSWIndex(dimensions = dim, metric = DistanceMetric.COSINE)

        for (i in 0 until 40) {
            val vec = generateMRLVector(dim, rng)
            index.insert("id_$i", vec)
        }

        val query = generateMRLVector(dim, Random(444L))
        val results = index.searchMatryoshka(
            query = query,
            limit = 5,
            coarseDims = 64,
            oversampleFactor = 3
        )

        assertEquals(5, results.size)
        for (i in 0 until results.size - 1) {
            assertTrue(results[i].second >= results[i + 1].second - 1e-5f)
        }
    }
}
