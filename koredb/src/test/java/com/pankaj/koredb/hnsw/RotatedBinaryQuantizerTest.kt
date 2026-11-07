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
import kotlin.math.abs
import kotlin.math.sqrt

class RotatedBinaryQuantizerTest {

    private fun generateRandomVector(dim: Int, rng: Random): FloatArray {
        val vec = FloatArray(dim) { rng.nextGaussian().toFloat() }
        var norm = 0f
        for (v in vec) norm += v * v
        norm = sqrt(norm.coerceAtLeast(1e-12f))
        for (i in 0 until dim) vec[i] /= norm
        return vec
    }

    @Test
    fun testOrthogonalMatrixProperties() {
        val dim = 64
        val seed = 42L
        val r = BinaryQuantizer.generateOrthogonalMatrix(dim, seed)

        assertEquals(dim, r.size)
        assertEquals(dim, r[0].size)

        // 1. Verify each row has unit norm: ||R_i|| = 1.0
        for (i in 0 until dim) {
            val norm = SimdVectorMath.getMagnitude16(r[i], dim)
            assertEquals("Row $i must have unit norm", 1.0f, norm, 1e-4f)
        }

        // 2. Verify rows are mutually orthogonal: R_i · R_j = 0 for i != j
        for (i in 0 until minOf(dim, 10)) {
            for (j in 0 until minOf(dim, 10)) {
                val dot = SimdVectorMath.dotProduct16(r[i], r[j], dim)
                val expected = if (i == j) 1.0f else 0.0f
                assertEquals("Dot product of row $i and $j", expected, dot, 1e-3f)
            }
        }
    }

    @Test
    fun testOrthogonalRotationPreservesNormAndInnerProduct() {
        val dim = 128
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true, randomSeed = 100L)
        val rng = Random(2026L)

        val x = generateRandomVector(dim, rng)
        val y = generateRandomVector(dim, rng)

        val origNormX = VectorMath.getMagnitude(x)
        val origNormY = VectorMath.getMagnitude(y)
        val origDot = SimdVectorMath.dotProduct16(x, y, dim)

        val rotX = bq.rotate(x)
        val rotY = bq.rotate(y)

        val rotNormX = VectorMath.getMagnitude(rotX)
        val rotNormY = VectorMath.getMagnitude(rotY)
        val rotDot = SimdVectorMath.dotProduct16(rotX, rotY, dim)

        assertEquals("Rotation must preserve norm of x", origNormX, rotNormX, 1e-4f)
        assertEquals("Rotation must preserve norm of y", origNormY, rotNormY, 1e-4f)
        assertEquals("Rotation must preserve inner product", origDot, rotDot, 1e-3f)
    }

    @Test
    fun testRotatedBinaryCodeQuantizationAndCompression() {
        val dim = 768
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true)
        val rng = Random(12345L)
        val vec = generateRandomVector(dim, rng)

        val code = bq.quantizeRotated(vec)

        // 768 dims / 64 bits = 12 Longs
        assertEquals(12, code.bits.size)
        assertEquals(1.0f, code.l2Norm, 1e-4f)
        assertTrue("Scale factor c_x must be positive", code.scaleFactor > 0f)

        // Bytes: 12 Longs * 8 bytes + 4 bytes l2Norm + 4 bytes scaleFactor = 104 bytes
        val packed = code.packToBytes()
        assertEquals(104, packed.size)

        // Compression ratio vs 768 * 4 bytes = 3072 bytes
        val compressionRatio = (768 * 4).toFloat() / packed.size.toFloat()
        assertEquals("Compression ratio should be ~29.5x", 29.538f, compressionRatio, 0.1f)

        // Round-trip pack / unpack
        val restored = RotatedBinaryCode.unpackFromBytes(packed)
        assertEquals(code, restored)
    }

    @Test
    fun testAsymmetricInnerProductEstimatorUnbiasedness() {
        val dim = 64
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true, randomSeed = 555L)
        val rng = Random(9999L)

        var totalError = 0.0f
        val numPairs = 100

        for (i in 0 until numPairs) {
            val q = generateRandomVector(dim, rng)
            val x = generateRandomVector(dim, rng)

            val exactIp = SimdVectorMath.dotProduct16(q, x, dim)
            val codeX = bq.quantizeRotated(x)
            val estIp = bq.estimateInnerProductAsymmetric(q, codeX)

            totalError += (estIp - exactIp)
        }

        val meanBias = totalError / numPairs
        assertTrue("Asymmetric estimator mean bias should be near 0 (|bias| < 0.05), was $meanBias",
            abs(meanBias) < 0.05f)
    }

    @Test
    fun testSymmetricEstimatorHardwarePopcnt() {
        val dim = 128
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true, randomSeed = 888L)
        val rng = Random(3333L)

        val q = generateRandomVector(dim, rng)
        val x = generateRandomVector(dim, rng)

        val qCode = bq.quantizeRotated(q)
        val xCode = bq.quantizeRotated(x)

        val estCos = bq.estimateCosineSymmetric(qCode, xCode)
        val exactCos = DistanceMetric.COSINE.compute(q, x)

        // Symmetric estimate should be strongly correlated with exact cosine similarity
        assertTrue("Estimated cosine must be in [-1, 1]", estCos in -1.0f..1.0f)
        assertTrue("Estimated cosine should be within 0.35 of exact for random vectors",
            abs(estCos - exactCos) < 0.35f)

        // Euclidean distance estimate
        val estL2 = bq.estimateEuclideanSymmetric(qCode, xCode)
        val exactL2 = -DistanceMetric.EUCLIDEAN.compute(q, x) // compute returns negative distance
        assertTrue("Estimated Euclidean distance must be >= 0", estL2 >= 0f)
        assertTrue("Estimated Euclidean should be within 0.5 of exact", abs(estL2 - exactL2) < 0.5f)
    }

    @Test
    fun testSearchTwoStageRotated() {
        val dim = 64
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true, randomSeed = 42L)
        val rng = Random(777L)

        val numDocs = 50
        val rawVectors = HashMap<String, FloatArray>()
        val rotatedIndex = HashMap<String, RotatedBinaryCode>()

        for (i in 0 until numDocs) {
            val id = "doc_$i"
            val vec = generateRandomVector(dim, rng)
            rawVectors[id] = vec
            rotatedIndex[id] = bq.quantizeRotated(vec)
        }

        // Query near doc_10
        val targetVec = rawVectors["doc_10"]!!
        val query = FloatArray(dim) { targetVec[it] + (rng.nextGaussian().toFloat() * 0.02f) }
        var qNorm = 0f
        for (v in query) qNorm += v * v
        qNorm = sqrt(qNorm)
        for (i in 0 until dim) query[i] /= qNorm

        val results = bq.searchTwoStageRotated(
            query = query,
            index = rotatedIndex,
            limit = 5,
            oversampleFactor = 4,
            metric = DistanceMetric.COSINE,
            exactVectorProvider = { rawVectors[it] }
        )

        assertFalse(results.isEmpty())
        assertEquals(5, results.size)
        // doc_10 should be the top match
        assertEquals("doc_10", results.first().first)
        assertTrue("Top similarity should be very high (> 0.95)", results.first().second > 0.95f)
    }
}
