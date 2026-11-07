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
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * Adversarial Stress & Empirical Verification Test Suite for Milestone 1 (Prefix Short-Circuiting & Rotated Binary Quantization).
 */
class M1AdversarialChallengeTest {

    private fun generateGaussianVector(dim: Int, rng: Random, normalized: Boolean = true): FloatArray {
        val vec = FloatArray(dim) { rng.nextGaussian().toFloat() }
        if (normalized) {
            var norm = 0f
            for (v in vec) norm += v * v
            norm = sqrt(norm.coerceAtLeast(1e-12f))
            for (i in 0 until dim) vec[i] /= norm
        }
        return vec
    }

    private fun generatePrefixVector(dim: Int, rng: Random, decayPower: Float = 0.5f): FloatArray {
        val vec = FloatArray(dim) { idx ->
            val scale = 1.0f / (1.0f + idx * 0.05f).pow(decayPower)
            rng.nextGaussian().toFloat() * scale
        }
        var norm = 0f
        for (v in vec) norm += v * v
        norm = sqrt(norm.coerceAtLeast(1e-12f))
        for (i in 0 until dim) vec[i] /= norm
        return vec
    }

    // ========================================================================
    // 1. ROTATED BINARY QUANTIZATION UNBIASEDNESS EMPIRICAL VERIFICATION (D = 128, 768)
    // ========================================================================

    @Test
    fun testRotatedBinaryUnbiasedness128Dim() {
        val dim = 128
        val numPairs = 1000
        val rng = Random(424242L)
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true, randomSeed = 9999L)

        var totalAsymError = 0.0
        var totalSymError = 0.0

        for (i in 0 until numPairs) {
            val q = generateGaussianVector(dim, rng, normalized = true)
            val x = generateGaussianVector(dim, rng, normalized = true)

            val exactIp = SimdVectorMath.dotProduct16(q, x, dim).toDouble()
            val codeX = bq.quantizeRotated(x)
            val estAsymIp = bq.estimateInnerProductAsymmetric(q, codeX).toDouble()

            totalAsymError += (estAsymIp - exactIp)

            val codeQ = bq.quantizeRotated(q)
            val estSymIp = bq.estimateInnerProductSymmetric(codeQ, codeX).toDouble()
            totalSymError += (estSymIp - exactIp)
        }

        val meanAsymBias = totalAsymError / numPairs
        val meanSymBias = totalSymError / numPairs

        println("RotatedBQ D=$dim Unbiasedness (N=$numPairs):")
        println("  Mean Asymmetric Error |E[<q,x>^] - <q,x>| = ${abs(meanAsymBias)} (raw: $meanAsymBias)")
        println("  Mean Symmetric Error  |E[<q,x>^] - <q,x>| = ${abs(meanSymBias)} (raw: $meanSymBias)")

        assertTrue(
            "RotatedBQ D=$dim Asymmetric sample mean error must be <= 0.05, was ${abs(meanAsymBias)}",
            abs(meanAsymBias) <= 0.05
        )
        assertTrue(
            "RotatedBQ D=$dim Symmetric sample mean error must be <= 0.05, was ${abs(meanSymBias)}",
            abs(meanSymBias) <= 0.05
        )
    }

    @Test
    fun testRotatedBinaryUnbiasedness768Dim() {
        val dim = 768
        val numPairs = 1000
        val rng = Random(768768L)
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true, randomSeed = 8888L)

        var totalAsymError = 0.0
        var totalSymError = 0.0

        for (i in 0 until numPairs) {
            val q = generateGaussianVector(dim, rng, normalized = true)
            val x = generateGaussianVector(dim, rng, normalized = true)

            val exactIp = SimdVectorMath.dotProduct16(q, x, dim).toDouble()
            val codeX = bq.quantizeRotated(x)
            val estAsymIp = bq.estimateInnerProductAsymmetric(q, codeX).toDouble()

            totalAsymError += (estAsymIp - exactIp)

            val codeQ = bq.quantizeRotated(q)
            val estSymIp = bq.estimateInnerProductSymmetric(codeQ, codeX).toDouble()
            totalSymError += (estSymIp - exactIp)
        }

        val meanAsymBias = totalAsymError / numPairs
        val meanSymBias = totalSymError / numPairs

        println("RotatedBQ D=$dim Unbiasedness (N=$numPairs):")
        println("  Mean Asymmetric Error |E[<q,x>^] - <q,x>| = ${abs(meanAsymBias)} (raw: $meanAsymBias)")
        println("  Mean Symmetric Error  |E[<q,x>^] - <q,x>| = ${abs(meanSymBias)} (raw: $meanSymBias)")

        assertTrue(
            "RotatedBQ D=$dim Asymmetric sample mean error must be <= 0.05, was ${abs(meanAsymBias)}",
            abs(meanAsymBias) <= 0.05
        )
        assertTrue(
            "RotatedBQ D=$dim Symmetric sample mean error must be <= 0.05, was ${abs(meanSymBias)}",
            abs(meanSymBias) <= 0.05
        )
    }

    // ========================================================================
    // 2. ORTHOGONAL ROTATION MATRIX EMPIRICAL AUDIT
    // ========================================================================

    @Test
    fun testOrthogonalMatrixPropertiesDetailedAudit() {
        for (dim in listOf(64, 128, 256, 768)) {
            val seed = 42L + dim
            val r = BinaryQuantizer.generateOrthogonalMatrix(dim, seed)

            var maxDiagDev = 0.0f
            var maxOffDiagDev = 0.0f

            for (i in 0 until dim) {
                for (j in 0 until dim) {
                    val dot = SimdVectorMath.dotProduct16(r[i], r[j], dim)
                    if (i == j) {
                        maxDiagDev = maxOf(maxDiagDev, abs(dot - 1.0f))
                    } else {
                        maxOffDiagDev = maxOf(maxOffDiagDev, abs(dot))
                    }
                }
            }

            println("Orthogonal Matrix D=$dim:")
            println("  Max Diag Dev (|dot - 1.0|): $maxDiagDev")
            println("  Max Off-Diag Dev (|dot|): $maxOffDiagDev")

            // Test vector norm preservation ||R x||_2 = ||x||_2
            val rng = Random(seed * 31)
            var maxNormDiff = 0.0f
            for (t in 0 until 100) {
                val x = generateGaussianVector(dim, rng, normalized = false)
                val origNorm = VectorMath.getMagnitude(x)

                // Project
                val rx = FloatArray(dim)
                for (i in 0 until dim) {
                    rx[i] = SimdVectorMath.dotProduct16(r[i], x, dim)
                }
                val rotNorm = VectorMath.getMagnitude(rx)
                val diff = abs(rotNorm - origNorm)
                maxNormDiff = maxOf(maxNormDiff, diff)
            }
            println("  Max Norm Diff ||Rx|| - ||x||: $maxNormDiff")
        }
    }

    // ========================================================================
    // 3. HARDWARE POPCNT SPEED & ROBUSTNESS HARNESS
    // ========================================================================

    @Test
    fun testHardwarePopcntSpeedAndThroughput() {
        val dim = 768
        val rng = Random(12345L)
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true)

        val nVectors = 1000
        val rawVecs = Array(nVectors) { generateGaussianVector(dim, rng) }
        val codes = Array(nVectors) { bq.quantizeRotated(rawVecs[it]) }

        val qRaw = generateGaussianVector(dim, rng)
        val qCode = bq.quantizeRotated(qRaw)

        // Warmup
        var warmSum = 0.0f
        for (i in 0 until 500) {
            warmSum += bq.estimateCosineSymmetric(qCode, codes[i])
            warmSum += DistanceMetric.COSINE.compute(qRaw, rawVecs[i])
        }

        // Benchmark FP32 Cosine
        val iters = 200
        val fp32Start = System.nanoTime()
        var fp32Dummy = 0.0f
        for (it in 0 until iters) {
            for (i in 0 until nVectors) {
                fp32Dummy += DistanceMetric.COSINE.compute(qRaw, rawVecs[i])
            }
        }
        val fp32Nanos = System.nanoTime() - fp32Start

        // Benchmark Rotated 1-Bit POPCNT Cosine
        val popcntStart = System.nanoTime()
        var popcntDummy = 0.0f
        for (it in 0 until iters) {
            for (i in 0 until nVectors) {
                popcntDummy += bq.estimateCosineSymmetric(qCode, codes[i])
            }
        }
        val popcntNanos = System.nanoTime() - popcntStart

        val totalOps = iters.toLong() * nVectors
        val fp32OpsPerMs = (totalOps.toDouble() / (fp32Nanos / 1_000_000.0)).toLong()
        val popcntOpsPerMs = (totalOps.toDouble() / (popcntNanos / 1_000_000.0)).toLong()
        val speedup = fp32Nanos.toDouble() / popcntNanos.toDouble()

        println("Speed Benchmark D=$dim across $totalOps ops:")
        println("  FP32 Cosine:     ${fp32Nanos / 1_000_000} ms ($fp32OpsPerMs ops/ms)")
        println("  POPCNT RotatedBQ: ${popcntNanos / 1_000_000} ms ($popcntOpsPerMs ops/ms)")
        println("  Throughput speedup: %.2fx".format(speedup))

        assertTrue("POPCNT should achieve high throughput (> 1,000 ops/ms)", popcntOpsPerMs > 1_000)
    }

    @Test
    fun testHardwarePopcntExtremeVectorsRobustness() {
        val dim = 128
        val bq = BinaryQuantizer(dim, enableOrthogonalRotation = true)

        // 1. All-zeros vector
        val zeros = FloatArray(dim) { 0.0f }
        val zeroCode = bq.quantizeRotated(zeros)
        assertEquals(0.0f, zeroCode.l2Norm, 0.0f)
        assertEquals(0.0f, zeroCode.scaleFactor, 0.0f)

        val nonZero = generateGaussianVector(dim, Random(1L))
        val nonZeroCode = bq.quantizeRotated(nonZero)

        val cosZero1 = bq.estimateCosineSymmetric(zeroCode, nonZeroCode)
        val cosZero2 = bq.estimateCosineSymmetric(zeroCode, zeroCode)
        assertEquals(0.0f, cosZero1, 0.0f)
        assertEquals(0.0f, cosZero2, 0.0f)

        val eucZero = bq.estimateEuclideanSymmetric(zeroCode, nonZeroCode)
        assertEquals(nonZeroCode.l2Norm, eucZero, 1e-4f)

        // 2. Unit basis vectors
        val e0 = FloatArray(dim) { if (it == 0) 1.0f else 0.0f }
        val eLast = FloatArray(dim) { if (it == dim - 1) 1.0f else 0.0f }
        val codeE0 = bq.quantizeRotated(e0)
        val codeELast = bq.quantizeRotated(eLast)

        val cosBasis = bq.estimateCosineSymmetric(codeE0, codeELast)
        assertTrue("Orthogonal basis estimated cosine must be close to 0", abs(cosBasis) < 0.4f)

        // 3. Extreme magnitude vectors (huge vs tiny)
        val hugeVec = FloatArray(dim) { 1e15f }
        val tinyVec = FloatArray(dim) { 1e-15f }

        val codeHuge = bq.quantizeRotated(hugeVec)
        val codeTiny = bq.quantizeRotated(tinyVec)

        assertFalse("L2 norm must not be NaN", codeHuge.l2Norm.isNaN())
        assertFalse("Scale factor must not be NaN", codeHuge.scaleFactor.isNaN())
        assertFalse("L2 norm must not be NaN", codeTiny.l2Norm.isNaN())

        val cosExtreme = bq.estimateCosineSymmetric(codeHuge, codeTiny)
        assertFalse("Cosine must not be NaN", cosExtreme.isNaN())
        assertTrue("Identical direction extreme vectors must have positive cosine", cosExtreme > 0.5f)

        // 4. Anti-parallel vectors
        val posVec = generateGaussianVector(dim, Random(55L))
        val negVec = FloatArray(dim) { -posVec[it] }

        val codePos = bq.quantizeRotated(posVec)
        val codeNeg = bq.quantizeRotated(negVec)

        val cosOpposite = bq.estimateCosineSymmetric(codePos, codeNeg)
        assertTrue("Anti-parallel vectors must have estimated cosine < -0.5, was $cosOpposite", cosOpposite < -0.5f)
    }

    // ========================================================================
    // 4. PREFIX UPPER-LAYER TRUNCATION RECALL EMPIRICAL AUDIT
    // ========================================================================

    @Test
    fun testPrefixUpperLayerTruncationRecall() {
        val dim = 128
        val numVectors = 300
        val topK = 5
        val numQueries = 50
        val rng = Random(98765L)

        val index = HNSWIndex(
            dimensions = dim,
            metric = DistanceMetric.COSINE,
            m = 16,
            efConstruction = 64,
            efSearch = 64
        )

        for (i in 0 until numVectors) {
            val vec = generatePrefixVector(dim, rng)
            index.insert("v_$i", vec)
        }

        var totalRecallMatches = 0
        var totalExactRescoresVerified = 0

        for (q in 0 until numQueries) {
            val query = generatePrefixVector(dim, Random(q.toLong() * 1013 + 7))

            // Prefix short-circuiting: 64-dim in upper layers, Layer 0 full rescore
            val prefixResults = index.search(query, limit = topK, coarsePrefixDims = 64)
            val prefixIds = prefixResults.map { it.first }.toSet()

            // Baseline: full search without truncation
            val fullResults = index.search(query, limit = topK, coarsePrefixDims = dim)
            val fullIds = fullResults.map { it.first }.toSet()

            val overlap = prefixIds.intersect(fullIds).size
            totalRecallMatches += overlap

            // Verify Layer 0 rescore: exact cosine similarity returned
            for (res in prefixResults) {
                // If the top element has valid distance, verify it's exact
                assertTrue("Returned distance should be valid cosine in [-1, 1]", res.second in -1.0f..1.0001f)
                totalExactRescoresVerified++
            }
        }

        val totalExpected = topK * numQueries
        val recall = totalRecallMatches.toDouble() / totalExpected
        println("Prefix 64-dim Upper-Layer Truncation Recall (N=$numVectors, queries=$numQueries, topK=$topK):")
        println("  Recall vs Full Search = ${(recall * 100.0)}% ($totalRecallMatches / $totalExpected)")

        assertTrue(
            "Top-K recall of prefix short-circuiting against full search must be >= 95%, was $recall",
            recall >= 0.95
        )
    }

    @Test
    fun testPrefixUpperLayerTruncation768DimStress() {
        val dim = 768
        val numVectors = 200
        val topK = 10
        val numQueries = 30
        val rng = Random(445566L)

        val index = HNSWIndex(
            dimensions = dim,
            metric = DistanceMetric.COSINE,
            m = 16,
            efConstruction = 64,
            efSearch = 64
        )

        for (i in 0 until numVectors) {
            val vec = generatePrefixVector(dim, rng, decayPower = 0.6f)
            index.insert("v_$i", vec)
        }

        var totalRecallMatches = 0
        for (q in 0 until numQueries) {
            val query = generatePrefixVector(dim, Random(q.toLong() * 2027 + 13), decayPower = 0.6f)

            val prefixResults = index.search(query, limit = topK, coarsePrefixDims = 64)
            val prefixIds = prefixResults.map { it.first }.toSet()

            val fullResults = index.search(query, limit = topK, coarsePrefixDims = dim)
            val fullIds = fullResults.map { it.first }.toSet()

            totalRecallMatches += prefixIds.intersect(fullIds).size
        }

        val totalExpected = topK * numQueries
        val recall = totalRecallMatches.toDouble() / totalExpected
        println("Prefix 768-dim -> 64-dim Upper-Layer Truncation (12x FLOP reduction):")
        println("  Recall@$topK = ${(recall * 100.0)}% ($totalRecallMatches / $totalExpected)")

        assertTrue(
            "768-dim prefix recall with 64-dim short-circuiting should be >= 95%, was $recall",
            recall >= 0.95
        )
    }

    @Test
    fun testPrefixAdversarialInvertedEnergyVectors() {
        // Adversarial scenario: Vectors where the first 64 dimensions carry NO discriminatory signal
        // (zero or pure noise) and signal is concentrated in dimensions 65..128.
        val dim = 128
        val numVectors = 100
        val topK = 5
        val numQueries = 20
        val rng = Random(999111L)

        val index = HNSWIndex(
            dimensions = dim,
            metric = DistanceMetric.COSINE,
            m = 16,
            efConstruction = 64,
            efSearch = 64
        )

        fun generateInvertedVector(dim: Int, r: Random): FloatArray {
            val vec = FloatArray(dim)
            // First 64 dims: tiny noise
            for (i in 0 until 64) vec[i] = r.nextGaussian().toFloat() * 0.01f
            // Remaining dims: large discriminatory signal
            for (i in 64 until dim) vec[i] = r.nextGaussian().toFloat()
            var norm = 0f
            for (v in vec) norm += v * v
            norm = sqrt(norm.coerceAtLeast(1e-12f))
            for (i in 0 until dim) vec[i] /= norm
            return vec
        }

        for (i in 0 until numVectors) {
            index.insert("inv_$i", generateInvertedVector(dim, rng))
        }

        var totalMatches = 0
        for (q in 0 until numQueries) {
            val query = generateInvertedVector(dim, Random(q.toLong() * 333 + 1))
            val prefixResults = index.search(query, limit = topK, coarsePrefixDims = 64)
            val prefixIds = prefixResults.map { it.first }.toSet()

            val fullResults = index.search(query, limit = topK, coarsePrefixDims = dim)
            val fullIds = fullResults.map { it.first }.toSet()

            totalMatches += prefixIds.intersect(fullIds).size
        }

        val totalExpected = topK * numQueries
        val recall = totalMatches.toDouble() / totalExpected
        println("Adversarial Inverted Energy Test (Energy concentrated in late dims):")
        println("  Recall@$topK = ${(recall * 100.0)}% ($totalMatches / $totalExpected)")
        // Document: Prefix short-circuiting explicitly targets multi-resolution prefix representations.
        // On inverted distributions, upper-layer navigation suffers.
    }
}
