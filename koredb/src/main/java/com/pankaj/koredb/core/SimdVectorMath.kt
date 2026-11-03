package com.pankaj.koredb.core

import java.nio.ByteBuffer
import kotlin.math.abs
import kotlin.math.sqrt

/**
 * 16-Lane Loop-Unrolled SIMD-Optimized Vector Math Engine.
 *
 * Employs 16-way loop unrolling designed for modern 128-bit/256-bit SIMD registers (ARM NEON, AVX2).
 * Enables the Android ART / JVM C2 JIT compiler to auto-vectorize float operations into packed SIMD instructions
 * (e.g. `fmla.4s` on ARM64) with zero branch misprediction penalties.
 */
object SimdVectorMath {

    /**
     * 16-lane unrolled dot product for two float arrays.
     */
    fun dotProduct16(a: FloatArray, b: FloatArray, size: Int = minOf(a.size, b.size)): Float {
        val n = size.coerceIn(0, minOf(a.size, b.size))
        if (n == 0) return 0f
        var acc0 = 0f
        var acc1 = 0f
        var acc2 = 0f
        var acc3 = 0f

        var i = 0
        val limit = n - 15
        while (i < limit) {
            acc0 += (a[i] * b[i]) + (a[i + 1] * b[i + 1]) + (a[i + 2] * b[i + 2]) + (a[i + 3] * b[i + 3])
            acc1 += (a[i + 4] * b[i + 4]) + (a[i + 5] * b[i + 5]) + (a[i + 6] * b[i + 6]) + (a[i + 7] * b[i + 7])
            acc2 += (a[i + 8] * b[i + 8]) + (a[i + 9] * b[i + 9]) + (a[i + 10] * b[i + 10]) + (a[i + 11] * b[i + 11])
            acc3 += (a[i + 12] * b[i + 12]) + (a[i + 13] * b[i + 13]) + (a[i + 14] * b[i + 14]) + (a[i + 15] * b[i + 15])
            i += 16
        }

        var total = acc0 + acc1 + acc2 + acc3
        while (i < n) {
            total += a[i] * b[i]
            i++
        }
        return total
    }

    /**
     * 16-lane unrolled squared Euclidean distance for two float arrays.
     */
    fun euclideanDistanceSq16(a: FloatArray, b: FloatArray, size: Int = minOf(a.size, b.size)): Float {
        val n = size.coerceIn(0, minOf(a.size, b.size))
        if (n == 0) return 0f
        var acc0 = 0f
        var acc1 = 0f
        var acc2 = 0f
        var acc3 = 0f

        var i = 0
        val limit = n - 15
        while (i < limit) {
            val d0 = a[i] - b[i]; val d1 = a[i + 1] - b[i + 1]; val d2 = a[i + 2] - b[i + 2]; val d3 = a[i + 3] - b[i + 3]
            acc0 += (d0 * d0) + (d1 * d1) + (d2 * d2) + (d3 * d3)

            val d4 = a[i + 4] - b[i + 4]; val d5 = a[i + 5] - b[i + 5]; val d6 = a[i + 6] - b[i + 6]; val d7 = a[i + 7] - b[i + 7]
            acc1 += (d4 * d4) + (d5 * d5) + (d6 * d6) + (d7 * d7)

            val d8 = a[i + 8] - b[i + 8]; val d9 = a[i + 9] - b[i + 9]; val d10 = a[i + 10] - b[i + 10]; val d11 = a[i + 11] - b[i + 11]
            acc2 += (d8 * d8) + (d9 * d9) + (d10 * d10) + (d11 * d11)

            val d12 = a[i + 12] - b[i + 12]; val d13 = a[i + 13] - b[i + 13]; val d14 = a[i + 14] - b[i + 14]; val d15 = a[i + 15] - b[i + 15]
            acc3 += (d12 * d12) + (d13 * d13) + (d14 * d14) + (d15 * d15)

            i += 16
        }

        var total = acc0 + acc1 + acc2 + acc3
        while (i < n) {
            val diff = a[i] - b[i]
            total += diff * diff
            i++
        }
        return total
    }

    /**
     * 16-lane unrolled Manhattan distance for two float arrays.
     */
    fun manhattanDistance16(a: FloatArray, b: FloatArray, size: Int = minOf(a.size, b.size)): Float {
        val n = size.coerceIn(0, minOf(a.size, b.size))
        if (n == 0) return 0f
        var acc0 = 0f
        var acc1 = 0f
        var acc2 = 0f
        var acc3 = 0f

        var i = 0
        val limit = n - 15
        while (i < limit) {
            acc0 += abs(a[i] - b[i]) + abs(a[i + 1] - b[i + 1]) + abs(a[i + 2] - b[i + 2]) + abs(a[i + 3] - b[i + 3])
            acc1 += abs(a[i + 4] - b[i + 4]) + abs(a[i + 5] - b[i + 5]) + abs(a[i + 6] - b[i + 6]) + abs(a[i + 7] - b[i + 7])
            acc2 += abs(a[i + 8] - b[i + 8]) + abs(a[i + 9] - b[i + 9]) + abs(a[i + 10] - b[i + 10]) + abs(a[i + 11] - b[i + 11])
            acc3 += abs(a[i + 12] - b[i + 12]) + abs(a[i + 13] - b[i + 13]) + abs(a[i + 14] - b[i + 14]) + abs(a[i + 15] - b[i + 15])
            i += 16
        }

        var total = acc0 + acc1 + acc2 + acc3
        while (i < n) {
            total += abs(a[i] - b[i])
            i++
        }
        return total
    }

    /**
     * 16-lane unrolled Cosine similarity between two float arrays with precomputed magnitudes.
     */
    fun cosineSimilarity16(a: FloatArray, aNorm: Float, b: FloatArray, bNorm: Float, size: Int = minOf(a.size, b.size)): Float {
        val denom = aNorm * bNorm
        if (aNorm <= 0f || bNorm <= 0f || !denom.isFinite()) return 0f
        val dot = dotProduct16(a, b, size)
        if (!dot.isFinite()) return 0f
        return (dot / denom).coerceIn(-1.0f, 1.0f)
    }

    /**
     * Single-pass fused 8-lane unrolled Cosine similarity between two float arrays when magnitudes
     * are not precomputed.
     *
     * Loads `a[i]` and `b[i]` once from L1 cache and accumulates `dot(a, b)`, `normSq(a)`, and `normSq(b)`
     * simultaneously across independent accumulators, cutting memory bandwidth by 50% compared to
     * separate `getMagnitude16(a)` + `getMagnitude16(b)` + `dotProduct16(a, b)` passes.
     */
    fun cosineSimilarityFused16(a: FloatArray, b: FloatArray, size: Int = minOf(a.size, b.size)): Float {
        val n = size.coerceIn(0, minOf(a.size, b.size))
        if (n == 0) return 0f

        var dot0 = 0f; var dot1 = 0f; var dot2 = 0f; var dot3 = 0f
        var na0 = 0f;  var na1 = 0f;  var na2 = 0f;  var na3 = 0f
        var nb0 = 0f;  var nb1 = 0f;  var nb2 = 0f;  var nb3 = 0f

        var i = 0
        val limit = n - 7
        while (i < limit) {
            val a0 = a[i];     val b0 = b[i]
            val a1 = a[i + 1]; val b1 = b[i + 1]
            dot0 += (a0 * b0) + (a1 * b1)
            na0 += (a0 * a0) + (a1 * a1)
            nb0 += (b0 * b0) + (b1 * b1)

            val a2 = a[i + 2]; val b2 = b[i + 2]
            val a3 = a[i + 3]; val b3 = b[i + 3]
            dot1 += (a2 * b2) + (a3 * b3)
            na1 += (a2 * a2) + (a3 * a3)
            nb1 += (b2 * b2) + (b3 * b3)

            val a4 = a[i + 4]; val b4 = b[i + 4]
            val a5 = a[i + 5]; val b5 = b[i + 5]
            dot2 += (a4 * b4) + (a5 * b5)
            na2 += (a4 * a4) + (a5 * a5)
            nb2 += (b4 * b4) + (b5 * b5)

            val a6 = a[i + 6]; val b6 = b[i + 6]
            val a7 = a[i + 7]; val b7 = b[i + 7]
            dot3 += (a6 * b6) + (a7 * b7)
            na3 += (a6 * a6) + (a7 * a7)
            nb3 += (b6 * b6) + (b7 * b7)

            i += 8
        }

        var dot = dot0 + dot1 + dot2 + dot3
        var normA = na0 + na1 + na2 + na3
        var normB = nb0 + nb1 + nb2 + nb3

        while (i < n) {
            val av = a[i]
            val bv = b[i]
            dot += av * bv
            normA += av * av
            normB += bv * bv
            i++
        }

        val denom = sqrt(normA) * sqrt(normB)
        if (denom <= 0f || !denom.isFinite() || !dot.isFinite()) return 0f
        return (dot / denom).coerceIn(-1.0f, 1.0f)
    }

    /**
     * Calculates L2 norm magnitude using 16-lane unrolling over the first [size] dimensions.
     */
    fun getMagnitude16(v: FloatArray, size: Int = v.size): Float {
        return sqrt(dotProduct16(v, v, size.coerceIn(0, v.size)))
    }

    /**
     * L2-normalizes [v] in-place over the first [size] dimensions using 16-lane unrolling.
     *
     * @return The original L2 magnitude before normalization (`0f` if the zero vector).
     */
    fun normalizeL2InPlace16(v: FloatArray, size: Int = v.size): Float {
        val n = size.coerceIn(0, v.size)
        if (n == 0) return 0f
        val mag = getMagnitude16(v, n)
        if (mag <= 0f || !mag.isFinite()) return 0f
        val invMag = 1.0f / mag

        var i = 0
        val limit = n - 15
        while (i < limit) {
            v[i] *= invMag;      v[i + 1] *= invMag;  v[i + 2] *= invMag;  v[i + 3] *= invMag
            v[i + 4] *= invMag;  v[i + 5] *= invMag;  v[i + 6] *= invMag;  v[i + 7] *= invMag
            v[i + 8] *= invMag;  v[i + 9] *= invMag;  v[i + 10] *= invMag; v[i + 11] *= invMag
            v[i + 12] *= invMag; v[i + 13] *= invMag; v[i + 14] *= invMag; v[i + 15] *= invMag
            i += 16
        }
        while (i < n) {
            v[i] *= invMag
            i++
        }
        return mag
    }

    /**
     * Slices [v] to [targetDimensions] (for Matryoshka Representation Learning embeddings)
     * and optionally re-normalizes the prefix to unit L2 length in a single unrolled pass.
     */
    fun sliceAndNormalizeL216(
        v: FloatArray,
        targetDimensions: Int = v.size,
        normalize: Boolean = true
    ): FloatArray {
        if (v.isEmpty() || targetDimensions <= 0) return FloatArray(0)
        val targetDim = targetDimensions.coerceIn(1, v.size)
        val out = v.copyOf(targetDim)
        if (normalize) {
            normalizeL2InPlace16(out, targetDim)
        }
        return out
    }
}
