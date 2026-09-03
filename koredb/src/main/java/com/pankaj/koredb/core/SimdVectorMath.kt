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
        var acc0 = 0f
        var acc1 = 0f
        var acc2 = 0f
        var acc3 = 0f

        var i = 0
        val limit = size - 15
        while (i < limit) {
            acc0 += (a[i] * b[i]) + (a[i + 1] * b[i + 1]) + (a[i + 2] * b[i + 2]) + (a[i + 3] * b[i + 3])
            acc1 += (a[i + 4] * b[i + 4]) + (a[i + 5] * b[i + 5]) + (a[i + 6] * b[i + 6]) + (a[i + 7] * b[i + 7])
            acc2 += (a[i + 8] * b[i + 8]) + (a[i + 9] * b[i + 9]) + (a[i + 10] * b[i + 10]) + (a[i + 11] * b[i + 11])
            acc3 += (a[i + 12] * b[i + 12]) + (a[i + 13] * b[i + 13]) + (a[i + 14] * b[i + 14]) + (a[i + 15] * b[i + 15])
            i += 16
        }

        var total = acc0 + acc1 + acc2 + acc3
        while (i < size) {
            total += a[i] * b[i]
            i++
        }
        return total
    }

    /**
     * 16-lane unrolled squared Euclidean distance for two float arrays.
     */
    fun euclideanDistanceSq16(a: FloatArray, b: FloatArray, size: Int = minOf(a.size, b.size)): Float {
        var acc0 = 0f
        var acc1 = 0f
        var acc2 = 0f
        var acc3 = 0f

        var i = 0
        val limit = size - 15
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
        while (i < size) {
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
        var acc0 = 0f
        var acc1 = 0f
        var acc2 = 0f
        var acc3 = 0f

        var i = 0
        val limit = size - 15
        while (i < limit) {
            acc0 += abs(a[i] - b[i]) + abs(a[i + 1] - b[i + 1]) + abs(a[i + 2] - b[i + 2]) + abs(a[i + 3] - b[i + 3])
            acc1 += abs(a[i + 4] - b[i + 4]) + abs(a[i + 5] - b[i + 5]) + abs(a[i + 6] - b[i + 6]) + abs(a[i + 7] - b[i + 7])
            acc2 += abs(a[i + 8] - b[i + 8]) + abs(a[i + 9] - b[i + 9]) + abs(a[i + 10] - b[i + 10]) + abs(a[i + 11] - b[i + 11])
            acc3 += abs(a[i + 12] - b[i + 12]) + abs(a[i + 13] - b[i + 13]) + abs(a[i + 14] - b[i + 14]) + abs(a[i + 15] - b[i + 15])
            i += 16
        }

        var total = acc0 + acc1 + acc2 + acc3
        while (i < size) {
            total += abs(a[i] - b[i])
            i++
        }
        return total
    }

    /**
     * 16-lane unrolled Cosine similarity between two float arrays.
     */
    fun cosineSimilarity16(a: FloatArray, aNorm: Float, b: FloatArray, bNorm: Float, size: Int = minOf(a.size, b.size)): Float {
        if (aNorm <= 0f || bNorm <= 0f) return 0f
        val dot = dotProduct16(a, b, size)
        return dot / (aNorm * bNorm)
    }

    /**
     * Calculates L2 norm magnitude using 16-lane unrolling.
     */
    fun getMagnitude16(v: FloatArray): Float {
        return sqrt(dotProduct16(v, v))
    }
}
