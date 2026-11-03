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

import kotlin.math.abs
import kotlin.math.sqrt

/**
 * Scalar Quantizer that compresses 32-bit float vectors into 8-bit unsigned integers.
 *
 * This provides **4x memory reduction** with typically **<1% recall loss** for
 * normalized embeddings. Essential for mobile/embedded deployments where RAM is limited.
 *
 * ### How it works:
 * 1. Training: Scans a set of vectors to compute per-dimension min/max bounds.
 * 2. Quantization: Maps each float to [0, 255] within the learned bounds.
 * 3. Distance: Computes approximate distances directly on quantized vectors without heap allocation.
 *
 * @param dimensions The dimensionality of the vectors to quantize.
 */
class ScalarQuantizer(val dimensions: Int) {

    init {
        require(dimensions > 0) { "Dimensions must be positive, got $dimensions" }
    }

    // Per-dimension calibration bounds (initialized to 0f so untrained calls never overflow to Float.MAX_VALUE/NaN)
    private val minBounds = FloatArray(dimensions)
    private val maxBounds = FloatArray(dimensions)

    // Pre-computed per-dimension scale factors for fast quantize/dequantize
    private val scales = FloatArray(dimensions)
    private val inverseScales = FloatArray(dimensions)

    @Volatile
    private var trained = false

    /**
     * Trains the quantizer on a representative sample of vectors.
     * Safe to invoke multiple times to recalibrate bounds.
     *
     * @param vectors A representative sample (at least 100 vectors recommended).
     */
    fun train(vectors: List<FloatArray>) {
        if (vectors.isEmpty()) return

        val localMin = FloatArray(dimensions) { Float.MAX_VALUE }
        val localMax = FloatArray(dimensions) { -Float.MAX_VALUE }
        var validCount = 0

        // Pass 1: Find per-dimension min/max across valid finite vectors
        for (vector in vectors) {
            val len = minOf(dimensions, vector.size)
            if (len <= 0) continue
            validCount++
            for (d in 0 until len) {
                val v = vector[d]
                if (!v.isFinite()) continue
                if (v < localMin[d]) localMin[d] = v
                if (v > localMax[d]) localMax[d] = v
            }
        }
        if (validCount == 0) return

        // Pass 2: Compute scale factors and commit calibration bounds
        for (d in 0 until dimensions) {
            val minV = if (localMin[d] <= localMax[d]) localMin[d] else 0f
            val maxV = if (localMax[d] >= localMin[d]) localMax[d] else 0f
            minBounds[d] = minV
            maxBounds[d] = maxV
            val range = maxV - minV
            scales[d] = if (range > 0f && range.isFinite()) 255f / range else 0f
            inverseScales[d] = if (range > 0f && range.isFinite()) range / 255f else 0f
        }

        trained = true
    }

    /**
     * Quantizes a float vector to a compact byte array using 8-lane loop unrolling.
     * Each float is mapped to [0, 255] based on the trained bounds.
     *
     * @param vector The FP32 vector to quantize.
     * @return A byte array of length [dimensions] representing the quantized vector.
     */
    fun quantize(vector: FloatArray): ByteArray {
        require(vector.size >= dimensions) {
            "Expected vector of at least $dimensions dimensions, got ${vector.size}"
        }
        val result = ByteArray(dimensions)
        val mins = minBounds
        val s = scales
        val n = dimensions
        var d = 0
        val limit = n - 7
        while (d < limit) {
            result[d]     = ((vector[d]     - mins[d])     * s[d]).coerceIn(0f, 255f).toInt().toByte()
            result[d + 1] = ((vector[d + 1] - mins[d + 1]) * s[d + 1]).coerceIn(0f, 255f).toInt().toByte()
            result[d + 2] = ((vector[d + 2] - mins[d + 2]) * s[d + 2]).coerceIn(0f, 255f).toInt().toByte()
            result[d + 3] = ((vector[d + 3] - mins[d + 3]) * s[d + 3]).coerceIn(0f, 255f).toInt().toByte()
            result[d + 4] = ((vector[d + 4] - mins[d + 4]) * s[d + 4]).coerceIn(0f, 255f).toInt().toByte()
            result[d + 5] = ((vector[d + 5] - mins[d + 5]) * s[d + 5]).coerceIn(0f, 255f).toInt().toByte()
            result[d + 6] = ((vector[d + 6] - mins[d + 6]) * s[d + 6]).coerceIn(0f, 255f).toInt().toByte()
            result[d + 7] = ((vector[d + 7] - mins[d + 7]) * s[d + 7]).coerceIn(0f, 255f).toInt().toByte()
            d += 8
        }
        while (d < n) {
            val normalized = (vector[d] - mins[d]) * s[d]
            result[d] = normalized.coerceIn(0f, 255f).toInt().toByte()
            d++
        }
        return result
    }

    /**
     * Dequantizes a byte array back to an approximate float vector using 8-lane loop unrolling.
     *
     * @param quantized The quantized byte array.
     * @return An approximate FP32 reconstruction.
     */
    fun dequantize(quantized: ByteArray): FloatArray {
        require(quantized.size >= dimensions) {
            "Expected quantized vector of at least $dimensions bytes, got ${quantized.size}"
        }
        val result = FloatArray(dimensions)
        val mins = minBounds
        val inv = inverseScales
        val n = dimensions
        var d = 0
        val limit = n - 7
        while (d < limit) {
            result[d]     = (quantized[d].toInt()     and 0xFF) * inv[d]     + mins[d]
            result[d + 1] = (quantized[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1]
            result[d + 2] = (quantized[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2]
            result[d + 3] = (quantized[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3]
            result[d + 4] = (quantized[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4]
            result[d + 5] = (quantized[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5]
            result[d + 6] = (quantized[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6]
            result[d + 7] = (quantized[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7]
            d += 8
        }
        while (d < n) {
            result[d] = (quantized[d].toInt() and 0xFF) * inv[d] + mins[d]
            d++
        }
        return result
    }

    /**
     * High-performance 8-lane unrolled distance calculation directly against SQ8 quantized bytes.
     * Eliminates FloatArray heap allocations on every graph hop and breaks floating-point
     * accumulator dependency chains via 4 independent vector accumulators.
     *
     * @param query The FP32 query vector.
     * @param quantized The SQ8 quantized vector.
     * @param metric The distance metric to use.
     * @return The approximate similarity score.
     */
    fun computeDistance(query: FloatArray, quantized: ByteArray, metric: DistanceMetric): Float {
        return computeDistancePrefix(query, quantized, metric, dimensions)
    }

    /**
     * Computes SQ8 distance over only the first [prefixDims] dimensions (supports MRL prefix evaluation
     * without allocating temporary dequantized FloatArrays).
     */
    fun computeDistancePrefix(
        query: FloatArray,
        quantized: ByteArray,
        metric: DistanceMetric,
        prefixDims: Int = dimensions
    ): Float {
        val n = minOf(prefixDims, minOf(dimensions, minOf(query.size, quantized.size)))
        if (n <= 0) return 0f
        val mins = minBounds
        val inv = inverseScales
        val limit = n - 7

        return when (metric) {
            DistanceMetric.COSINE -> {
                var dot0 = 0f; var dot1 = 0f; var dot2 = 0f; var dot3 = 0f
                var na0 = 0f;  var na1 = 0f;  var na2 = 0f;  var na3 = 0f
                var nb0 = 0f;  var nb1 = 0f;  var nb2 = 0f;  var nb3 = 0f
                var d = 0
                while (d < limit) {
                    val q0 = query[d];     val r0 = (quantized[d].toInt()     and 0xFF) * inv[d]     + mins[d]
                    val q1 = query[d + 1]; val r1 = (quantized[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1]
                    dot0 += (q0 * r0) + (q1 * r1)
                    na0 += (q0 * q0) + (q1 * q1)
                    nb0 += (r0 * r0) + (r1 * r1)

                    val q2 = query[d + 2]; val r2 = (quantized[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2]
                    val q3 = query[d + 3]; val r3 = (quantized[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3]
                    dot1 += (q2 * r2) + (q3 * r3)
                    na1 += (q2 * q2) + (q3 * q3)
                    nb1 += (r2 * r2) + (r3 * r3)

                    val q4 = query[d + 4]; val r4 = (quantized[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4]
                    val q5 = query[d + 5]; val r5 = (quantized[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5]
                    dot2 += (q4 * r4) + (q5 * r5)
                    na2 += (q4 * q4) + (q5 * q5)
                    nb2 += (r4 * r4) + (r5 * r5)

                    val q6 = query[d + 6]; val r6 = (quantized[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6]
                    val q7 = query[d + 7]; val r7 = (quantized[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7]
                    dot3 += (q6 * r6) + (q7 * r7)
                    na3 += (q6 * q6) + (q7 * q7)
                    nb3 += (r6 * r6) + (r7 * r7)

                    d += 8
                }
                var dot = dot0 + dot1 + dot2 + dot3
                var normA = na0 + na1 + na2 + na3
                var normB = nb0 + nb1 + nb2 + nb3
                while (d < n) {
                    val q = query[d]
                    val recon = (quantized[d].toInt() and 0xFF) * inv[d] + mins[d]
                    dot += q * recon
                    normA += q * q
                    normB += recon * recon
                    d++
                }
                val div = sqrt(normA) * sqrt(normB)
                if (div <= 0f || !div.isFinite() || !dot.isFinite()) 0f else (dot / div).coerceIn(-1.0f, 1.0f)
            }
            DistanceMetric.EUCLIDEAN -> {
                var acc0 = 0f; var acc1 = 0f; var acc2 = 0f; var acc3 = 0f
                var d = 0
                while (d < limit) {
                    val diff0 = query[d]     - ((quantized[d].toInt()     and 0xFF) * inv[d]     + mins[d])
                    val diff1 = query[d + 1] - ((quantized[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1])
                    acc0 += (diff0 * diff0) + (diff1 * diff1)

                    val diff2 = query[d + 2] - ((quantized[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2])
                    val diff3 = query[d + 3] - ((quantized[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3])
                    acc1 += (diff2 * diff2) + (diff3 * diff3)

                    val diff4 = query[d + 4] - ((quantized[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4])
                    val diff5 = query[d + 5] - ((quantized[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5])
                    acc2 += (diff4 * diff4) + (diff5 * diff5)

                    val diff6 = query[d + 6] - ((quantized[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6])
                    val diff7 = query[d + 7] - ((quantized[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7])
                    acc3 += (diff6 * diff6) + (diff7 * diff7)

                    d += 8
                }
                var sumSq = acc0 + acc1 + acc2 + acc3
                while (d < n) {
                    val recon = (quantized[d].toInt() and 0xFF) * inv[d] + mins[d]
                    val diff = query[d] - recon
                    sumSq += diff * diff
                    d++
                }
                -sqrt(sumSq)
            }
            DistanceMetric.INNER_PRODUCT -> {
                var acc0 = 0f; var acc1 = 0f; var acc2 = 0f; var acc3 = 0f
                var d = 0
                while (d < limit) {
                    val r0 = (quantized[d].toInt()     and 0xFF) * inv[d]     + mins[d]
                    val r1 = (quantized[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1]
                    acc0 += (query[d] * r0) + (query[d + 1] * r1)

                    val r2 = (quantized[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2]
                    val r3 = (quantized[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3]
                    acc1 += (query[d + 2] * r2) + (query[d + 3] * r3)

                    val r4 = (quantized[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4]
                    val r5 = (quantized[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5]
                    acc2 += (query[d + 4] * r4) + (query[d + 5] * r5)

                    val r6 = (quantized[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6]
                    val r7 = (quantized[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7]
                    acc3 += (query[d + 6] * r6) + (query[d + 7] * r7)

                    d += 8
                }
                var dot = acc0 + acc1 + acc2 + acc3
                while (d < n) {
                    val recon = (quantized[d].toInt() and 0xFF) * inv[d] + mins[d]
                    dot += query[d] * recon
                    d++
                }
                dot
            }
            DistanceMetric.MANHATTAN -> {
                var acc0 = 0f; var acc1 = 0f; var acc2 = 0f; var acc3 = 0f
                var d = 0
                while (d < limit) {
                    val diff0 = query[d]     - ((quantized[d].toInt()     and 0xFF) * inv[d]     + mins[d])
                    val diff1 = query[d + 1] - ((quantized[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1])
                    acc0 += abs(diff0) + abs(diff1)

                    val diff2 = query[d + 2] - ((quantized[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2])
                    val diff3 = query[d + 3] - ((quantized[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3])
                    acc1 += abs(diff2) + abs(diff3)

                    val diff4 = query[d + 4] - ((quantized[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4])
                    val diff5 = query[d + 5] - ((quantized[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5])
                    acc2 += abs(diff4) + abs(diff5)

                    val diff6 = query[d + 6] - ((quantized[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6])
                    val diff7 = query[d + 7] - ((quantized[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7])
                    acc3 += abs(diff6) + abs(diff7)

                    d += 8
                }
                var sum = acc0 + acc1 + acc2 + acc3
                while (d < n) {
                    val recon = (quantized[d].toInt() and 0xFF) * inv[d] + mins[d]
                    sum += abs(query[d] - recon)
                    d++
                }
                -sum
            }
        }
    }

    /**
     * Zero-allocation 8-lane unrolled distance calculation directly between two SQ8 quantized byte arrays.
     * Eliminates temporary `FloatArray` allocations during HNSW RNG neighbor pruning.
     */
    fun computeDistanceQuantized(a: ByteArray, b: ByteArray, metric: DistanceMetric): Float {
        val n = minOf(dimensions, minOf(a.size, b.size))
        if (n <= 0) return 0f
        val mins = minBounds
        val inv = inverseScales
        val limit = n - 7

        return when (metric) {
            DistanceMetric.COSINE -> {
                var dot0 = 0f; var dot1 = 0f; var dot2 = 0f; var dot3 = 0f
                var na0 = 0f;  var na1 = 0f;  var na2 = 0f;  var na3 = 0f
                var nb0 = 0f;  var nb1 = 0f;  var nb2 = 0f;  var nb3 = 0f
                var d = 0
                while (d < limit) {
                    val a0 = (a[d].toInt()     and 0xFF) * inv[d]     + mins[d]; val b0 = (b[d].toInt()     and 0xFF) * inv[d]     + mins[d]
                    val a1 = (a[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1]; val b1 = (b[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1]
                    dot0 += (a0 * b0) + (a1 * b1)
                    na0 += (a0 * a0) + (a1 * a1)
                    nb0 += (b0 * b0) + (b1 * b1)

                    val a2 = (a[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2]; val b2 = (b[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2]
                    val a3 = (a[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3]; val b3 = (b[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3]
                    dot1 += (a2 * b2) + (a3 * b3)
                    na1 += (a2 * a2) + (a3 * a3)
                    nb1 += (b2 * b2) + (b3 * b3)

                    val a4 = (a[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4]; val b4 = (b[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4]
                    val a5 = (a[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5]; val b5 = (b[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5]
                    dot2 += (a4 * b4) + (a5 * b5)
                    na2 += (a4 * a4) + (a5 * a5)
                    nb2 += (b4 * b4) + (b5 * b5)

                    val a6 = (a[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6]; val b6 = (b[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6]
                    val a7 = (a[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7]; val b7 = (b[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7]
                    dot3 += (a6 * b6) + (a7 * b7)
                    na3 += (a6 * a6) + (a7 * a7)
                    nb3 += (b6 * b6) + (b7 * b7)

                    d += 8
                }
                var dot = dot0 + dot1 + dot2 + dot3
                var normA = na0 + na1 + na2 + na3
                var normB = nb0 + nb1 + nb2 + nb3
                while (d < n) {
                    val av = (a[d].toInt() and 0xFF) * inv[d] + mins[d]
                    val bv = (b[d].toInt() and 0xFF) * inv[d] + mins[d]
                    dot += av * bv
                    normA += av * av
                    normB += bv * bv
                    d++
                }
                val div = sqrt(normA) * sqrt(normB)
                if (div <= 0f || !div.isFinite() || !dot.isFinite()) 0f else (dot / div).coerceIn(-1.0f, 1.0f)
            }
            DistanceMetric.EUCLIDEAN -> {
                var acc0 = 0f; var acc1 = 0f; var acc2 = 0f; var acc3 = 0f
                var d = 0
                while (d < limit) {
                    val diff0 = ((a[d].toInt()     and 0xFF) - (b[d].toInt()     and 0xFF)) * inv[d]
                    val diff1 = ((a[d + 1].toInt() and 0xFF) - (b[d + 1].toInt() and 0xFF)) * inv[d + 1]
                    acc0 += (diff0 * diff0) + (diff1 * diff1)

                    val diff2 = ((a[d + 2].toInt() and 0xFF) - (b[d + 2].toInt() and 0xFF)) * inv[d + 2]
                    val diff3 = ((a[d + 3].toInt() and 0xFF) - (b[d + 3].toInt() and 0xFF)) * inv[d + 3]
                    acc1 += (diff2 * diff2) + (diff3 * diff3)

                    val diff4 = ((a[d + 4].toInt() and 0xFF) - (b[d + 4].toInt() and 0xFF)) * inv[d + 4]
                    val diff5 = ((a[d + 5].toInt() and 0xFF) - (b[d + 5].toInt() and 0xFF)) * inv[d + 5]
                    acc2 += (diff4 * diff4) + (diff5 * diff5)

                    val diff6 = ((a[d + 6].toInt() and 0xFF) - (b[d + 6].toInt() and 0xFF)) * inv[d + 6]
                    val diff7 = ((a[d + 7].toInt() and 0xFF) - (b[d + 7].toInt() and 0xFF)) * inv[d + 7]
                    acc3 += (diff6 * diff6) + (diff7 * diff7)

                    d += 8
                }
                var sumSq = acc0 + acc1 + acc2 + acc3
                while (d < n) {
                    val diff = ((a[d].toInt() and 0xFF) - (b[d].toInt() and 0xFF)) * inv[d]
                    sumSq += diff * diff
                    d++
                }
                -sqrt(sumSq)
            }
            DistanceMetric.INNER_PRODUCT -> {
                var acc0 = 0f; var acc1 = 0f; var acc2 = 0f; var acc3 = 0f
                var d = 0
                while (d < limit) {
                    val a0 = (a[d].toInt()     and 0xFF) * inv[d]     + mins[d]; val b0 = (b[d].toInt()     and 0xFF) * inv[d]     + mins[d]
                    val a1 = (a[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1]; val b1 = (b[d + 1].toInt() and 0xFF) * inv[d + 1] + mins[d + 1]
                    acc0 += (a0 * b0) + (a1 * b1)

                    val a2 = (a[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2]; val b2 = (b[d + 2].toInt() and 0xFF) * inv[d + 2] + mins[d + 2]
                    val a3 = (a[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3]; val b3 = (b[d + 3].toInt() and 0xFF) * inv[d + 3] + mins[d + 3]
                    acc1 += (a2 * b2) + (a3 * b3)

                    val a4 = (a[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4]; val b4 = (b[d + 4].toInt() and 0xFF) * inv[d + 4] + mins[d + 4]
                    val a5 = (a[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5]; val b5 = (b[d + 5].toInt() and 0xFF) * inv[d + 5] + mins[d + 5]
                    acc2 += (a4 * b4) + (a5 * b5)

                    val a6 = (a[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6]; val b6 = (b[d + 6].toInt() and 0xFF) * inv[d + 6] + mins[d + 6]
                    val a7 = (a[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7]; val b7 = (b[d + 7].toInt() and 0xFF) * inv[d + 7] + mins[d + 7]
                    acc3 += (a6 * b6) + (a7 * b7)

                    d += 8
                }
                var dot = acc0 + acc1 + acc2 + acc3
                while (d < n) {
                    val av = (a[d].toInt() and 0xFF) * inv[d] + mins[d]
                    val bv = (b[d].toInt() and 0xFF) * inv[d] + mins[d]
                    dot += av * bv
                    d++
                }
                dot
            }
            DistanceMetric.MANHATTAN -> {
                var acc0 = 0f; var acc1 = 0f; var acc2 = 0f; var acc3 = 0f
                var d = 0
                while (d < limit) {
                    val diff0 = ((a[d].toInt()     and 0xFF) - (b[d].toInt()     and 0xFF)) * inv[d]
                    val diff1 = ((a[d + 1].toInt() and 0xFF) - (b[d + 1].toInt() and 0xFF)) * inv[d + 1]
                    acc0 += abs(diff0) + abs(diff1)

                    val diff2 = ((a[d + 2].toInt() and 0xFF) - (b[d + 2].toInt() and 0xFF)) * inv[d + 2]
                    val diff3 = ((a[d + 3].toInt() and 0xFF) - (b[d + 3].toInt() and 0xFF)) * inv[d + 3]
                    acc1 += abs(diff2) + abs(diff3)

                    val diff4 = ((a[d + 4].toInt() and 0xFF) - (b[d + 4].toInt() and 0xFF)) * inv[d + 4]
                    val diff5 = ((a[d + 5].toInt() and 0xFF) - (b[d + 5].toInt() and 0xFF)) * inv[d + 5]
                    acc2 += abs(diff4) + abs(diff5)

                    val diff6 = ((a[d + 6].toInt() and 0xFF) - (b[d + 6].toInt() and 0xFF)) * inv[d + 6]
                    val diff7 = ((a[d + 7].toInt() and 0xFF) - (b[d + 7].toInt() and 0xFF)) * inv[d + 7]
                    acc3 += abs(diff6) + abs(diff7)

                    d += 8
                }
                var sum = acc0 + acc1 + acc2 + acc3
                while (d < n) {
                    val diff = ((a[d].toInt() and 0xFF) - (b[d].toInt() and 0xFF)) * inv[d]
                    sum += abs(diff)
                    d++
                }
                -sum
            }
        }
    }

    /**
     * Returns the calibration data needed for serialization.
     */
    fun getCalibration(): Pair<FloatArray, FloatArray> = minBounds.clone() to maxBounds.clone()

    /**
     * Restores calibration from saved data.
     */
    fun loadCalibration(min: FloatArray, max: FloatArray) {
        require(min.size >= dimensions && max.size >= dimensions) {
            "Expected calibration arrays of at least $dimensions elements, got min=${min.size}, max=${max.size}"
        }
        System.arraycopy(min, 0, minBounds, 0, dimensions)
        System.arraycopy(max, 0, maxBounds, 0, dimensions)
        for (d in 0 until dimensions) {
            val range = maxBounds[d] - minBounds[d]
            scales[d] = if (range > 0f && range.isFinite()) 255f / range else 0f
            inverseScales[d] = if (range > 0f && range.isFinite()) range / 255f else 0f
        }
        trained = true
    }

    fun isTrained() = trained
}
