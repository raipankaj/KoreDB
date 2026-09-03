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
    
    // Per-dimension calibration bounds
    private val minBounds = FloatArray(dimensions) { Float.MAX_VALUE }
    private val maxBounds = FloatArray(dimensions) { Float.MIN_VALUE }
    
    // Pre-computed per-dimension scale factors for fast quantize/dequantize
    private val scales = FloatArray(dimensions)
    private val inverseScales = FloatArray(dimensions)
    
    @Volatile
    private var trained = false
    
    /**
     * Trains the quantizer on a representative sample of vectors.
     *
     * @param vectors A representative sample (at least 100 vectors recommended).
     */
    fun train(vectors: List<FloatArray>) {
        if (vectors.isEmpty()) return
        
        // Pass 1: Find per-dimension min/max
        for (vector in vectors) {
            for (d in 0 until dimensions) {
                if (vector[d] < minBounds[d]) minBounds[d] = vector[d]
                if (vector[d] > maxBounds[d]) maxBounds[d] = vector[d]
            }
        }
        
        // Pass 2: Compute scale factors
        for (d in 0 until dimensions) {
            val range = maxBounds[d] - minBounds[d]
            scales[d] = if (range > 0f) 255f / range else 0f
            inverseScales[d] = if (range > 0f) range / 255f else 0f
        }
        
        trained = true
    }
    
    /**
     * Quantizes a float vector to a compact byte array.
     * Each float is mapped to [0, 255] based on the trained bounds.
     *
     * @param vector The FP32 vector to quantize.
     * @return A byte array of length [dimensions] representing the quantized vector.
     */
    fun quantize(vector: FloatArray): ByteArray {
        val result = ByteArray(dimensions)
        for (d in 0 until dimensions) {
            val normalized = (vector[d] - minBounds[d]) * scales[d]
            result[d] = normalized.coerceIn(0f, 255f).toInt().toByte()
        }
        return result
    }
    
    /**
     * Dequantizes a byte array back to an approximate float vector.
     *
     * @param quantized The quantized byte array.
     * @return An approximate FP32 reconstruction.
     */
    fun dequantize(quantized: ByteArray): FloatArray {
        val result = FloatArray(dimensions)
        for (d in 0 until dimensions) {
            result[d] = (quantized[d].toInt() and 0xFF) * inverseScales[d] + minBounds[d]
        }
        return result
    }
    
    /**
     * High-performance on-the-fly distance calculation directly against SQ8 quantized bytes.
     * Eliminates FloatArray heap allocations on every graph hop.
     *
     * @param query The FP32 query vector.
     * @param quantized The SQ8 quantized vector.
     * @param metric The distance metric to use.
     * @return The approximate similarity score.
     */
    fun computeDistance(query: FloatArray, quantized: ByteArray, metric: DistanceMetric): Float {
        return when (metric) {
            DistanceMetric.COSINE -> {
                var dot = 0f
                var normB = 0f
                var normA = 0f
                for (d in 0 until dimensions) {
                    val q = query[d]
                    val recon = (quantized[d].toInt() and 0xFF) * inverseScales[d] + minBounds[d]
                    dot += q * recon
                    normA += q * q
                    normB += recon * recon
                }
                val div = sqrt(normA) * sqrt(normB)
                if (div == 0f) 0f else dot / div
            }
            DistanceMetric.EUCLIDEAN -> {
                var sumSq = 0f
                for (d in 0 until dimensions) {
                    val recon = (quantized[d].toInt() and 0xFF) * inverseScales[d] + minBounds[d]
                    val diff = query[d] - recon
                    sumSq += diff * diff
                }
                -sqrt(sumSq)
            }
            DistanceMetric.INNER_PRODUCT -> {
                var dot = 0f
                for (d in 0 until dimensions) {
                    val recon = (quantized[d].toInt() and 0xFF) * inverseScales[d] + minBounds[d]
                    dot += query[d] * recon
                }
                dot
            }
            DistanceMetric.MANHATTAN -> {
                var sum = 0f
                for (d in 0 until dimensions) {
                    val recon = (quantized[d].toInt() and 0xFF) * inverseScales[d] + minBounds[d]
                    sum += abs(query[d] - recon)
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
        System.arraycopy(min, 0, minBounds, 0, dimensions)
        System.arraycopy(max, 0, maxBounds, 0, dimensions)
        for (d in 0 until dimensions) {
            val range = maxBounds[d] - minBounds[d]
            scales[d] = if (range > 0f) 255f / range else 0f
            inverseScales[d] = if (range > 0f) range / 255f else 0f
        }
        trained = true
    }
    
    fun isTrained() = trained
}
