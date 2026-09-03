package com.pankaj.koredb.hnsw

import com.pankaj.koredb.core.SimdVectorMath
import kotlin.math.sqrt

/**
 * High-Performance Product Quantizer (PQ) with Asymmetric Distance Computation (ADC).
 *
 * Compresses floating-point embedding vectors by up to 32× (e.g., from 512 bytes down to 16 bytes for 128-d vectors)
 * by decomposing the vector space into Cartesian products of lower-dimensional sub-spaces.
 *
 * Features Asymmetric Distance Computation (ADC): distance queries precompute a lookup table (LUT)
 * of sub-vector distances against centroids once, then evaluate nearest neighbors via pure memory table lookups
 * with zero floating-point multiplications.
 */
class ProductQuantizer(
    val dimensions: Int,
    val numSubVectors: Int = minOf(16, maxOf(1, dimensions / 4)),
    val numCentroids: Int = 256
) {
    val subVectorDim: Int = dimensions / numSubVectors

    // Centroids: [subVectorIndex][centroidIndex][subVectorDimension]
    var centroids: Array<Array<FloatArray>> = Array(numSubVectors) {
        Array(numCentroids) { FloatArray(subVectorDim) }
    }
        private set

    @Volatile
    var isTrained: Boolean = false
        private set

    init {
        require(dimensions % numSubVectors == 0) {
            "Dimensions ($dimensions) must be divisible by numSubVectors ($numSubVectors)"
        }
        require(numCentroids in 2..256) {
            "numCentroids must be between 2 and 256 to fit in a single byte"
        }
    }

    /**
     * Trains codebook centroids across each sub-space using K-Means clustering.
     */
    fun train(vectors: List<FloatArray>, maxIterations: Int = 8) {
        if (vectors.isEmpty()) return

        val sampleCount = vectors.size

        for (m in 0 until numSubVectors) {
            val offset = m * subVectorDim
            val subVectors = Array(sampleCount) { i ->
                val v = vectors[i]
                FloatArray(subVectorDim) { d -> v[offset + d] }
            }

            // Initialize centroids from sampled data
            for (k in 0 until numCentroids) {
                val sampleIdx = (k * sampleCount / numCentroids) % sampleCount
                System.arraycopy(subVectors[sampleIdx], 0, centroids[m][k], 0, subVectorDim)
            }

            val assignments = IntArray(sampleCount)
            val counts = IntArray(numCentroids)
            val sums = Array(numCentroids) { FloatArray(subVectorDim) }

            for (iter in 0 until maxIterations) {
                // 1. Assign each sample to nearest centroid
                for (i in 0 until sampleCount) {
                    val sample = subVectors[i]
                    var bestDist = Float.MAX_VALUE
                    var bestK = 0
                    for (k in 0 until numCentroids) {
                        val d = SimdVectorMath.euclideanDistanceSq16(sample, centroids[m][k], subVectorDim)
                        if (d < bestDist) {
                            bestDist = d
                            bestK = k
                        }
                    }
                    assignments[i] = bestK
                }

                // 2. Recompute centroids
                counts.fill(0)
                for (k in 0 until numCentroids) sums[k].fill(0f)

                for (i in 0 until sampleCount) {
                    val k = assignments[i]
                    counts[k]++
                    val sample = subVectors[i]
                    val sumK = sums[k]
                    for (d in 0 until subVectorDim) {
                        sumK[d] += sample[d]
                    }
                }

                for (k in 0 until numCentroids) {
                    val count = counts[k]
                    if (count > 0) {
                        val cK = centroids[m][k]
                        val sumK = sums[k]
                        for (d in 0 until subVectorDim) {
                            cK[d] = sumK[d] / count
                        }
                    }
                }
            }
        }
        isTrained = true
    }

    /**
     * Compresses a high-dimensional vector into a compact [ByteArray] code of size [numSubVectors].
     */
    fun quantize(vector: FloatArray): ByteArray {
        val code = ByteArray(numSubVectors)
        for (m in 0 until numSubVectors) {
            val offset = m * subVectorDim
            val subVector = FloatArray(subVectorDim) { d -> vector[offset + d] }

            var bestDist = Float.MAX_VALUE
            var bestK = 0
            for (k in 0 until numCentroids) {
                val d = SimdVectorMath.euclideanDistanceSq16(subVector, centroids[m][k], subVectorDim)
                if (d < bestDist) {
                    bestDist = d
                    bestK = k
                }
            }
            code[m] = bestK.toByte()
        }
        return code
    }

    /**
     * Reconstructs an approximate full-precision float array from a compressed code.
     */
    fun dequantize(code: ByteArray): FloatArray {
        val result = FloatArray(dimensions)
        for (m in 0 until numSubVectors) {
            val k = code[m].toInt() and 0xFF
            val offset = m * subVectorDim
            System.arraycopy(centroids[m][k], 0, result, offset, subVectorDim)
        }
        return result
    }

    /**
     * Precomputes an Asymmetric Distance Computation (ADC) Lookup Table (LUT) for a query vector.
     * Table dimension: [numSubVectors][numCentroids].
     */
    fun computeAsymmetricDistanceTable(query: FloatArray, metric: DistanceMetric): Array<FloatArray> {
        val table = Array(numSubVectors) { FloatArray(numCentroids) }
        for (m in 0 until numSubVectors) {
            val offset = m * subVectorDim
            val qSub = FloatArray(subVectorDim) { d -> query[offset + d] }
            val tM = table[m]

            when (metric) {
                DistanceMetric.EUCLIDEAN -> {
                    for (k in 0 until numCentroids) {
                        tM[k] = SimdVectorMath.euclideanDistanceSq16(qSub, centroids[m][k], subVectorDim)
                    }
                }
                DistanceMetric.MANHATTAN -> {
                    for (k in 0 until numCentroids) {
                        tM[k] = SimdVectorMath.manhattanDistance16(qSub, centroids[m][k], subVectorDim)
                    }
                }
                DistanceMetric.COSINE, DistanceMetric.INNER_PRODUCT -> {
                    for (k in 0 until numCentroids) {
                        tM[k] = SimdVectorMath.dotProduct16(qSub, centroids[m][k], subVectorDim)
                    }
                }
            }
        }
        return table
    }

    /**
     * Computes similarity distance using precomputed ADC Lookup Table with M array accesses and additions.
     * Follows the KoreDB contract: higher score = closer neighbor.
     */
    fun computeDistanceWithTable(table: Array<FloatArray>, code: ByteArray, metric: DistanceMetric): Float {
        var acc = 0f
        for (m in 0 until numSubVectors) {
            val k = code[m].toInt() and 0xFF
            acc += table[m][k]
        }

        return when (metric) {
            DistanceMetric.EUCLIDEAN -> -sqrt(acc) // Negated so closer points have higher scores
            DistanceMetric.MANHATTAN -> -acc
            DistanceMetric.COSINE, DistanceMetric.INNER_PRODUCT -> acc
        }
    }

    /**
     * Direct distance computation between a raw query vector and a quantized code.
     */
    fun computeDistance(query: FloatArray, code: ByteArray, metric: DistanceMetric): Float {
        val table = computeAsymmetricDistanceTable(query, metric)
        return computeDistanceWithTable(table, code, metric)
    }
}
