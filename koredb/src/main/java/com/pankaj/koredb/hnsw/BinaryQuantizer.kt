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

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.PriorityQueue

/**
 * Compact Rotated Binary Quantization (Orthogonal Projection) code representation.
 *
 * @property bits Packed sign bits (1 bit per dimension, packed in 64-bit Longs).
 * @property l2Norm Exact L2 norm of the original vector ||x||_2.
 * @property scaleFactor Normalized L1 norm of the rotated vector: c_x = ||x~||_1 / D.
 */
data class RotatedBinaryCode(
    val bits: LongArray,
    val l2Norm: Float,
    val scaleFactor: Float
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RotatedBinaryCode) return false
        return bits.contentEquals(other.bits) &&
                l2Norm == other.l2Norm &&
                scaleFactor == other.scaleFactor
    }

    override fun hashCode(): Int {
        var result = bits.contentHashCode()
        result = 31 * result + l2Norm.hashCode()
        result = 31 * result + scaleFactor.hashCode()
        return result
    }

    fun packToBytes(): ByteArray {
        val buf = ByteBuffer.allocate(bits.size * 8 + 8).order(ByteOrder.LITTLE_ENDIAN)
        for (w in bits) buf.putLong(w)
        buf.putFloat(l2Norm)
        buf.putFloat(scaleFactor)
        return buf.array()
    }

    companion object {
        fun unpackFromBytes(bytes: ByteArray): RotatedBinaryCode {
            val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
            val bitWords = (bytes.size - 8) ushr 3
            val bits = LongArray(bitWords) { buf.long }
            val l2Norm = buf.float
            val scaleFactor = buf.float
            return RotatedBinaryCode(bits, l2Norm, scaleFactor)
        }
    }
}

/**
 * 1-Bit Binary Quantizer (BQ) & Rotated Binary Quantization for ultra-dense on-device vector search.
 *
 * Compresses 32-bit `FloatArray` embeddings into bit-packed 64-bit `LongArray` words,
 * achieving a **32x memory reduction** (e.g., a 768-dim vector shrinks from 3,072 bytes to 96 bytes)
 * and enabling hardware-accelerated Hamming distance via CPU `POPCNT` / ARM `CNT` instructions
 * (`java.lang.Long.bitCount(a xor b)`).
 *
 * Supports Orthogonal Projection Binary Quantization with deterministic orthogonal rotation $R$,
 * compact [RotatedBinaryCode], and unbiased asymmetric/symmetric distance estimators.
 *
 * @property dimensions Vector dimensionality $D$.
 * @property enableOrthogonalRotation Whether to enable orthogonal rotation for variance equalization and unbiased estimation.
 * @property randomSeed Seed for deterministic orthogonal matrix generation.
 */
class BinaryQuantizer(
    val dimensions: Int,
    val enableOrthogonalRotation: Boolean = true,
    val randomSeed: Long = 42L
) {

    init {
        require(dimensions > 0) { "Dimensions must be positive, got $dimensions" }
    }

    /** Number of 64-bit `Long` words required to store [dimensions] bits. */
    val wordCount: Int = (dimensions + 63) ushr 6

    /** Orthogonal rotation matrix for variance equalization and norm preservation. */
    val rotationMatrix: Array<FloatArray> = if (enableOrthogonalRotation) {
        Companion.generateOrthogonalMatrix(dimensions, randomSeed)
    } else {
        emptyArray()
    }

    /** Optional per-dimension thresholds learned via [train]. Defaults to `null` (0.0f sign threshold). */
    @Volatile
    var thresholds: FloatArray? = null
        private set

    val isTrained: Boolean
        get() = thresholds != null

    /**
     * Calibrates per-dimension mean thresholds across a sample of embeddings.
     * Useful for non-zero-centered embedding models (e.g., ReLU-activated encoders).
     */
    fun train(vectors: Collection<FloatArray>) {
        if (vectors.isEmpty()) return
        val means = FloatArray(dimensions)
        var count = 0
        for (vec in vectors) {
            require(vec.size == dimensions) { "Expected $dimensions dimensions, got ${vec.size}" }
            for (d in 0 until dimensions) {
                means[d] += vec[d]
            }
            count++
        }
        val invCount = 1.0f / count
        for (d in 0 until dimensions) {
            means[d] *= invCount
        }
        thresholds = means
    }

    /**
     * Quantizes a `FloatArray` of size [dimensions] into a bit-packed `LongArray` of size [wordCount].
     *
     * Each dimension $d$ sets bit $(d \bmod 64)$ of word $(d / 64)$ to `1` if `vector[d] > threshold[d]`,
     * or `0` otherwise.
     */
    fun quantize(vector: FloatArray): LongArray {
        require(vector.size == dimensions) { "Expected $dimensions dimensions, got ${vector.size}" }
        val packed = LongArray(wordCount)
        val thresh = thresholds

        if (thresh == null) {
            for (d in 0 until dimensions) {
                if (vector[d] > 0.0f) {
                    val wordIdx = d ushr 6
                    val bitIdx = d and 63
                    packed[wordIdx] = packed[wordIdx] or (1L shl bitIdx)
                }
            }
        } else {
            for (d in 0 until dimensions) {
                if (vector[d] > thresh[d]) {
                    val wordIdx = d ushr 6
                    val bitIdx = d and 63
                    packed[wordIdx] = packed[wordIdx] or (1L shl bitIdx)
                }
            }
        }
        return packed
    }

    /**
     * Computes the exact Hamming distance (number of differing bits) between two bit-packed vectors
     * using 4x loop unrolling and hardware population count (`Long.bitCount`).
     */
    fun hammingDistance(a: LongArray, b: LongArray): Int {
        var dist = 0
        var i = 0
        val n = wordCount
        val limit = n - 3
        while (i < limit) {
            dist += java.lang.Long.bitCount(a[i] xor b[i]) +
                    java.lang.Long.bitCount(a[i + 1] xor b[i + 1]) +
                    java.lang.Long.bitCount(a[i + 2] xor b[i + 2]) +
                    java.lang.Long.bitCount(a[i + 3] xor b[i + 3])
            i += 4
        }
        while (i < n) {
            dist += java.lang.Long.bitCount(a[i] xor b[i])
            i++
        }
        return dist
    }

    /**
     * Computes normalized Hamming similarity in $[0.0, 1.0]$, where $1.0$ means identical binary codes.
     */
    fun hammingSimilarity(a: LongArray, b: LongArray): Float {
        val dist = hammingDistance(a, b)
        return 1.0f - (dist.toFloat() / dimensions.toFloat())
    }

    /**
     * Executes a Two-Stage Binary Quantized Search:
     * 1. **Coarse Stage (1-Bit Hamming)**: Scans [binaryIndex] using hardware `POPCNT` to find the top
     *    `limit * oversampleFactor` candidates.
     * 2. **Fine Stage (Exact Rescoring)**: Rescores only those shortlisted candidates using [exactVectorProvider]
     *    and [metric], returning the top [limit] results sorted by exact similarity descending.
     *
     * @param query Full-precision query vector.
     * @param binaryIndex Map or entries of `(id -> bit-packed LongArray)`.
     * @param limit Final number of results to return.
     * @param oversampleFactor Candidate multiplier for Stage 1 (default 4, i.e., $4 \times \text{limit}$).
     * @param metric Exact distance metric for Stage 2 rescoring (default [DistanceMetric.COSINE]).
     * @param exactVectorProvider Callback to fetch the full-precision `FloatArray` for a candidate ID.
     */
    fun searchTwoStage(
        query: FloatArray,
        binaryIndex: Map<String, LongArray>,
        limit: Int = 10,
        oversampleFactor: Int = 4,
        metric: DistanceMetric = DistanceMetric.COSINE,
        exactVectorProvider: (String) -> FloatArray?
    ): List<Pair<String, Float>> {
        if (binaryIndex.isEmpty() || limit <= 0) return emptyList()
        val queryBits = quantize(query)
        val stage1Limit = (limit * oversampleFactor.coerceAtLeast(1)).coerceAtMost(binaryIndex.size)

        // Min-heap by similarity (which corresponds to max allowed Hamming distance at top of heap)
        val topCoarse = PriorityQueue<Pair<String, Int>>(stage1Limit + 1) { x, y ->
            // Larger Hamming distance = worse candidate, so put largest distance at head of heap
            y.second.compareTo(x.second)
        }

        for ((id, code) in binaryIndex) {
            val hDist = hammingDistance(queryBits, code)
            if (topCoarse.size < stage1Limit) {
                topCoarse.add(id to hDist)
            } else if (hDist < topCoarse.peek()!!.second) {
                topCoarse.poll()
                topCoarse.add(id to hDist)
            }
        }

        // Stage 2: Exact rescoring on shortlisted candidates
        val rescored = ArrayList<Pair<String, Float>>(topCoarse.size)
        for ((id, hDist) in topCoarse) {
            val exactVec = exactVectorProvider(id)
            val score = if (exactVec != null) {
                metric.compute(query, exactVec)
            } else {
                1.0f - (hDist.toFloat() / dimensions.toFloat())
            }
            rescored.add(id to score)
        }

        rescored.sortByDescending { it.second }
        return if (rescored.size <= limit) rescored else rescored.subList(0, limit)
    }

    /**
     * Projects vector onto the deterministic orthogonal matrix R: x~ = R * x.
     * Preserves L2 norm and pairwise inner products while equalizing coordinate variance.
     */
    fun rotate(vector: FloatArray): FloatArray {
        require(vector.size == dimensions) { "Expected $dimensions dimensions, got ${vector.size}" }
        if (!enableOrthogonalRotation || rotationMatrix.isEmpty()) return vector
        val out = FloatArray(dimensions)
        for (i in 0 until dimensions) {
            out[i] = com.pankaj.koredb.core.SimdVectorMath.dotProduct16(rotationMatrix[i], vector, dimensions)
        }
        return out
    }

    /**
     * Quantizes an FP32 vector using orthogonal projection into a compact [RotatedBinaryCode].
     */
    fun quantizeRotated(vector: FloatArray): RotatedBinaryCode {
        require(vector.size == dimensions) { "Expected $dimensions dimensions, got ${vector.size}" }

        val rotated = if (enableOrthogonalRotation) rotate(vector) else vector
        val l2Norm = com.pankaj.koredb.core.VectorMath.getMagnitude(vector)

        val bits = LongArray(wordCount)
        var l1Sum = 0.0f

        for (d in 0 until dimensions) {
            val v = rotated[d]
            l1Sum += kotlin.math.abs(v)
            if (v >= 0.0f) {
                val wordIdx = d ushr 6
                val bitIdx = d and 63
                bits[wordIdx] = bits[wordIdx] or (1L shl bitIdx)
            }
        }

        val scaleFactor = l1Sum / dimensions.toFloat()
        return RotatedBinaryCode(bits, l2Norm, scaleFactor)
    }

    /**
     * Unbiased Asymmetric Inner Product estimation between full-precision query and [RotatedBinaryCode]:
     * <q, x>^ = c_x * <q~, b(x~)> * sqrt(PI / 2)
     */
    fun estimateInnerProductAsymmetric(query: FloatArray, code: RotatedBinaryCode): Float {
        require(query.size == dimensions) { "Expected $dimensions dimensions, got ${query.size}" }
        val qRot = if (enableOrthogonalRotation) rotate(query) else query
        var sumQOnes = 0.0f
        var sumQTotal = 0.0f

        for (d in 0 until dimensions) {
            val qd = qRot[d]
            sumQTotal += qd
            val bit = (code.bits[d ushr 6] ushr (d and 63)) and 1L
            if (bit == 1L) {
                sumQOnes += qd
            }
        }

        val dotSign = (2.0f * sumQOnes) - sumQTotal
        val gaussianFactor = kotlin.math.sqrt(Math.PI / 2.0).toFloat()
        return code.scaleFactor * dotSign * gaussianFactor
    }

    /**
     * Unbiased Asymmetric Cosine Similarity estimation between full-precision query and [RotatedBinaryCode].
     */
    fun estimateCosineAsymmetric(query: FloatArray, code: RotatedBinaryCode): Float {
        val qNorm = com.pankaj.koredb.core.VectorMath.getMagnitude(query)
        val denom = qNorm * code.l2Norm
        if (denom <= 0f) return 0f
        val estIp = estimateInnerProductAsymmetric(query, code)
        return (estIp / denom).coerceIn(-1.0f, 1.0f)
    }

    /**
     * Unbiased Asymmetric Euclidean Distance estimation between full-precision query and [RotatedBinaryCode].
     */
    fun estimateEuclideanAsymmetric(query: FloatArray, code: RotatedBinaryCode): Float {
        val qNorm = com.pankaj.koredb.core.VectorMath.getMagnitude(query)
        val estIp = estimateInnerProductAsymmetric(query, code)
        val sqDist = (qNorm * qNorm) + (code.l2Norm * code.l2Norm) - (2.0f * estIp)
        return kotlin.math.sqrt(sqDist.coerceAtLeast(0.0f))
    }

    /**
     * Unbiased Symmetric Inner Product estimation between two [RotatedBinaryCode]s using POPCNT:
     * <q, x>^ = c_q * c_x * (PI / 2) * (D - 2 * d_H)
     */
    fun estimateInnerProductSymmetric(qCode: RotatedBinaryCode, xCode: RotatedBinaryCode): Float {
        val hDist = hammingDistance(qCode.bits, xCode.bits)
        val innerSign = dimensions - (2 * hDist)
        val factor = (Math.PI / 2.0).toFloat()
        return qCode.scaleFactor * xCode.scaleFactor * factor * innerSign
    }

    /**
     * Unbiased Symmetric Cosine Similarity estimation between two [RotatedBinaryCode]s using POPCNT.
     */
    fun estimateCosineSymmetric(qCode: RotatedBinaryCode, xCode: RotatedBinaryCode): Float {
        val denom = qCode.l2Norm * xCode.l2Norm
        if (denom <= 0f) return 0f
        val estIp = estimateInnerProductSymmetric(qCode, xCode)
        return (estIp / denom).coerceIn(-1.0f, 1.0f)
    }

    /**
     * Unbiased Symmetric Euclidean Distance estimation between two [RotatedBinaryCode]s using POPCNT.
     */
    fun estimateEuclideanSymmetric(qCode: RotatedBinaryCode, xCode: RotatedBinaryCode): Float {
        val estIp = estimateInnerProductSymmetric(qCode, xCode)
        val sqDist = (qCode.l2Norm * qCode.l2Norm) + (xCode.l2Norm * xCode.l2Norm) - (2.0f * estIp)
        return kotlin.math.sqrt(sqDist.coerceAtLeast(0.0f))
    }

    /**
     * Two-stage search using rotated 1-bit POPCNT estimators for Stage 1 candidate retrieval,
     * followed by Stage 2 exact rescoring.
     */
    fun searchTwoStageRotated(
        query: FloatArray,
        index: Map<String, RotatedBinaryCode>,
        limit: Int = 10,
        oversampleFactor: Int = 4,
        metric: DistanceMetric = DistanceMetric.COSINE,
        exactVectorProvider: (String) -> FloatArray?
    ): List<Pair<String, Float>> {
        if (index.isEmpty() || limit <= 0) return emptyList()
        val queryCode = quantizeRotated(query)
        val stage1Limit = (limit * oversampleFactor.coerceAtLeast(1)).coerceAtMost(index.size)

        val topCoarse = PriorityQueue<Pair<String, Float>>(stage1Limit + 1) { a, b ->
            a.second.compareTo(b.second)
        }

        for ((id, code) in index) {
            val score = when (metric) {
                DistanceMetric.COSINE -> (dimensions - (2 * hammingDistance(queryCode.bits, code.bits))).toFloat()
                DistanceMetric.INNER_PRODUCT -> (dimensions - (2 * hammingDistance(queryCode.bits, code.bits))) * code.l2Norm
                DistanceMetric.EUCLIDEAN, DistanceMetric.MANHATTAN -> -estimateEuclideanSymmetric(queryCode, code)
            }
            if (topCoarse.size < stage1Limit) {
                topCoarse.add(id to score)
            } else if (score > topCoarse.peek()!!.second) {
                topCoarse.poll()
                topCoarse.add(id to score)
            }
        }

        val rescored = ArrayList<Pair<String, Float>>(topCoarse.size)
        for ((id, coarseScore) in topCoarse) {
            val exactVec = exactVectorProvider(id)
            val score = if (exactVec != null) {
                metric.compute(query, exactVec)
            } else {
                coarseScore
            }
            rescored.add(id to score)
        }

        rescored.sortByDescending { it.second }
        return if (rescored.size <= limit) rescored else rescored.subList(0, limit)
    }

    /** Packs a `LongArray` binary code into a compact Little-Endian `ByteArray`. */
    fun packToBytes(code: LongArray): ByteArray {
        val buf = ByteBuffer.allocate(code.size * 8).order(ByteOrder.LITTLE_ENDIAN)
        for (word in code) {
            buf.putLong(word)
        }
        return buf.array()
    }

    /** Unpacks a Little-Endian `ByteArray` back into a `LongArray` binary code. */
    fun unpackFromBytes(bytes: ByteArray): LongArray {
        val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        return LongArray(bytes.size ushr 3) { buf.long }
    }

    /** Serializes the quantizer configuration and optional learned thresholds. */
    fun toByteArray(): ByteArray {
        val thresh = thresholds
        val hasThresholds = if (thresh != null) 1.toByte() else 0.toByte()
        val size = 4 + 1 + (if (thresh != null) dimensions * 4 else 0)
        val buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)
        buf.putInt(dimensions)
        buf.put(hasThresholds)
        if (thresh != null) {
            for (v in thresh) buf.putFloat(v)
        }
        return buf.array()
    }

    companion object {
        /**
         * Deterministically generates a DxD orthogonal projection matrix via Gram-Schmidt.
         */
        fun generateOrthogonalMatrix(dim: Int, seed: Long = 42L): Array<FloatArray> {
            val random = java.util.Random(seed)
            val matrix = Array(dim) { FloatArray(dim) }

            // 1. Fill with Gaussian entries
            for (i in 0 until dim) {
                for (j in 0 until dim) {
                    matrix[i][j] = random.nextGaussian().toFloat()
                }
            }

            // 2. Gram-Schmidt orthogonalization
            for (i in 0 until dim) {
                for (j in 0 until i) {
                    var dot = 0.0f
                    for (k in 0 until dim) {
                        dot += matrix[i][k] * matrix[j][k]
                    }
                    for (k in 0 until dim) {
                        matrix[i][k] -= dot * matrix[j][k]
                    }
                }
                var norm = 0.0f
                for (k in 0 until dim) {
                    norm += matrix[i][k] * matrix[i][k]
                }
                norm = kotlin.math.sqrt(kotlin.math.max(1e-12f, norm))
                for (k in 0 until dim) {
                    matrix[i][k] /= norm
                }
            }
            return matrix
        }

        fun fromByteArray(bytes: ByteArray): BinaryQuantizer {
            val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
            val dims = buf.int
            val hasThresholds = buf.get() == 1.toByte()
            val bq = BinaryQuantizer(dims)
            if (hasThresholds) {
                bq.thresholds = FloatArray(dims) { buf.float }
            }
            return bq
        }
    }
}
