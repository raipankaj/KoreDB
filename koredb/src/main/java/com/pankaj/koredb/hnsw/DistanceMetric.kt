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
 * Supported distance/similarity metrics for vector search.
 *
 * Each metric defines how "closeness" is computed between two vectors.
 * The [compute] method returns a score where **higher = more similar**
 * for all metrics (Euclidean is negated internally so the HNSW max-heap
 * ordering is consistent).
 *
 * Usage:
 * ```kotlin
 * val collection = db.vectorCollection("products", metric = DistanceMetric.COSINE)
 * ```
 */
enum class DistanceMetric {

    /**
     * Cosine Similarity: measures the angle between two vectors.
     * Range: [-1.0, 1.0]. Best for normalized text/image embeddings.
     */
    COSINE {
        override fun compute(a: FloatArray, b: FloatArray): Float {
            var dot = 0f; var normA = 0f; var normB = 0f
            var i = 0
            val size = a.size
            // 4x loop unrolling for SIMD vectorization
            while (i <= size - 4) {
                dot += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
                normA += a[i]*a[i] + a[i+1]*a[i+1] + a[i+2]*a[i+2] + a[i+3]*a[i+3]
                normB += b[i]*b[i] + b[i+1]*b[i+1] + b[i+2]*b[i+2] + b[i+3]*b[i+3]
                i += 4
            }
            while (i < size) {
                dot += a[i]*b[i]; normA += a[i]*a[i]; normB += b[i]*b[i]; i++
            }
            val divisor = sqrt(normA) * sqrt(normB)
            return if (divisor == 0f) 0f else dot / divisor
        }

        override fun computeWithMagnitudes(a: FloatArray, magA: Float, b: FloatArray, magB: Float): Float {
            var dot = 0f
            var i = 0; val size = a.size
            while (i <= size - 4) {
                dot += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]; i += 4
            }
            while (i < size) { dot += a[i]*b[i]; i++ }
            val divisor = magA * magB
            return if (divisor == 0f) 0f else dot / divisor
        }
    },

    /**
     * Euclidean (L2) Distance: straight-line distance between points.
     * Returned as negative so that higher = closer (consistent with HNSW max-heap).
     * Best for classification and clustering tasks.
     */
    EUCLIDEAN {
        override fun compute(a: FloatArray, b: FloatArray): Float {
            var sum = 0f
            var i = 0; val size = a.size
            while (i <= size - 4) {
                val d0 = a[i]-b[i]; val d1 = a[i+1]-b[i+1]
                val d2 = a[i+2]-b[i+2]; val d3 = a[i+3]-b[i+3]
                sum += d0*d0 + d1*d1 + d2*d2 + d3*d3; i += 4
            }
            while (i < size) { val d = a[i]-b[i]; sum += d*d; i++ }
            return -sqrt(sum) // Negate: higher = closer
        }
    },

    /**
     * Inner Product (Dot Product): raw projection of one vector onto another.
     * Range: unbounded. Best for recommendation systems where magnitude matters.
     */
    INNER_PRODUCT {
        override fun compute(a: FloatArray, b: FloatArray): Float {
            var dot = 0f
            var i = 0; val size = a.size
            while (i <= size - 4) {
                dot += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]; i += 4
            }
            while (i < size) { dot += a[i]*b[i]; i++ }
            return dot
        }
    },

    /**
     * Manhattan (L1) Distance: sum of absolute differences.
     * Returned as negative for consistent ordering.
     * Best for high-dimensional sparse data.
     */
    MANHATTAN {
        override fun compute(a: FloatArray, b: FloatArray): Float {
            var sum = 0f
            var i = 0; val size = a.size
            while (i <= size - 4) {
                sum += abs(a[i]-b[i]) + abs(a[i+1]-b[i+1]) + abs(a[i+2]-b[i+2]) + abs(a[i+3]-b[i+3])
                i += 4
            }
            while (i < size) { sum += abs(a[i]-b[i]); i++ }
            return -sum // Negate: higher = closer
        }
    };

    /**
     * Computes the similarity/distance between two vectors.
     * Higher return value = more similar (for all metrics).
     */
    abstract fun compute(a: FloatArray, b: FloatArray): Float

    /**
     * Optimized computation with pre-calculated magnitudes.
     * Default implementation ignores magnitudes and delegates to [compute].
     * Overridden by COSINE for O(N) instead of O(3N).
     */
    open fun computeWithMagnitudes(a: FloatArray, magA: Float, b: FloatArray, magB: Float): Float {
        return compute(a, b)
    }
}
