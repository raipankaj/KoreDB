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
            val magA = com.pankaj.koredb.core.SimdVectorMath.getMagnitude16(a)
            val magB = com.pankaj.koredb.core.SimdVectorMath.getMagnitude16(b)
            return com.pankaj.koredb.core.SimdVectorMath.cosineSimilarity16(a, magA, b, magB)
        }

        override fun computeWithMagnitudes(a: FloatArray, magA: Float, b: FloatArray, magB: Float): Float {
            return com.pankaj.koredb.core.SimdVectorMath.cosineSimilarity16(a, magA, b, magB)
        }
    },

    /**
     * Euclidean (L2) Distance: straight-line distance between points.
     * Returned as negative so that higher = closer (consistent with HNSW max-heap).
     * Best for classification and clustering tasks.
     */
    EUCLIDEAN {
        override fun compute(a: FloatArray, b: FloatArray): Float {
            val sq = com.pankaj.koredb.core.SimdVectorMath.euclideanDistanceSq16(a, b)
            return -sqrt(sq) // Negate: higher = closer
        }
    },

    /**
     * Inner Product (Dot Product): raw projection of one vector onto another.
     * Range: unbounded. Best for recommendation systems where magnitude matters.
     */
    INNER_PRODUCT {
        override fun compute(a: FloatArray, b: FloatArray): Float {
            return com.pankaj.koredb.core.SimdVectorMath.dotProduct16(a, b)
        }
    },

    /**
     * Manhattan (L1) Distance: sum of absolute differences.
     * Returned as negative for consistent ordering.
     * Best for high-dimensional sparse data.
     */
    MANHATTAN {
        override fun compute(a: FloatArray, b: FloatArray): Float {
            val sum = com.pankaj.koredb.core.SimdVectorMath.manhattanDistance16(a, b)
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
