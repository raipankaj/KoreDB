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

package com.pankaj.koredb.core

import java.nio.ByteBuffer
import kotlin.math.sqrt

/**
 * Utility object for high-performance vector operations.
 */
object VectorMath {
    
    /**
     * Calculates the magnitude (L2 norm) of a vector.
     *
     * @param vector The vector to calculate the magnitude for.
     * @return The magnitude of the vector.
     */
    fun getMagnitude(vector: FloatArray): Float {
        var norm = 0f
        for (v in vector) norm += v * v
        return sqrt(norm)
    }

    /** 
     * Calculates the Cosine Similarity between a query vector and a vector stored in a [ByteBuffer].
     *
     * This implementation uses loop unrolling to optimize for SIMD instructions (like ARM NEON)
     * and performs calculations directly on the buffer to avoid unnecessary allocations.
     *
     * @param query The query vector.
     * @param queryMagnitude The pre-calculated magnitude of the query vector.
     * @param buffer The buffer containing the vector to compare against.
     * @param offset The starting position of the vector in the buffer.
     * @return The cosine similarity score, ranging from -1.0 to 1.0.
     */
    fun cosineSimilarity(
        query: FloatArray, 
        queryMagnitude: Float, 
        buffer: ByteBuffer, 
        offset: Int
    ): Float {
        var dotProduct = 0f
        var normB = 0f
        val size = query.size

        var i = 0
        // Unroll loop by 4 for SIMD vectorization
        while (i <= size - 4) {
            val q0 = query[i];     val b0 = buffer.getFloat(offset + (i * 4))
            val q1 = query[i+1];   val b1 = buffer.getFloat(offset + ((i+1) * 4))
            val q2 = query[i+2];   val b2 = buffer.getFloat(offset + ((i+2) * 4))
            val q3 = query[i+3];   val b3 = buffer.getFloat(offset + ((i+3) * 4))

            dotProduct += (q0 * b0) + (q1 * b1) + (q2 * b2) + (q3 * b3)
            normB += (b0 * b0) + (b1 * b1) + (b2 * b2) + (b3 * b3)
            i += 4
        }

        // Process remaining elements
        while (i < size) {
            val q = query[i]
            val b = buffer.getFloat(offset + (i * 4))
            dotProduct += q * b
            normB += b * b
            i++
        }

        val divisor = queryMagnitude * sqrt(normB)
        return if (divisor == 0f) 0f else dotProduct / divisor
    }
}
