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
     * Optimized dot product that leverages sequential ByteBuffer reads.
     */
    fun dotProduct(
        query: FloatArray,
        buffer: ByteBuffer,
        offset: Int,
        size: Int
    ): Float {
        var dot = 0f
        var i = 0
        // Use absolute indexing (getFloat(index)) to avoid internal position state updates.
        // This is significantly faster in tight loops on the Android Runtime (ART).
        while (i <= size - 4) {
            dot += (query[i] * buffer.getFloat(offset + (i * 4))) +
                   (query[i + 1] * buffer.getFloat(offset + (i + 1) * 4)) +
                   (query[i + 2] * buffer.getFloat(offset + (i + 2) * 4)) +
                   (query[i + 3] * buffer.getFloat(offset + (i + 3) * 4))
            i += 4
        }
        while (i < size) {
            dot += query[i] * buffer.getFloat(offset + (i * 4))
            i++
        }
        return dot
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
     * @param storedVectorLength The number of float elements in the stored vector.
     * @return The cosine similarity score (-1.0 to 1.0), or -2.0f if dimensions mismatch.
     */
    fun cosineSimilarity(
        query: FloatArray, 
        queryMagnitude: Float, 
        buffer: ByteBuffer, 
        offset: Int,
        storedVectorLength: Int
    ): Float {
        if (query.size != storedVectorLength) {
            return -2.0f
        }

        var dotProduct = 0f
        var normB = 0f
        val size = query.size

        // Set position to avoid indexed lookups (faster on some JVMs)
        buffer.position(offset)
        
        var i = 0
        // Unroll loop by 4 for SIMD vectorization
        while (i <= size - 4) {
            val q0 = query[i];     val b0 = buffer.getFloat()
            val q1 = query[i+1];   val b1 = buffer.getFloat()
            val q2 = query[i+2];   val b2 = buffer.getFloat()
            val q3 = query[i+3];   val b3 = buffer.getFloat()

            dotProduct += (q0 * b0) + (q1 * b1) + (q2 * b2) + (q3 * b3)
            normB += (b0 * b0) + (b1 * b1) + (b2 * b2) + (b3 * b3)
            i += 4
        }

        // Process remaining elements
        while (i < size) {
            val q = query[i]
            val b = buffer.getFloat()
            dotProduct += q * b
            normB += b * b
            i++
        }

        val divisor = queryMagnitude * sqrt(normB)
        return if (divisor == 0f) 0f else dotProduct / divisor
    }
}
