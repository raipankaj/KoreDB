/*
 * Copyright 2024 KoreDB Authors
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

/**
 * Utility for converting [FloatArray] vectors to [ByteArray] and back.
 */
object VectorSerializer {
    
    /**
     * Converts a [FloatArray] to a [ByteArray].
     *
     * @param vector The vector to serialize.
     * @return The byte representation of the vector.
     */
    fun toByteArray(vector: FloatArray): ByteArray {
        val buffer = ByteBuffer.allocate(vector.size * 4) 
        for (f in vector) buffer.putFloat(f)
        return buffer.array()
    }

    /**
     * Reconstructs a [FloatArray] from a [ByteArray].
     *
     * @param bytes The byte array to deserialize.
     * @return The reconstructed vector.
     */
    fun fromByteArray(bytes: ByteArray): FloatArray {
        val buffer = ByteBuffer.wrap(bytes)
        val result = FloatArray(bytes.size / 4)
        for (i in result.indices) result[i] = buffer.getFloat()
        return result
    }
}
