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

/**
 * Interface for converting objects to raw byte arrays and back.
 *
 * Implementing this interface allows KoreDB to store custom types. Common
 * implementations can wrap popular serialization libraries like kotlinx.serialization,
 * Moshi, or Gson.
 *
 * @param T The type of object to be serialized.
 */
interface KoreSerializer<T> {
    /**
     * Converts the given object into a [ByteArray].
     *
     * @param obj The object to serialize.
     * @return The serialized byte representation of the object.
     */
    fun serialize(obj: T): ByteArray

    /**
     * Converts the given [ByteArray] back into an object of type [T].
     *
     * @param bytes The byte array to deserialize.
     * @return The reconstructed object.
     */
    fun deserialize(bytes: ByteArray): T
}
