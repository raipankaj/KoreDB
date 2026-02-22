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

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

/**
 * A [KoreSerializer] implementation that uses Kotlinx Serialization.
 *
 * This serializer converts Kotlin objects to JSON byte arrays and vice-versa.
 * It is suitable for storing data classes in KoreDB.
 *
 * @param T The type of object to be serialized.
 * @property kSerializer The Kotlinx [KSerializer] for type [T].
 * @property jsonConfig The [Json] configuration to use for serialization.
 */
class KotlinxKoreSerializer<T>(
    private val kSerializer: KSerializer<T>,
    private val jsonConfig: Json = Json { ignoreUnknownKeys = true }
) : KoreSerializer<T> {

    override fun serialize(obj: T): ByteArray {
        val jsonString = jsonConfig.encodeToString(kSerializer, obj)
        return jsonString.toByteArray(Charsets.UTF_8)
    }

    override fun deserialize(bytes: ByteArray): T {
        val jsonString = String(bytes, Charsets.UTF_8)
        return jsonConfig.decodeFromString(kSerializer, jsonString)
    }
}
