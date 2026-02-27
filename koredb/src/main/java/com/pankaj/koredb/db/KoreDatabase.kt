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

package com.pankaj.koredb.db

import com.pankaj.koredb.core.KoreCollection
import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.core.KoreVectorCollection
import com.pankaj.koredb.core.KotlinxKoreSerializer
import com.pankaj.koredb.engine.KoreDB
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * The main entry point for interacting with KoreDB.
 *
 * A [KoreDatabase] instance manages the underlying storage engine and provides
 * access to typed collections and vector collections.
 *
 * @param directory The directory where database files are stored.
 */
class KoreDatabase(directory: File) {

    private val engine = KoreDB(directory)
    private val collections = ConcurrentHashMap<String, KoreCollection<*>>()

    /**
     * Retrieves a [KoreCollection] for the specified type [T].
     *
     * This method automatically resolves the [KSerializer] for the given type.
     *
     * @param T The type of the document. Must be a class annotated with @Serializable.
     * @param name The unique name of the collection.
     * @return A thread-safe collection instance.
     */
    inline fun <reified T : Any> collection(name: String) =
        collection(name, serializer<T>())

    /**
     * Retrieves or creates a [KoreCollection] for the specified type [T] using the provided serializer.
     *
     * @param T The type of the document.
     * @param name The unique name of the collection.
     * @param serializer The [KSerializer] to use for document serialization.
     * @return A thread-safe collection instance.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> collection(
        name: String, 
        serializer: KSerializer<T>
    ): KoreCollection<T> {
        return collections.getOrPut(name) {
            val koreSerializer = KotlinxKoreSerializer(serializer)
            KoreCollection(name, engine, koreSerializer)
        } as KoreCollection<T>
    }

    /**
     * Retrieves or creates a [KoreVectorCollection] for similarity search.
     *
     * @param name The unique name of the vector collection.
     * @return A collection instance capable of performing vector search.
     */
    fun vectorCollection(name: String): KoreVectorCollection {
        return KoreVectorCollection(name, engine)
    }

    /**
     * Closes the database and releases all underlying resources.
     *
     * After calling this method, further operations on the database or its
     * collections may fail or result in undefined behavior.
     */
    fun close() {
        engine.close()
    }

    fun graph(): GraphStorage {
        return GraphStorage(engine) // engine is the KoreDB instance
    }

    fun deleteAllRaw() {
        // 1. Clear the cached collection objects
        collections.clear()

        // 2. Tell the engine to wipe the disk
        engine.nuke()
    }
}
