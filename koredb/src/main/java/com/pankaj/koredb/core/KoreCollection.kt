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

import com.pankaj.koredb.engine.KoreDB
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onStart

/**
 * Manages a collection of documents of type [T].
 *
 * This class provides methods for CRUD operations, secondary indexing, and
 * reactive observations of document changes.
 *
 * @param T The type of document stored in this collection.
 * @property name The name of the collection.
 * @property db The underlying database engine.
 * @property serializer The serializer used to convert documents to and from bytes.
 */
class KoreCollection<T>(
    val name: String,
    private val db: KoreDB,
    private val serializer: KoreSerializer<T>
) {
    private val updates = MutableSharedFlow<String>(extraBufferCapacity = 100)
    private val indexExtractors = mutableMapOf<String, (T) -> String>()

    /**
     * Registers a secondary index for the collection.
     *
     * @param indexName Unique name for the index.
     * @param extractor Function to extract the indexed value from a document.
     */
    fun createIndex(indexName: String, extractor: (T) -> String) {
        indexExtractors[indexName] = extractor
    }

    private fun makeDocKey(id: String) = "doc:$name:$id".toByteArray(Charsets.UTF_8)
    private fun makeIndexKey(idxName: String, idxValue: String) = "idx:$name:$idxName:$idxValue".toByteArray(Charsets.UTF_8)

    /**
     * Inserts or updates a document.
     *
     * @param id Unique identifier for the document.
     * @param document The document to store.
     */
    suspend fun insert(id: String, document: T) {
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()

        batch.add(Pair(makeDocKey(id), serializer.serialize(document)))

        indexExtractors.forEach { (idxName, extractor) ->
            val extractedValue = extractor(document)
            val idxKey = makeIndexKey(idxName, extractedValue)

            val existingIds = db.getRaw(idxKey)?.let { String(it, Charsets.UTF_8) } ?: ""
            val newList = if (existingIds.isEmpty()) id else "$existingIds,$id"
            batch.add(Pair(idxKey, newList.toByteArray(Charsets.UTF_8)))
        }

        db.writeBatchRaw(batch)
        updates.tryEmit(id)
    }

    /**
     * Deletes a document by its ID.
     *
     * @param id The ID of the document to delete.
     */
    suspend fun delete(id: String) {
        db.deleteRaw(makeDocKey(id))
        updates.tryEmit(id)
    }

    /**
     * Retrieves a document by its ID.
     *
     * @param id The ID of the document to retrieve.
     * @return The deserialized document, or null if not found.
     */
    fun getById(id: String): T? {
        val resultBytes = db.getRaw(makeDocKey(id)) ?: return null
        return serializer.deserialize(resultBytes)
    }

    /**
     * Returns document IDs matching the given prefix.
     * Avoids deserialization cost.
     */
    fun getIdsByPrefix(idPrefix: String): List<String> {
        val prefixBytes = "doc:$name:$idPrefix"
            .toByteArray(Charsets.UTF_8)

        val keys = db.getKeysByPrefixRaw(prefixBytes)

        return keys.map { keyBytes ->
            val fullKey = String(keyBytes, Charsets.UTF_8)
            fullKey.substringAfterLast(":")
        }
    }

    /**
     * Retrieves all documents whose ID starts with the given prefix.
     *
     * This performs an efficient prefix range scan using the underlying
     * LSM-tree sparse index and early termination.
     */
    fun getByIdPrefix(idPrefix: String): List<T> {
        val prefixBytes = "doc:$name:$idPrefix"
            .toByteArray(Charsets.UTF_8)

        val rawResults = db.getByPrefixRaw(prefixBytes)

        return rawResults.map { serializer.deserialize(it) }
    }

    /**
     * Retrieves all documents matching a specific index value.
     *
     * @param indexName The name of the index to query.
     * @param value The value to look for in the index.
     * @return A list of matching documents.
     */
    fun getByIndex(indexName: String, value: String): List<T> {
        val idxKey = makeIndexKey(indexName, value)
        val idsCsv = db.getRaw(idxKey)?.let { String(it, Charsets.UTF_8) } ?: return emptyList()

        return idsCsv.split(",").mapNotNull { getById(it) }
    }

    /**
     * Returns a [Flow] that emits the document whenever it changes.
     *
     * @param id The ID of the document to observe.
     * @return A flow emitting the current state of the document.
     */
    fun observeById(id: String): Flow<T?> = flow {
        updates.collect { updatedId ->
            if (updatedId == id || updatedId == "*") {
                emit(getById(id))
            }
        }
    }.onStart {
        emit(getById(id))
    }

    /**
     * Retrieves all documents in the collection.
     *
     * @return A list of all documents.
     */
    fun getAll(): List<T> {
        val prefix = "doc:$name:".toByteArray(Charsets.UTF_8)
        val rawResults = db.getByPrefixRaw(prefix)
        return rawResults.map { serializer.deserialize(it) }
    }

    /**
     * Returns a [Flow] that emits the full list of documents whenever any change occurs in the collection.
     *
     * @return A flow emitting the current list of all documents.
     */
    fun observeAll(): Flow<List<T>> = flow {
        updates.collect {
            emit(getAll())
        }
    }.onStart {
        emit(getAll())
    }

    /**
     * Performs a batch insert of multiple documents.
     *
     * @param documents A map of IDs to documents.
     */
    suspend fun insertBatch(documents: Map<String, T>) {
        val totalBatch = mutableListOf<Pair<ByteArray, ByteArray>>()

        documents.forEach { (id, document) ->
            totalBatch.add(Pair(makeDocKey(id), serializer.serialize(document)))

            indexExtractors.forEach { (idxName, extractor) ->
                val extractedValue = extractor(document)
                val idxKey = makeIndexKey(idxName, extractedValue)

                val existingIds = db.getRaw(idxKey)?.let { String(it, Charsets.UTF_8) } ?: ""
                val newList = if (existingIds.isEmpty()) id else "$existingIds,$id"
                totalBatch.add(Pair(idxKey, newList.toByteArray(Charsets.UTF_8)))
            }
        }

        db.writeBatchRaw(totalBatch)
        updates.tryEmit("*")
    }

    /**
     * Deletes all documents in the collection and notifies observers.
     */
    suspend fun deleteAll() {
        val prefix = "doc:$name:".toByteArray(Charsets.UTF_8)
        val keysToDelete = db.getKeysByPrefixRaw(prefix)

        if (keysToDelete.isEmpty()) return

        val batch = keysToDelete.map { keyBytes ->
            Pair(keyBytes, KoreDB.TOMBSTONE)
        }

        db.writeBatchRaw(batch)
        updates.tryEmit("*")
    }
}
