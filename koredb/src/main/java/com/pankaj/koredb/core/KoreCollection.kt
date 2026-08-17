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
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onStart
import java.util.LinkedHashMap

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
    val serializer: KoreSerializer<T>
) {
    private val updates = MutableSharedFlow<String>(extraBufferCapacity = 100)
    private val indexExtractors = mutableMapOf<String, (T) -> String>()
    private val searchableExtractors = mutableListOf<(T) -> String>()
    val ftsIndex: com.pankaj.koredb.fts.FtsIndex = com.pankaj.koredb.fts.FtsIndex(name, db)

    // Object Cache to bypass JSON deserialization for frequent queries
    // Size increased to 65536 to handle large prefix/range bulk reads smoothly without thrashing
    private val documentCache = object : LinkedHashMap<String, T>(8192, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, T>): Boolean {
            return size > 65536
        }
    }

    private fun escape(value: String): String {
        return value.replace("%", "%25").replace(":", "%3A")
    }

    private fun unescape(value: String): String {
        return value.replace("%3A", ":").replace("%25", "%")
    }

    /**
     * Registers a secondary index for the collection.
     *
     * @param indexName Unique name for the index.
     * @param extractor Function to extract the indexed value from a document.
     */
    fun createIndex(indexName: String, extractor: (T) -> String) {
        indexExtractors[indexName] = extractor
    }

    /**
     * Registers text field extractors for Okapi BM25 Full-Text Search indexing.
     *
     * ```kotlin
     * collection.searchableFields({ it.title }, { it.content })
     * ```
     */
    fun searchableFields(vararg extractors: (T) -> String) {
        searchableExtractors.addAll(extractors)
    }

    private val docPrefix = "doc:$name:".toByteArray(Charsets.UTF_8)
    private val idxPrefix = "idx:$name:".toByteArray(Charsets.UTF_8)
    private val rptrPrefix = "rptr:$name:".toByteArray(Charsets.UTF_8)

    // Cache for index name bytes to avoid repeated UTF-8 encoding
    private val indexNameCache = mutableMapOf<String, ByteArray>()

    private fun getIndexNameBytes(name: String): ByteArray {
        return indexNameCache.getOrPut(name) { name.toByteArray(Charsets.UTF_8) }
    }

    private fun makeDocKey(idBytes: ByteArray): ByteArray {
        val result = ByteArray(docPrefix.size + idBytes.size)
        System.arraycopy(docPrefix, 0, result, 0, docPrefix.size)
        System.arraycopy(idBytes, 0, result, docPrefix.size, idBytes.size)
        return result
    }

    /**
     * A non-empty byte array used to indicate entry existence in secondary indices.
     */
    private val PRESENCE_MARKER = ByteArray(1) { 1 }

    /**
     * Inserts or updates a document.
     *
     * @param id Unique identifier for the document.
     * @param document The document to store.
     */
    suspend fun insert(id: String, document: T) = coroutineScope {
        val idBytes = escape(id).toByteArray(Charsets.UTF_8)
        val docBytes = serializer.serialize(document)
        val docKey = makeDocKey(idBytes)

        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        batch.add(docKey to docBytes)

        // Index for Full-Text Search if configured
        if (searchableExtractors.isNotEmpty()) {
            val combinedText = searchableExtractors.joinToString(" ") { it(document) }
            ftsIndex.indexDocument(id, combinedText, batch)
        }

        indexExtractors.forEach { (idxName, extractor) ->
            val value = extractor(document)
            val valBytes = escape(value).toByteArray(Charsets.UTF_8)
            val idxNameBytes = getIndexNameBytes(idxName)

            // sidx: idx:$name:$idxName:$idxValue:$id
            val idxKey = buildKey(idxPrefix, idxNameBytes, valBytes, idBytes)
            // rptr: rptr:$name:$idxName:$id
            val rptrKey = buildKey(rptrPrefix, idxNameBytes, idBytes)

            batch.add(idxKey to PRESENCE_MARKER)
            batch.add(rptrKey to valBytes)
        }

        db.writeBatchRaw(batch)
        synchronized(documentCache) {
            documentCache[id] = document
        }
        updates.tryEmit(id)
    }

    private fun buildKey(prefix: ByteArray, vararg parts: ByteArray): ByteArray {
        var partsSize = 0
        for (part in parts) partsSize += part.size
        val totalSize = prefix.size + partsSize + parts.size

        val result = ByteArray(totalSize)
        System.arraycopy(prefix, 0, result, 0, prefix.size)
        var pos = prefix.size
        for (part in parts) {
            System.arraycopy(part, 0, result, pos, part.size)
            pos += part.size
            if (pos < totalSize) {
                result[pos] = ':'.code.toByte()
                pos++
            }
        }
        return result
    }

    /**
     * Deletes a document by its ID.
     *
     * @param id The ID of the document to delete.
     */
    suspend fun delete(id: String) {
        val idBytes = escape(id).toByteArray(Charsets.UTF_8)
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        batch.add(makeDocKey(idBytes) to KoreDB.TOMBSTONE)

        if (searchableExtractors.isNotEmpty()) {
            ftsIndex.removeDocument(id, batch)
        }

        indexExtractors.forEach { (idxName, _) ->
            val idxNameBytes = getIndexNameBytes(idxName)
            val rptrKey = buildKey(rptrPrefix, idxNameBytes, idBytes)
            val oldValBytes = db.getRaw(rptrKey)
            if (oldValBytes != null) {
                val idxKey = buildKey(idxPrefix, idxNameBytes, oldValBytes, idBytes)
                batch.add(idxKey to KoreDB.TOMBSTONE)
                batch.add(rptrKey to KoreDB.TOMBSTONE)
            }
        }

        db.writeBatchRaw(batch)
        synchronized(documentCache) {
            documentCache.remove(id)
        }
        updates.tryEmit(id)
    }

    /**
     * Retrieves a document by its ID.
     *
     * @param id The ID of the document to retrieve.
     * @return The deserialized document, or null if not found.
     */
    fun getById(id: String): T? {
        synchronized(documentCache) {
            val cached = documentCache[id]
            if (cached != null) return cached
        }

        val resultBytes = db.getRaw(makeDocKey(escape(id).toByteArray(Charsets.UTF_8))) ?: return null
        val doc = serializer.deserialize(resultBytes)

        synchronized(documentCache) {
            documentCache[id] = doc
        }

        return doc
    }

    /**
     * Returns document IDs matching the given prefix.
     * Avoids deserialization cost.
     */
    fun getIdsByPrefix(idPrefix: String): List<String> {
        val prefixBytes = makeDocKey(escape(idPrefix).toByteArray(Charsets.UTF_8))
        val keys = db.getKeysByPrefixRaw(prefixBytes)

        return keys.map { keyBytes ->
            val escapedId = String(keyBytes, docPrefix.size, keyBytes.size - docPrefix.size, Charsets.UTF_8)
            unescape(escapedId)
        }
    }

    /**
     * Retrieves all documents whose ID falls within the range [startId, endId).
     *
     * This performs an efficient range scan using the underlying LSM-tree
     * sparse index and early termination.
     */
    fun getByIdRange(startId: String, endId: String): List<T> {
        val startKey = makeDocKey(escape(startId).toByteArray(Charsets.UTF_8))
        val endKey = makeDocKey(escape(endId).toByteArray(Charsets.UTF_8))

        val rawResults = db.getRangeWithKeysRaw(startKey, endKey)

        return rawResults.map { (keyBytes, valueBytes) ->
            val escapedId = String(keyBytes, docPrefix.size, keyBytes.size - docPrefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)
            
            var doc: T? = null
            synchronized(documentCache) {
                doc = documentCache[id]
            }

            if (doc == null) {
                doc = serializer.deserialize(valueBytes)
                synchronized(documentCache) {
                    documentCache[id] = doc!!
                }
            }
            doc!!
        }
    }

    /**
     * Retrieves all documents whose ID starts with the given prefix.
     *
     * This performs an efficient prefix range scan using the underlying
     * LSM-tree sparse index and early termination.
     */
    fun getByIdPrefix(idPrefix: String): List<T> {
        val prefixBytes = makeDocKey(escape(idPrefix).toByteArray(Charsets.UTF_8))
        val rawResults = db.getByPrefixWithKeysRaw(prefixBytes)

        return rawResults.map { (keyBytes, valueBytes) ->
            val escapedId = String(keyBytes, docPrefix.size, keyBytes.size - docPrefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)
            
            var doc: T? = null
            synchronized(documentCache) {
                doc = documentCache[id]
            }

            if (doc == null) {
                doc = serializer.deserialize(valueBytes)
                synchronized(documentCache) {
                    documentCache[id] = doc!!
                }
            }
            doc!!
        }
    }

    /**
     * Retrieves all documents matching a specific index value.
     * 
     * Uses the "Reverse Pointer" strategy to filter out stale index entries 
     * without reading full documents, keeping performance high even with 
     * frequent updates.
     *
     * @param indexName The name of the index to query.
     * @param value The value to look for in the index.
     * @return A list of matching documents.
     */
    fun getByIndex(indexName: String, value: String): List<T> {
        val idxNameBytes = getIndexNameBytes(indexName)
        val escapedValue = escape(value)
        val valBytes = escapedValue.toByteArray(Charsets.UTF_8)
        val prefix = buildKey(idxPrefix, idxNameBytes, valBytes)
        
        val indexKeys = db.getKeysByPrefixRaw(prefix)

        return indexKeys.mapNotNull { keyBytes ->
            val escapedId = String(keyBytes, prefix.size, keyBytes.size - 1 - prefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)

            val rptrKey = buildKey(rptrPrefix, idxNameBytes, escapedId.toByteArray(Charsets.UTF_8))
            val currentValueBytes = db.getRaw(rptrKey)
            
            // If no rptr exists, the entry has never been updated → index is fresh & valid.
            // If rptr exists but points to a different value, the entry is stale → skip.
            if (currentValueBytes != null) {
                val currentValue = String(currentValueBytes, Charsets.UTF_8)
                if (currentValue != escapedValue) {
                    return@mapNotNull null // Stale entry
                }
            }
            
            getById(id)
        }
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
     * Retrieves all documents in the collection as a lazy [Sequence].
     * Avoids eager deserialization of all documents when combined with limit or filters.
     */
    fun asSequence(): Sequence<T> = sequence {
        val rawResults = db.getByPrefixWithKeysRaw(docPrefix)
        for ((keyBytes, valueBytes) in rawResults) {
            val escapedId = String(keyBytes, docPrefix.size, keyBytes.size - docPrefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)
            
            var doc: T? = null
            synchronized(documentCache) {
                doc = documentCache[id]
            }

            if (doc == null) {
                doc = serializer.deserialize(valueBytes)
                synchronized(documentCache) {
                    documentCache[id] = doc!!
                }
            }
            yield(doc!!)
        }
    }

    /**
     * Retrieves all documents in the collection.
     *
     * @return A list of all documents.
     */
    fun getAll(): List<T> {
        return getAllWithIds().values.toList()
    }

    /**
     * Retrieves all documents in the collection as a map of ID to document.
     */
    fun getAllWithIds(): Map<String, T> {
        val rawResults = db.getByPrefixWithKeysRaw(docPrefix)
        val map = LinkedHashMap<String, T>()
        for ((keyBytes, valueBytes) in rawResults) {
            val escapedId = String(keyBytes, docPrefix.size, keyBytes.size - docPrefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)
            
            var doc: T? = null
            synchronized(documentCache) {
                doc = documentCache[id]
            }

            if (doc == null) {
                doc = serializer.deserialize(valueBytes)
                synchronized(documentCache) {
                    documentCache[id] = doc!!
                }
            }
            map[id] = doc!!
        }
        return map
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
     * Architecture:
     * 1. Serialization is parallelized across CPU cores using chunked async.
     * 2. All serialized KV pairs are combined into a single flat list.
     * 3. A single [KoreDB.writeBatchRaw] call writes everything atomically,
     *    minimizing mutex acquisitions, WAL headers, and MemTable operations.
     *
     * For very large batches (>50K KV pairs), falls back to chunked writes
     * to avoid excessive memory pressure.
     *
     * @param documents A map of IDs to documents.
     */
    suspend fun insertBatch(documents: Map<String, T>) = coroutineScope {
        // Snapshot existing IDs to determine fresh vs update inserts.
        // Fresh inserts skip rptr writes (33% fewer KV pairs).
        val existingIds: Set<String> = synchronized(documentCache) {
            documentCache.keys.toHashSet()
        }
        
        val entriesPerDoc = 1 + indexExtractors.size * 2
        val totalKvPairs = documents.size * entriesPerDoc
        
        if (totalKvPairs <= SINGLE_WRITE_THRESHOLD) {
            // 🚀 FAST PATH: Parallel serialize → single atomic write
            val chunks = documents.entries.chunked(5000)
            
            val serializedChunks = chunks.map { chunk ->
                async(Dispatchers.Default) { serializeChunk(chunk, existingIds) }
            }.awaitAll()
            
            // Combine into a single flat list for one writeBatchRaw call
            val combined = ArrayList<Pair<ByteArray, ByteArray>>(totalKvPairs)
            for (chunk in serializedChunks) {
                combined.addAll(chunk)
            }
            
            db.writeBatchRaw(combined)
        } else {
            // LARGE BATCH PATH: Chunked writes to bound memory usage
            val chunks = documents.entries.chunked(5000)
            for (chunk in chunks) {
                val batch = serializeChunk(chunk, existingIds)
                db.writeBatchRaw(batch)
            }
        }

        // Update Cache & Notify
        synchronized(documentCache) {
            for (entry in documents) {
                documentCache[entry.key] = entry.value
            }
        }
        updates.tryEmit("*")
    }
    
    companion object {
        /**
         * Maximum number of KV pairs to write in a single writeBatchRaw call.
         * Beyond this, we chunk writes to avoid excessive memory pressure.
         * 50K pairs ≈ 5MB of data, well within the 16MB MemTable threshold.
         */
        private const val SINGLE_WRITE_THRESHOLD = 50_000
    }

    /**
     * Serializes a chunk of document entries into raw key-value pairs for storage.
     *
     * Optimizations:
     * - For fresh inserts (document not in [existingIds]), the reverse pointer
     *   (rptr) is skipped. The rptr is only needed when updating, to invalidate
     *   the previous index entry. Fresh entries have no prior index to invalidate.
     * - This reduces KV pairs from 3 to 2 per new document (33% fewer entries).
     *
     * @param chunk The document entries to serialize.
     * @param existingIds IDs of documents already in the collection (need rptr on update).
     */
    private fun serializeChunk(
        chunk: List<Map.Entry<String, T>>,
        existingIds: Set<String>
    ): ArrayList<Pair<ByteArray, ByteArray>> {
        val list = ArrayList<Pair<ByteArray, ByteArray>>(chunk.size * (1 + indexExtractors.size * 2))
        for (entry in chunk) {
            val escapedId = escape(entry.key)
            val idBytes = escapedId.toByteArray(Charsets.UTF_8)
            val docBytes = serializer.serialize(entry.value)
            val isUpdate = existingIds.contains(entry.key)
            
            // Primary Document
            list.add(makeDocKey(idBytes) to docBytes)

            // Full-Text Search Indexing
            if (searchableExtractors.isNotEmpty()) {
                val combinedText = searchableExtractors.joinToString(" ") { it(entry.value) }
                ftsIndex.indexDocument(entry.key, combinedText, list)
            }

            // Secondary Indices
            for ((idxName, extractor) in indexExtractors) {
                val value = extractor(entry.value)
                val valBytes = escape(value).toByteArray(Charsets.UTF_8)
                val idxNameBytes = getIndexNameBytes(idxName)
                
                list.add(buildKey(idxPrefix, idxNameBytes, valBytes, idBytes) to PRESENCE_MARKER)
                
                // Only write rptr for updates (fresh inserts have no stale index to invalidate)
                if (isUpdate) {
                    list.add(buildKey(rptrPrefix, idxNameBytes, idBytes) to valBytes)
                }
            }
        }
        return list
    }

    /**
     * Executes an Okapi BM25 Full-Text Search query across registered searchable fields.
     *
     * @param query Keyword search query string.
     * @param limit Maximum number of top-matching results to return.
     * @return List of Pair(Document, BM25Score) sorted in descending order of relevance.
     */
    fun searchBM25(query: String, limit: Int = 10): List<Pair<T, Float>> {
        val scoredIds = ftsIndex.search(query, limit)
        return scoredIds.mapNotNull { (id, score) ->
            val doc = getById(id) ?: return@mapNotNull null
            doc to score
        }
    }

    /**
     * Deletes all documents in the collection and notifies observers.
     */
    suspend fun deleteAll() {
        val keysToDelete = db.getKeysByPrefixRaw(docPrefix)
        val idxKeysToDelete = db.getKeysByPrefixRaw(idxPrefix)
        val rptrKeysToDelete = db.getKeysByPrefixRaw(rptrPrefix)
        val ftsKeysToDelete = db.getKeysByPrefixRaw("fts:$name:".toByteArray(Charsets.UTF_8))
        val ftsLenKeysToDelete = db.getKeysByPrefixRaw("ftslen:$name:".toByteArray(Charsets.UTF_8))

        val allKeys = keysToDelete + idxKeysToDelete + rptrKeysToDelete + ftsKeysToDelete + ftsLenKeysToDelete
        if (allKeys.isEmpty()) return

        val batch = allKeys.map { keyBytes ->
            Pair(keyBytes, KoreDB.TOMBSTONE)
        }

        db.writeBatchRaw(batch)
        ftsIndex.clear()
        synchronized(documentCache) {
            documentCache.clear()
        }
        updates.tryEmit("*")
    }

    // ========================================================================
    // QUERY DSL
    // ========================================================================

    private val propertyExtractors = mutableMapOf<String, (T) -> String>()

    /**
     * Registers a property extractor for use in queries and aggregations.
     *
     * ```kotlin
     * collection.registerProperty("price") { it.price.toString() }
     * collection.registerProperty("category") { it.category }
     * ```
     */
    fun registerProperty(name: String, extractor: (T) -> String) {
        propertyExtractors[name] = extractor
    }

    /**
     * Creates a query builder for this collection.
     *
     * ```kotlin
     * val expensive = collection.query()
     *     .where("price") { it.toDouble() > 100 }
     *     .sortBy("price", descending = true) { it.toDouble() }
     *     .limit(10)
     *     .execute()
     * ```
     */
    fun query(): KoreQuery<T> = KoreQuery(this, propertyExtractors.toMap())

    /**
     * Returns the total count of documents in the collection.
     */
    fun count(): Int {
        return db.getKeysByPrefixRaw(docPrefix).size
    }

    // ========================================================================
    // PARTIAL DOCUMENT UPDATES
    // ========================================================================

    /**
     * Updates specific fields of a document without replacing the entire object.
     *
     * This reads the existing document, applies the [transform] function to produce
     * a modified version, then persists it. This is more efficient than a full
     * read-modify-write cycle from the application layer because it operates
     * within a single atomic batch.
     *
     * ```kotlin
     * collection.updateFields("note_1") { note ->
     *     note.copy(title = "New Title", isPinned = true)
     * }
     * ```
     *
     * @param id The document ID to update.
     * @param transform A function that receives the current document and returns the modified version.
     * @return The updated document, or null if the document doesn't exist.
     */
    suspend fun updateFields(id: String, transform: (T) -> T): T? {
        val existing = getById(id) ?: return null
        val updated = transform(existing)

        // Re-insert the modified document (handles index updates atomically)
        insertBatch(mapOf(id to updated))

        return updated
    }

    /**
     * Updates a specific field for multiple documents in batch.
     *
     * @param ids The document IDs to update.
     * @param transform A function applied to each existing document.
     * @return The number of documents successfully updated.
     */
    suspend fun updateFieldsBatch(ids: List<String>, transform: (T) -> T): Int {
        val updates = mutableMapOf<String, T>()
        for (id in ids) {
            val existing = getById(id) ?: continue
            updates[id] = transform(existing)
        }
        if (updates.isNotEmpty()) {
            insertBatch(updates)
        }
        return updates.size
    }
}
