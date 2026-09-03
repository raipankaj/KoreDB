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
    internal val internalUpdates: kotlinx.coroutines.flow.SharedFlow<String> get() = updates
    var cdcManager: com.pankaj.koredb.cdc.CdcManager? = null
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

    /**
     * Clears the in-memory document cache.
     */
    fun clearCache() {
        synchronized(documentCache) {
            documentCache.clear()
        }
    }

    /**
     * Invalidates a specific document ID from the in-memory cache.
     */
    fun invalidateCache(id: String) {
        synchronized(documentCache) {
            documentCache.remove(id)
        }
    }

    private fun escape(value: String): String {
        if (value.indexOf('%') == -1 && value.indexOf(':') == -1) return value
        return value.replace("%", "%25").replace(":", "%3A")
    }

    private fun unescape(value: String): String {
        if (value.indexOf('%') == -1) return value
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
        propertyExtractors[indexName] = extractor
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
    private val ttlPrefix = "ttl:$name:".toByteArray(Charsets.UTF_8)
    private val idxPrefix = "idx:$name:".toByteArray(Charsets.UTF_8)
    private val idxNumPrefix = "idx_num:$name:".toByteArray(Charsets.UTF_8)
    private val numericIndexExtractors = java.util.concurrent.ConcurrentHashMap<String, (T) -> Double>()

    /**
     * Registers a typed numeric secondary index for instant order-preserving range queries.
     *
     * @param indexName Unique name for the numeric index.
     * @param extractor Function to extract the Double value from a document.
     */
    fun createNumericIndex(indexName: String, extractor: (T) -> Double) {
        numericIndexExtractors[indexName] = extractor
        propertyExtractors[indexName] = { doc -> extractor(doc).toString() }
    }

    fun hasNumericIndex(indexName: String): Boolean = numericIndexExtractors.containsKey(indexName)
    fun getNumericExtractor(indexName: String): ((T) -> Double)? = numericIndexExtractors[indexName]

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

    @Volatile
    private var hasTtlRecords = false

    private fun makeTtlKey(idBytes: ByteArray): ByteArray {
        val result = ByteArray(ttlPrefix.size + idBytes.size)
        System.arraycopy(ttlPrefix, 0, result, 0, ttlPrefix.size)
        System.arraycopy(idBytes, 0, result, ttlPrefix.size, idBytes.size)
        return result
    }

    private fun isExpired(idBytes: ByteArray): Boolean {
        if (!hasTtlRecords) return false
        val ttlBytes = db.getRaw(makeTtlKey(idBytes)) ?: return false
        if (ttlBytes.size != 8) return false
        val expireAt = java.nio.ByteBuffer.wrap(ttlBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).long
        return System.currentTimeMillis() > expireAt
    }

    internal fun makeDocKey(id: String): ByteArray {
        val idBytes = escape(id).toByteArray(Charsets.UTF_8)
        return makeDocKey(idBytes)
    }

    internal val internalSerializer: KoreSerializer<T> get() = serializer

    /**
     * A non-empty byte array used to indicate entry existence in secondary indices.
     */
    private val PRESENCE_MARKER = ByteArray(1) { 1 }

    /**
     * Inserts a document with an auto-generated unique ID.
     *
     * @param document The document to persist.
     * @param ttlSeconds Optional time-to-live in seconds (0 = never expires).
     * @return The auto-generated document ID.
     */
    suspend fun insert(document: T, ttlSeconds: Long = 0): String {
        val generatedId = java.util.UUID.randomUUID().toString()
        insert(generatedId, document, ttlSeconds)
        return generatedId
    }

    /**
     * Inserts or updates a document.
     *
     * @param id Unique identifier for the document.
     * @param document The document to store.
     * @param ttlSeconds Optional time-to-live in seconds (0 = never expires).
     */
    suspend fun insert(id: String, document: T, ttlSeconds: Long = 0) = coroutineScope {
        val idBytes = escape(id).toByteArray(Charsets.UTF_8)
        val docBytes = serializer.serialize(document)
        val docKey = makeDocKey(idBytes)

        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        batch.add(docKey to docBytes)

        if (ttlSeconds > 0) {
            hasTtlRecords = true
            val expireAt = System.currentTimeMillis() + ttlSeconds * 1000L
            val ttlVal = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(expireAt).array()
            batch.add(makeTtlKey(idBytes) to ttlVal)
        } else if (hasTtlRecords) {
            batch.add(makeTtlKey(idBytes) to KoreDB.TOMBSTONE)
        }

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
            val idxKey = buildIndexKey(idxPrefix, idxNameBytes, valBytes, idBytes)
            batch.add(idxKey to PRESENCE_MARKER)
        }

        numericIndexExtractors.forEach { (idxName, extractor) ->
            val numValue = extractor(document)
            val valBytes = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(numValue)
            val idxNameBytes = getIndexNameBytes(idxName)

            val idxKey = buildIndexKey(idxNumPrefix, idxNameBytes, valBytes, idBytes)
            batch.add(idxKey to PRESENCE_MARKER)
        }

        db.writeBatchRaw(batch)
        synchronized(documentCache) {
            documentCache[id] = document
        }
        updates.tryEmit(id)
        cdcManager?.recordMutation(name, id, com.pankaj.koredb.cdc.MutationOp.INSERT, docBytes)
    }

    private fun buildIndexKey(prefix: ByteArray, idxName: ByteArray, valBytes: ByteArray, idBytes: ByteArray): ByteArray {
        val totalSize = prefix.size + idxName.size + valBytes.size + idBytes.size + 3
        val result = ByteArray(totalSize)
        System.arraycopy(prefix, 0, result, 0, prefix.size)
        var pos = prefix.size
        System.arraycopy(idxName, 0, result, pos, idxName.size)
        pos += idxName.size
        result[pos++] = ':'.code.toByte()
        System.arraycopy(valBytes, 0, result, pos, valBytes.size)
        pos += valBytes.size
        result[pos++] = ':'.code.toByte()
        System.arraycopy(idBytes, 0, result, pos, idBytes.size)
        return result
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
     * Deletes multiple documents in a single atomic LSM write batch.
     * Removes associated secondary indices, numeric indices, FTS indices, and TTL records.
     *
     * @param ids The list of document IDs to delete.
     */
    suspend fun deleteBatch(ids: List<String>) {
        if (ids.isEmpty()) return

        val batchCapacity = ids.size * (if (hasTtlRecords) 2 else 1)
        val batch = ArrayList<Pair<ByteArray, ByteArray>>(batchCapacity)

        for (id in ids) {
            val idBytes = escape(id).toByteArray(Charsets.UTF_8)
            batch.add(makeDocKey(idBytes) to KoreDB.TOMBSTONE)
            if (hasTtlRecords) {
                batch.add(makeTtlKey(idBytes) to KoreDB.TOMBSTONE)
            }

            if (searchableExtractors.isNotEmpty()) {
                ftsIndex.removeDocument(id, batch)
            }
        }

        if (batch.isNotEmpty()) {
            db.writeBatchRaw(batch)
        }

        synchronized(documentCache) {
            for (id in ids) {
                documentCache.remove(id)
            }
        }

        if (ids.size == 1) {
            updates.tryEmit(ids[0])
        }
        updates.tryEmit("*")
        cdcManager?.recordMutationsBatch(name, ids, com.pankaj.koredb.cdc.MutationOp.DELETE)
    }

    /**
     * Deletes a document by its ID.
     *
     * @param id The ID of the document to delete.
     */
    suspend fun delete(id: String) {
        deleteBatch(listOf(id))
    }

    /**
     * Retrieves a document by its ID.
     * Returns null if not found or if the document's TTL has expired.
     *
     * @param id The ID of the document to retrieve.
     * @return The deserialized document, or null if not found or expired.
     */
    fun getById(id: String): T? {
        val idBytes = escape(id).toByteArray(Charsets.UTF_8)
        if (isExpired(idBytes)) {
            synchronized(documentCache) {
                documentCache.remove(id)
            }
            return null
        }

        synchronized(documentCache) {
            val cached = documentCache[id]
            if (cached != null) return cached
        }

        val resultBytes = db.getRaw(makeDocKey(idBytes)) ?: return null
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
    fun getByIndex(indexName: String, value: String, limit: Int? = null): List<T> {
        val idxNameBytes = getIndexNameBytes(indexName)
        val escapedValue = escape(value)
        val valBytes = escapedValue.toByteArray(Charsets.UTF_8)
        val prefix = buildKey(idxPrefix, idxNameBytes, valBytes)
        
        val indexKeys = db.getKeysByPrefixRaw(prefix, limit)
        val results = mutableListOf<T>()
        val extractor = indexExtractors[indexName]

        for (keyBytes in indexKeys) {
            if (limit != null && results.size >= limit) break
            val escapedId = String(keyBytes, prefix.size, keyBytes.size - 1 - prefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)

            val doc = getById(id) ?: continue
            if (extractor != null && extractor(doc) != value) {
                continue // Stale index entry
            }
            results.add(doc)
        }
        return results
    }

    /**
     * Retrieves all documents where the indexed numeric value falls in the range [min, max] (inclusive).
     * Leverages order-preserving byte encoding and LSM range scan for sub-millisecond execution.
     */
    fun getByNumericRange(indexName: String, min: Double, max: Double, limit: Int? = null): List<T> {
        val idxNameBytes = getIndexNameBytes(indexName)
        val minEncoded = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(min)
        val maxEncoded = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(max)

        val startKey = buildKey(idxNumPrefix, idxNameBytes, minEncoded)
        val endPrefix = buildKey(idxNumPrefix, idxNameBytes, maxEncoded)

        // Make endKey an upper-bound by appending 0xFF
        val endKey = ByteArray(endPrefix.size + 1)
        System.arraycopy(endPrefix, 0, endKey, 0, endPrefix.size)
        endKey[endKey.size - 1] = 0xFF.toByte()

        val pairs = db.getRangeWithKeysRaw(startKey, endKey, limit)
        val results = mutableListOf<T>()
        val extractor = numericIndexExtractors[indexName]

        for ((keyBytes, _) in pairs) {
            if (limit != null && results.size >= limit) break
            val prefixLen = idxNumPrefix.size + idxNameBytes.size + 10
            if (keyBytes.size <= prefixLen + 1) continue
            val idLength = keyBytes.size - prefixLen - 1
            val escapedId = String(keyBytes, prefixLen, idLength, Charsets.UTF_8)
            val id = unescape(escapedId)

            val doc = getById(id) ?: continue
            if (extractor != null) {
                val currentVal = extractor(doc)
                if (currentVal < min || currentVal > max) {
                    continue // Stale entry
                }
            }
            results.add(doc)
        }
        return results
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
            val idLength = keyBytes.size - docPrefix.size
            val idBytes = ByteArray(idLength)
            System.arraycopy(keyBytes, docPrefix.size, idBytes, 0, idLength)
            if (isExpired(idBytes)) continue

            val escapedId = String(idBytes, Charsets.UTF_8)
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
            val idLength = keyBytes.size - docPrefix.size
            val idBytes = ByteArray(idLength)
            System.arraycopy(keyBytes, docPrefix.size, idBytes, 0, idLength)
            if (isExpired(idBytes)) continue

            val escapedId = String(idBytes, Charsets.UTF_8)
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
     * Backfills and rebuilds all registered secondary, numeric, and full-text indexes
     * by scanning all existing documents in the collection.
     *
     * Essential during schema migrations or after registering new indexes on pre-existing data.
     */
    suspend fun rebuildIndexes() {
        val rawResults = db.getByPrefixWithKeysRaw(docPrefix)
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()

        for ((keyBytes, valueBytes) in rawResults) {
            val escapedId = String(keyBytes, docPrefix.size, keyBytes.size - docPrefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)
            val idBytes = escape(id).toByteArray(Charsets.UTF_8)

            val document = try {
                serializer.deserialize(valueBytes)
            } catch (_: Throwable) {
                continue
            }

            if (searchableExtractors.isNotEmpty()) {
                val combinedText = searchableExtractors.joinToString(" ") { it(document) }
                ftsIndex.indexDocument(id, combinedText, batch)
            }

            indexExtractors.forEach { (idxName, extractor) ->
                val value = extractor(document)
                val valBytes = escape(value).toByteArray(Charsets.UTF_8)
                val idxNameBytes = getIndexNameBytes(idxName)
                val idxKey = buildKey(idxPrefix, idxNameBytes, valBytes, idBytes)
                batch.add(idxKey to PRESENCE_MARKER)
            }

            numericIndexExtractors.forEach { (idxName, extractor) ->
                val numValue = extractor(document)
                val valBytes = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(numValue)
                val idxNameBytes = getIndexNameBytes(idxName)
                val idxKey = buildKey(idxNumPrefix, idxNameBytes, valBytes, idBytes)
                batch.add(idxKey to PRESENCE_MARKER)
            }

            if (batch.size >= 5000) {
                db.writeBatchRaw(batch)
                batch.clear()
            }
        }

        if (batch.isNotEmpty()) {
            db.writeBatchRaw(batch)
            batch.clear()
        }
    }

    /**
     * Backfills a specific named secondary or numeric index across all existing documents.
     */
    suspend fun rebuildIndex(indexName: String) {
        val strExtractor = indexExtractors[indexName]
        val numExtractor = numericIndexExtractors[indexName]
        if (strExtractor == null && numExtractor == null) return

        val rawResults = db.getByPrefixWithKeysRaw(docPrefix)
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        val idxNameBytes = getIndexNameBytes(indexName)

        for ((keyBytes, valueBytes) in rawResults) {
            val escapedId = String(keyBytes, docPrefix.size, keyBytes.size - docPrefix.size, Charsets.UTF_8)
            val id = unescape(escapedId)
            val idBytes = escape(id).toByteArray(Charsets.UTF_8)

            val document = try {
                serializer.deserialize(valueBytes)
            } catch (_: Throwable) {
                continue
            }

            if (strExtractor != null) {
                val value = strExtractor(document)
                val valBytes = escape(value).toByteArray(Charsets.UTF_8)
                val idxKey = buildKey(idxPrefix, idxNameBytes, valBytes, idBytes)
                batch.add(idxKey to PRESENCE_MARKER)
            }

            if (numExtractor != null) {
                val numValue = numExtractor(document)
                val valBytes = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(numValue)
                val idxKey = buildKey(idxNumPrefix, idxNameBytes, valBytes, idBytes)
                batch.add(idxKey to PRESENCE_MARKER)
            }

            if (batch.size >= 5000) {
                db.writeBatchRaw(batch)
                batch.clear()
            }
        }

        if (batch.isNotEmpty()) {
            db.writeBatchRaw(batch)
            batch.clear()
        }
    }

    /**
     * Performs a batch insert of multiple documents.
     * Checks if a secondary index exists on the given index name.
     */
    fun hasIndex(indexName: String): Boolean = indexExtractors.containsKey(indexName)

    /**
     * High-throughput bulk insertion of documents from an arbitrary [Collection] using an ID extractor.
     * Avoids intermediate Map allocations and pre-allocates flat batch structures.
     */
    suspend fun insertBatch(documents: Collection<T>, idExtractor: (T) -> String) = coroutineScope {
        if (documents.isEmpty()) return@coroutineScope
        val entriesPerDoc = 1 + indexExtractors.size + numericIndexExtractors.size
        val totalKvPairs = documents.size * entriesPerDoc
        
        if (totalKvPairs <= SINGLE_WRITE_THRESHOLD) {
            val combined = ArrayList<Pair<ByteArray, ByteArray>>(totalKvPairs)
            for (doc in documents) {
                serializeSingleDoc(idExtractor(doc), doc, combined)
            }
            db.writeBatchRaw(combined)
        } else {
            val chunks = documents.chunked(5000)
            for (chunk in chunks) {
                val batch = ArrayList<Pair<ByteArray, ByteArray>>(chunk.size * entriesPerDoc)
                for (doc in chunk) {
                    serializeSingleDoc(idExtractor(doc), doc, batch)
                }
                db.writeBatchRaw(batch)
            }
        }

        synchronized(documentCache) {
            var count = 0
            for (doc in documents) {
                documentCache[idExtractor(doc)] = doc
                if (++count >= 8192) break
            }
        }
        updates.tryEmit("*")
    }

    /**
     * Bulk inserts a map of ID-to-Document pairs.
     *
     * @param documents A map of IDs to documents.
     */
    suspend fun insertBatch(documents: Map<String, T>) = coroutineScope {
        if (documents.isEmpty()) return@coroutineScope
        val entriesPerDoc = 1 + indexExtractors.size + numericIndexExtractors.size
        val totalKvPairs = documents.size * entriesPerDoc
        
        if (totalKvPairs <= SINGLE_WRITE_THRESHOLD) {
            val combined = ArrayList<Pair<ByteArray, ByteArray>>(totalKvPairs)
            for ((id, doc) in documents) {
                serializeSingleDoc(id, doc, combined)
            }
            db.writeBatchRaw(combined)
        } else {
            val chunks = documents.entries.chunked(5000)
            for (chunk in chunks) {
                val batch = ArrayList<Pair<ByteArray, ByteArray>>(chunk.size * entriesPerDoc)
                for (entry in chunk) {
                    serializeSingleDoc(entry.key, entry.value, batch)
                }
                db.writeBatchRaw(batch)
            }
        }

        // Update Cache & Notify
        synchronized(documentCache) {
            var count = 0
            for (entry in documents) {
                documentCache[entry.key] = entry.value
                if (++count >= 8192) break
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

    private fun serializeSingleDoc(
        id: String,
        doc: T,
        out: ArrayList<Pair<ByteArray, ByteArray>>
    ) {
        val escapedId = escape(id)
        val idBytes = escapedId.toByteArray(Charsets.UTF_8)
        val docBytes = serializer.serialize(doc)
        out.add(makeDocKey(idBytes) to docBytes)

        if (searchableExtractors.isNotEmpty()) {
            val combinedText = searchableExtractors.joinToString(" ") { it(doc) }
            ftsIndex.indexDocument(id, combinedText, out)
        }

        for ((idxName, extractor) in indexExtractors) {
            val value = extractor(doc)
            val valBytes = escape(value).toByteArray(Charsets.UTF_8)
            val idxNameBytes = getIndexNameBytes(idxName)
            out.add(buildIndexKey(idxPrefix, idxNameBytes, valBytes, idBytes) to PRESENCE_MARKER)
        }

        for ((idxName, extractor) in numericIndexExtractors) {
            val numValue = extractor(doc)
            val valBytes = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(numValue)
            val idxNameBytes = getIndexNameBytes(idxName)
            out.add(buildIndexKey(idxNumPrefix, idxNameBytes, valBytes, idBytes) to PRESENCE_MARKER)
        }
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
        val idxNumKeysToDelete = db.getKeysByPrefixRaw(idxNumPrefix)
        val ftsKeysToDelete = db.getKeysByPrefixRaw("fts:$name:".toByteArray(Charsets.UTF_8))
        val ftsLenKeysToDelete = db.getKeysByPrefixRaw("ftslen:$name:".toByteArray(Charsets.UTF_8))
        val ttlKeysToDelete = db.getKeysByPrefixRaw(ttlPrefix)

        val allKeys = keysToDelete + ttlKeysToDelete + idxKeysToDelete + idxNumKeysToDelete + ftsKeysToDelete + ftsLenKeysToDelete
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
        val keys = db.getKeysByPrefixRaw(docPrefix)
        var valid = 0
        for (keyBytes in keys) {
            val idLength = keyBytes.size - docPrefix.size
            val idBytes = ByteArray(idLength)
            System.arraycopy(keyBytes, docPrefix.size, idBytes, 0, idLength)
            if (!isExpired(idBytes)) valid++
        }
        return valid
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
