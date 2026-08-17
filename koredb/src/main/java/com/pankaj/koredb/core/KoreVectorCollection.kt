package com.pankaj.koredb.core

import com.pankaj.koredb.engine.KoreDB
import com.pankaj.koredb.hnsw.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

/**
 * Configuration for a vector collection.
 */
data class VectorCollectionConfig(
    val dimensions: Int = 0,
    val metric: DistanceMetric = DistanceMetric.COSINE,
    val quantization: Boolean = false,
    val maxConnections: Int = 16,
    val efConstruction: Int = 200,
    val efSearch: Int = 50,
    val namespace: String = ""
) {
    class Builder {
        var dimensions: Int = 0
        var metric: DistanceMetric = DistanceMetric.COSINE
        var quantization: Boolean = false
        var maxConnections: Int = 16
        var efConstruction: Int = 200
        var efSearch: Int = 50
        var namespace: String = ""
        
        fun build() = VectorCollectionConfig(
            dimensions, metric, quantization,
            maxConnections, efConstruction, efSearch, namespace
        )
    }
}

/**
 * A world-class vector collection with production-grade features.
 *
 * ### Features:
 * - **Multiple Distance Metrics**: Cosine, Euclidean, Inner Product, Manhattan
 * - **Hybrid Search**: Vector similarity + metadata filtering in a single query
 * - **Vector Delete & Update**: Soft-delete with background compaction
 * - **Scalar Quantization**: Optional SQ8 for 4x memory reduction
 * - **Multi-Vector**: Store multiple named vector fields per document
 * - **Namespace Isolation**: Logical partitioning for multi-tenant apps
 * - **Background Indexing**: Non-blocking HNSW construction via coroutine channels
 * - **Persistent**: Durable WAL-protected storage + binary HNSW snapshots
 * - **Index Health Monitoring**: Statistics and diagnostic information
 *
 * Usage:
 * ```kotlin
 * val collection = db.vectorCollection("products") {
 *     metric = DistanceMetric.COSINE
 *     quantization = true
 *     efSearch = 100
 * }
 *
 * collection.insert("product_1", embedding, metadata = mapOf(
 *     "category" to "shoes",
 *     "price" to 99.99
 * ))
 *
 * val results = collection.search(queryVector, limit = 10) {
 *     where("category", eq("shoes"))
 *     where("price", lt(100.0))
 * }
 * ```
 */
class KoreVectorCollection(
    val name: String,
    private val db: KoreDB,
    private val config: VectorCollectionConfig = VectorCollectionConfig()
) {
    private val quantizer = if (config.quantization && config.dimensions > 0) {
        ScalarQuantizer(config.dimensions)
    } else null

    private val hnsw = HNSWIndex(
        maxNeighbors = config.maxConnections,
        efConstruction = config.efConstruction,
        efSearch = config.efSearch,
        metric = config.metric,
        quantizer = quantizer
    )

    // Multi-vector: separate HNSW index per vector field
    private val fieldIndices = java.util.concurrent.ConcurrentHashMap<String, HNSWIndex>()

    private var mmapHnsw: MmapHNSWIndex? = null

    private fun ensureMutableHnsw() {
        if (mmapHnsw != null && hnsw.size() == 0) {
            try {
                hnsw.loadFromDisk(indexFile)
            } catch (_: Exception) {}
            mmapHnsw?.close()
            mmapHnsw = null
        }
    }

    // Background worker for non-blocking HNSW construction
    private val indexingScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val indexingChannel = Channel<IndexingTask>(Channel.UNLIMITED)

    // Namespace-aware key prefix
    private val keyPrefix = if (config.namespace.isEmpty()) {
        "vec:$name:"
    } else {
        "vec:${config.namespace}:$name:"
    }

    private val metaPrefix = if (config.namespace.isEmpty()) {
        "vmeta:$name:"
    } else {
        "vmeta:${config.namespace}:$name:"
    }

    private val indexFile = java.io.File(
        db.directory,
        if (config.namespace.isEmpty()) "hnsw_${name}.bin"
        else "hnsw_${config.namespace}_${name}.bin"
    )

    private sealed class IndexingTask {
        data class Insert(val id: String, val vector: FloatArray, val meta: Map<String, Any>?) : IndexingTask()
        data class InsertField(val id: String, val field: String, val vector: FloatArray, val meta: Map<String, Any>?) : IndexingTask()
        data class Delete(val id: String) : IndexingTask()
        data class Update(val id: String, val vector: FloatArray, val meta: Map<String, Any>?) : IndexingTask()
    }

    init {
        indexingScope.launch {
            hydrateFromDisk()
            val numWorkers = maxOf(2, Runtime.getRuntime().availableProcessors())
            val jobs = List(numWorkers) {
                launch(Dispatchers.Default) {
                    for (task in indexingChannel) {
                        ensureMutableHnsw()
                        when (task) {
                            is IndexingTask.Insert -> {
                                hnsw.insert(task.id, task.vector, VectorMath.getMagnitude(task.vector), task.meta)
                            }
                            is IndexingTask.InsertField -> {
                                val fieldIndex = fieldIndices.getOrPut(task.field) {
                                    HNSWIndex(
                                        maxNeighbors = config.maxConnections,
                                        efConstruction = config.efConstruction,
                                        efSearch = config.efSearch,
                                        metric = config.metric
                                    )
                                }
                                fieldIndex.insert(task.id, task.vector, VectorMath.getMagnitude(task.vector), task.meta)
                            }
                            is IndexingTask.Delete -> {
                                hnsw.delete(task.id)
                                fieldIndices.values.forEach { it.delete(task.id) }
                            }
                            is IndexingTask.Update -> {
                                hnsw.update(task.id, task.vector, task.meta)
                            }
                        }
                    }
                }
            }
        }
    }

    // ========================================================================
    // INSERT
    // ========================================================================

    /**
     * Inserts a vector with optional metadata.
     * Returns immediately — HNSW indexing happens in the background.
     *
     * @param id Unique identifier for this vector.
     * @param vector The float vector to store and index.
     * @param metadata Optional key-value metadata for hybrid search.
     */
    suspend fun insert(id: String, vector: FloatArray, metadata: Map<String, Any>? = null) {
        val isUpdate = mmapHnsw?.contains(id) ?: hnsw.contains(id)
        db.putRaw(makeKey(id), VectorSerializer.toByteArray(vector))
        if (metadata != null) {
            db.putRaw(makeMetaKey(id), serializeMetadata(metadata))
        }
        if (isUpdate) {
            indexingChannel.send(IndexingTask.Update(id, vector, metadata))
        } else {
            indexingChannel.send(IndexingTask.Insert(id, vector, metadata))
        }
    }

    /**
     * Inserts multiple named vectors for a single document (multi-vector support).
     *
     * ```kotlin
     * collection.insertMultiVector("product_123", mapOf(
     *     "title" to titleEmbedding,
     *     "image" to imageEmbedding
     * ))
     * ```
     *
     * @param id Document identifier.
     * @param vectors Map of field name to vector.
     * @param metadata Optional shared metadata.
     */
    suspend fun insertMultiVector(
        id: String,
        vectors: Map<String, FloatArray>,
        metadata: Map<String, Any>? = null
    ) {
        val batch = vectors.map { (field, vector) ->
            makeFieldKey(id, field) to VectorSerializer.toByteArray(vector)
        }.toMutableList()

        // Also store the first vector as the primary vector
        vectors.entries.firstOrNull()?.let { (_, vector) ->
            batch.add(makeKey(id) to VectorSerializer.toByteArray(vector))
        }

        if (metadata != null) {
            batch.add(makeMetaKey(id) to serializeMetadata(metadata))
        }

        db.writeBatchRaw(batch)

        for ((field, vector) in vectors) {
            indexingChannel.send(IndexingTask.InsertField(id, field, vector, metadata))
        }
    }

    /**
     * High-throughput batch insert of multiple vectors.
     */
    suspend fun insertBatch(
        vectors: Map<String, FloatArray>,
        metadataMap: Map<String, Map<String, Any>>? = null
    ) = coroutineScope {
        // Parallel serialization
        val batch = vectors.entries.chunked(2500).map { chunk ->
            async(Dispatchers.Default) {
                val list = mutableListOf<Pair<ByteArray, ByteArray>>()
                for ((id, vector) in chunk) {
                    list.add(makeKey(id) to VectorSerializer.toByteArray(vector))
                    val meta = metadataMap?.get(id)
                    if (meta != null) {
                        list.add(makeMetaKey(id) to serializeMetadata(meta))
                    }
                }
                list
            }
        }.awaitAll().flatten()

        db.writeBatchRaw(batch)

        // Offload HNSW construction to background
        for ((id, vector) in vectors) {
            indexingChannel.send(IndexingTask.Insert(id, vector, metadataMap?.get(id)))
        }
    }

    // ========================================================================
    // SEARCH
    // ========================================================================

    /**
     * Searches for the most similar vectors with optional metadata filtering.
     *
     * ```kotlin
     * val results = collection.search(queryVector, limit = 10) {
     *     where("category", eq("shoes"))
     *     where("price", lt(100.0))
     * }
     * ```
     *
     * @param query The query vector.
     * @param limit Maximum number of results.
     * @param filterBuilder Optional metadata filter DSL.
     * @return List of (id, similarity) pairs sorted by descending similarity.
     */
    suspend fun search(
        query: FloatArray,
        limit: Int = 5,
        filterBuilder: (VectorFilterBuilder.() -> Unit)? = null
    ): List<Pair<String, Float>> {
        val filter = if (filterBuilder != null) {
            VectorFilterBuilder().apply(filterBuilder).build()
        } else {
            VectorFilter.EMPTY
        }

        // Use HNSW for indexed search
        val mmap = mmapHnsw
        if (mmap != null) {
            return mmap.search(query, limit, filter)
        }
        if (hnsw.size() > 0) {
            return hnsw.search(query, limit, filter)
        }

        // Brute-force fallback for cold starts
        return bruteForceFallback(query, limit, filter)
    }

    /**
     * Searches a specific vector field (multi-vector search).
     *
     * ```kotlin
     * val results = collection.searchField("image", imageQuery, limit = 10)
     * ```
     */
    suspend fun searchField(
        field: String,
        query: FloatArray,
        limit: Int = 5,
        filterBuilder: (VectorFilterBuilder.() -> Unit)? = null
    ): List<Pair<String, Float>> {
        val filter = if (filterBuilder != null) {
            VectorFilterBuilder().apply(filterBuilder).build()
        } else {
            VectorFilter.EMPTY
        }

        val fieldIndex = fieldIndices[field]
        if (fieldIndex != null && fieldIndex.size() > 0) {
            return fieldIndex.search(query, limit, filter)
        }

        return emptyList()
    }

    // ========================================================================
    // DELETE & UPDATE
    // ========================================================================

    /**
     * Deletes a vector and its metadata from the collection.
     */
    suspend fun delete(id: String) {
        val batch = listOf(
            makeKey(id) to KoreDB.TOMBSTONE,
            makeMetaKey(id) to KoreDB.TOMBSTONE
        )
        db.writeBatchRaw(batch)
        indexingChannel.send(IndexingTask.Delete(id))
    }

    /**
     * Deletes multiple vectors in a batch.
     */
    suspend fun deleteBatch(ids: List<String>) {
        val batch = ids.flatMap { id ->
            listOf(
                makeKey(id) to KoreDB.TOMBSTONE,
                makeMetaKey(id) to KoreDB.TOMBSTONE
            )
        }
        db.writeBatchRaw(batch)
        ids.forEach { indexingChannel.send(IndexingTask.Delete(it)) }
    }

    /**
     * Updates a vector and optionally its metadata.
     */
    suspend fun update(id: String, vector: FloatArray, metadata: Map<String, Any>? = null) {
        db.putRaw(makeKey(id), VectorSerializer.toByteArray(vector))
        if (metadata != null) {
            db.putRaw(makeMetaKey(id), serializeMetadata(metadata))
        }
        indexingChannel.send(IndexingTask.Update(id, vector, metadata))
    }

    /**
     * Updates only the metadata without re-indexing the vector.
     */
    suspend fun updateMetadata(id: String, metadata: Map<String, Any>) {
        db.putRaw(makeMetaKey(id), serializeMetadata(metadata))
        hnsw.updateMetadata(id, metadata)
    }

    // ========================================================================
    // UTILITIES
    // ========================================================================

    /**
     * Retrieves the raw vector for a given ID.
     */
    suspend fun getVector(id: String): FloatArray? {
        val bytes = db.getRaw(makeKey(id)) ?: return null
        if (bytes.contentEquals(KoreDB.TOMBSTONE)) return null
        return VectorSerializer.fromByteArray(bytes)
    }

    /**
     * Retrieves raw vectors for multiple IDs in a batch.
     */
    suspend fun getBatchVectors(ids: Collection<String>): Map<String, FloatArray> {
        val result = mutableMapOf<String, FloatArray>()
        for (id in ids) {
            val vector = getVector(id)
            if (vector != null) {
                result[id] = vector
            }
        }
        return result
    }

    /**
     * Retrieves metadata for a given vector ID.
     */
    suspend fun getMetadata(id: String): Map<String, Any>? {
        val bytes = db.getRaw(makeMetaKey(id)) ?: return null
        if (bytes.contentEquals(KoreDB.TOMBSTONE)) return null
        return deserializeMetadata(bytes)
    }

    /**
     * Returns index health statistics.
     */
    fun stats(): HNSWIndex.IndexStats {
        ensureMutableHnsw()
        return hnsw.stats()
    }

    /**
     * Compacts the HNSW index by removing deleted nodes.
     * Call during idle time for best performance.
     */
    fun compactIndex(): Int {
        ensureMutableHnsw()
        return hnsw.compact()
    }

    /**
     * Returns the total number of indexed vectors.
     */
    fun size(): Int = mmapHnsw?.size() ?: hnsw.size()

    /**
     * Waits for all background indexing tasks to complete and saves to disk.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun waitForIndexing() {
        while (!indexingChannel.isEmpty) {
            delay(50)
        }
        // Allow the consumer to process remaining items
        delay(100)
        
        withContext(Dispatchers.IO) {
            if (hnsw.size() > 0) {
                hnsw.saveToDisk(indexFile)
            }
            // Save field indices
            for ((field, index) in fieldIndices) {
                val fieldFile = java.io.File(db.directory, "hnsw_${name}_${field}.bin")
                index.saveToDisk(fieldFile)
            }
        }
    }

    /**
     * Shuts down the background indexing worker.
     */
    fun close() {
        indexingChannel.close()
        indexingScope.cancel()
        mmapHnsw?.close()
    }

    // ========================================================================
    // INTERNAL
    // ========================================================================

    private fun escape(value: String): String {
        return value.replace("%", "%25").replace(":", "%3A")
    }

    private fun unescape(value: String): String {
        return value.replace("%3A", ":").replace("%25", "%")
    }

    private fun makeKey(id: String) = "$keyPrefix${escape(id)}".toByteArray(Charsets.UTF_8)
    private fun makeMetaKey(id: String) = "$metaPrefix${escape(id)}".toByteArray(Charsets.UTF_8)
    private fun makeFieldKey(id: String, field: String) = "${keyPrefix}f:${escape(field)}:${escape(id)}".toByteArray(Charsets.UTF_8)

    private suspend fun hydrateFromDisk() = withContext(Dispatchers.IO) {
        if (indexFile.exists()) {
            try {
                mmapHnsw = MmapHNSWIndex(indexFile)
            } catch (e: Exception) {
                try {
                    hnsw.loadFromDisk(indexFile)
                } catch (ex: Exception) {
                    indexFile.delete()
                }
            }
        }

        // Hydrate any new vectors not yet in the HNSW index
        val prefix = keyPrefix.toByteArray(Charsets.UTF_8)
        val rawKeys = db.getKeysByPrefixRaw(prefix)
        var addedNew = false

        for (keyBytes in rawKeys) {
            val fullKey = String(keyBytes, Charsets.UTF_8)
            if (fullKey.contains(":f:")) continue // Skip multi-vector field keys
            
            val escapedId = fullKey.removePrefix(keyPrefix)
            val id = unescape(escapedId)
            val exists = mmapHnsw?.contains(id) ?: hnsw.contains(id)
            if (exists) continue

            val value = db.getRaw(keyBytes)
            if (value != null && value.isNotEmpty() && !value.contentEquals(KoreDB.TOMBSTONE)) {
                ensureMutableHnsw()
                val vector = VectorSerializer.fromByteArray(value)
                val meta = loadMetadataFromDb(id)
                hnsw.insert(id, vector, VectorMath.getMagnitude(vector), meta)
                addedNew = true
            }
        }

        if (addedNew && hnsw.size() > 0) {
            hnsw.saveToDisk(indexFile)
        }
    }

    private suspend fun loadMetadataFromDb(id: String): Map<String, Any>? {
        val bytes = db.getRaw(makeMetaKey(id)) ?: return null
        if (bytes.contentEquals(KoreDB.TOMBSTONE)) return null
        return deserializeMetadata(bytes)
    }

    private suspend fun bruteForceFallback(
        query: FloatArray,
        limit: Int,
        filter: VectorFilter
    ): List<Pair<String, Float>> {
        val prefix = keyPrefix.toByteArray(Charsets.UTF_8)
        val rawResults = db.searchVectorsRaw(prefix, query, limit)

        return rawResults.mapNotNull {
            val escapedId = String(it.first, Charsets.UTF_8).removePrefix(keyPrefix)
            val id = unescape(escapedId)
            if (!filter.isEmpty()) {
                val meta = loadMetadataFromDb(id)
                if (!filter.matches(meta)) return@mapNotNull null
            }
            Pair(id, it.second)
        }
    }

    // ========================================================================
    // METADATA SERIALIZATION (Compact binary format)
    // ========================================================================

    private fun serializeMetadata(meta: Map<String, Any>): ByteArray {
        val buffer = java.io.ByteArrayOutputStream(256)
        val out = java.io.DataOutputStream(buffer)
        out.writeInt(meta.size)
        for ((key, value) in meta) {
            out.writeUTF(key)
            when (value) {
                is String  -> { out.writeByte(1); out.writeUTF(value) }
                is Int     -> { out.writeByte(2); out.writeInt(value) }
                is Long    -> { out.writeByte(3); out.writeLong(value) }
                is Float   -> { out.writeByte(4); out.writeFloat(value) }
                is Double  -> { out.writeByte(5); out.writeDouble(value) }
                is Boolean -> { out.writeByte(6); out.writeBoolean(value) }
                else       -> { out.writeByte(1); out.writeUTF(value.toString()) }
            }
        }
        return buffer.toByteArray()
    }

    private fun deserializeMetadata(bytes: ByteArray): Map<String, Any> {
        val input = java.io.DataInputStream(java.io.ByteArrayInputStream(bytes))
        val size = input.readInt()
        val meta = HashMap<String, Any>(size)
        repeat(size) {
            val key = input.readUTF()
            val type = input.readByte().toInt()
            meta[key] = when (type) {
                1 -> input.readUTF()
                2 -> input.readInt()
                3 -> input.readLong()
                4 -> input.readFloat()
                5 -> input.readDouble()
                6 -> input.readBoolean()
                else -> input.readUTF()
            }
        }
        return meta
    }
}
