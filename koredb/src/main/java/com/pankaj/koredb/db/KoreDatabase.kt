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
class KoreDatabase(
    private val directory: File,
    private val crypto: com.pankaj.koredb.crypto.KoreCrypto? = null,
    private val compressionCodec: com.pankaj.koredb.compression.CompressionCodec = com.pankaj.koredb.compression.Lz4CompressionCodec(),
    val targetSchemaVersion: Int = 1,
    val minFreeSpaceBytes: Long = 10 * 1024 * 1024L,
    val enableCdc: Boolean = true,
    private val onMigrate: ((db: KoreDatabase, oldVersion: Int, newVersion: Int) -> Unit)? = null
) {

    /**
     * The underlying storage engine. 
     * Initialized lazily to ensure [KoreAndroid.create] is non-blocking on the UI thread.
     */
    val engine: KoreDB by lazy { KoreDB(directory, crypto, compressionCodec, minFreeSpaceBytes) }
    val cdcManager: com.pankaj.koredb.cdc.CdcManager by lazy { com.pankaj.koredb.cdc.CdcManager(engine, enabled = enableCdc) }
    
    private val collections = ConcurrentHashMap<String, KoreCollection<*>>()
    var onCloseCallback: (() -> Unit)? = null
    @Volatile
    private var isMigrated = false
    @Volatile
    private var isDecryptionFailure = false

    companion object {
        private val SCHEMA_VERSION_KEY = "__koredb_schema_version__".toByteArray(Charsets.UTF_8)

        /**
         * Creates a transient in-memory [KoreDatabase] instance for fast unit testing.
         * Automatically cleans up its temporary storage when [close] is called.
         */
        fun inMemory(): KoreDatabase {
            val tempDir = java.nio.file.Files.createTempDirectory("koredb_mem_").toFile()
            val db = KoreDatabase(tempDir)
            db.onCloseCallback = {
                tempDir.deleteRecursively()
            }
            return db
        }
    }

    init {
        if (onMigrate != null) {
            ensureMigrated()
        }
    }

    private fun ensureMigrated() {
        if (!isMigrated) {
            synchronized(this) {
                if (!isMigrated) {
                    isMigrated = true
                    val currentVer = getStoredSchemaVersion()
                    if (!isDecryptionFailure && currentVer < targetSchemaVersion) {
                        onMigrate?.invoke(this, currentVer, targetSchemaVersion)
                        setSchemaVersion(targetSchemaVersion)
                        kotlinx.coroutines.runBlocking {
                            engine.flushMemTableInternal()
                        }
                    }
                }
            }
        }
    }

    fun getStoredSchemaVersion(): Int {
        val bytes = try {
            engine.getRaw(SCHEMA_VERSION_KEY)
        } catch (e: com.pankaj.koredb.engine.CorruptionException) {
            isDecryptionFailure = true
            null
        } catch (_: Throwable) {
            null
        } ?: return 0
        return String(bytes, Charsets.UTF_8).toIntOrNull() ?: 0
    }

    val schemaVersion: Int 
        get() {
            ensureMigrated()
            return getStoredSchemaVersion()
        }

    private fun setSchemaVersion(version: Int) {
        kotlinx.coroutines.runBlocking {
            engine.putRaw(SCHEMA_VERSION_KEY, version.toString().toByteArray(Charsets.UTF_8))
        }
    }

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

    /**
     * Retrieves all mutations recorded at or after [sinceTimestamp], ordered chronologically.
     */
    fun getMutationsSince(sinceTimestamp: Long, limit: Int = 1000): List<com.pankaj.koredb.cdc.MutationRecord> =
        cdcManager.getMutationsSince(sinceTimestamp, limit)

    /**
     * Prunes acknowledged mutations up to [upToSequence] to free disk space.
     */
    fun acknowledgeMutations(upToSequence: Long) =
        cdcManager.acknowledgeMutations(upToSequence)

    /**
     * Registers a listener for real-time mutation streaming to sync workers.
     */
    fun registerMutationListener(listener: com.pankaj.koredb.cdc.MutationListener) =
        cdcManager.registerListener(listener)

    /**
     * Unregisters a previously registered mutation listener.
     */
    fun unregisterMutationListener(listener: com.pankaj.koredb.cdc.MutationListener) =
        cdcManager.unregisterListener(listener)

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> collection(
        name: String, 
        serializer: KSerializer<T>
    ): KoreCollection<T> {
        ensureMigrated()
        val cacheKey = "$name:${serializer.descriptor.serialName}"
        return collections.getOrPut(cacheKey) {
            val koreSerializer = KotlinxKoreSerializer(serializer)
            val col = KoreCollection(name, engine, koreSerializer)
            col.cdcManager = cdcManager
            com.pankaj.koredb.core.KoreAnnotationIndexer.autoConfigure(col, serializer)
            col
        } as KoreCollection<T>
    }

    /**
     * Retrieves or creates a high-performance binary [KoreCollection] for type [T] using CBOR serialization.
     * Yields up to 4x higher throughput and 50-70% smaller on-disk records compared to JSON.
     */
    inline fun <reified T : Any> binaryCollection(name: String) =
        binaryCollection(name, serializer<T>())

    @OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> binaryCollection(
        name: String,
        serializer: KSerializer<T>
    ): KoreCollection<T> {
        ensureMigrated()
        val cacheKey = "binary:$name:${serializer.descriptor.serialName}"
        return collections.getOrPut(cacheKey) {
            val koreSerializer = com.pankaj.koredb.core.CborKoreSerializer(serializer)
            val col = KoreCollection(name, engine, koreSerializer)
            col.cdcManager = cdcManager
            com.pankaj.koredb.core.KoreAnnotationIndexer.autoConfigure(col, serializer)
            col
        } as KoreCollection<T>
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> collection(
        name: String,
        serializer: com.pankaj.koredb.core.KoreSerializer<T>
    ): KoreCollection<T> {
        ensureMigrated()
        val cacheKey = "$name:${serializer.serialName}"
        return collections.getOrPut(cacheKey) {
            val col = KoreCollection(name, engine, serializer)
            col.cdcManager = cdcManager
            col
        } as KoreCollection<T>
    }

    /**
     * Clears cached collection instances. Useful during schema migrations.
     */
    fun clearCollectionCache() {
        collections.clear()
    }

    /**
     * Clears in-memory document caches across all active collections.
     */
    fun clearDocumentCaches() {
        collections.values.forEach { it.clearCache() }
    }

    /**
     * Selectively invalidates modified keys from the document caches.
     */
    fun invalidateCachedDocuments(keys: Set<String>) {
        for (key in keys) {
            if (key.startsWith("doc:")) {
                val parts = key.split(":")
                if (parts.size >= 3) {
                    val collName = parts[1]
                    val id = parts.subList(2, parts.size).joinToString(":")
                    for ((k, col) in collections) {
                        if (k == collName || k.startsWith("$collName:")) {
                            col.invalidateCache(id)
                        }
                    }
                }
            }
        }
    }

    /**
     * Completely drops a collection by deleting all its documents, secondary indices,
     * numeric indices, FTS indices, and in-memory caches.
     *
     * @param name The unique name of the collection to drop.
     */
    suspend fun dropCollection(name: String) {
        val matchingCols = collections.filterKeys { it.startsWith("$name:") || it == name }
        if (matchingCols.isNotEmpty()) {
            for ((_, col) in matchingCols) {
                col.deleteAll()
            }
        } else {
            val dummySerializer = KotlinxKoreSerializer(kotlinx.serialization.builtins.ByteArraySerializer())
            val tempCol = KoreCollection(name, engine, dummySerializer)
            tempCol.deleteAll()
        }
        for (k in matchingCols.keys) {
            collections.remove(k)
        }
    }

    private val vectorCollections = ConcurrentHashMap<String, KoreVectorCollection>()

    /**
     * Retrieves or creates a [KoreVectorCollection] for similarity search.
     *
     * @param name The unique name of the vector collection.
     * @return A collection instance with default configuration.
     */
    fun vectorCollection(name: String): KoreVectorCollection {
        return vectorCollections.getOrPut(name) {
            KoreVectorCollection(name, engine)
        }
    }

    /**
     * Creates or retrieves a [KoreVectorCollection] with custom configuration.
     *
     * Usage:
     * ```kotlin
     * val collection = db.vectorCollection("products") {
     *     dimensions = 768
     *     metric = DistanceMetric.COSINE
     *     quantization = true
     *     efSearch = 100
     *     namespace = "user_123"
     * }
     * ```
     *
     * @param name The unique name of the vector collection.
     * @param configure Configuration builder lambda.
     * @return A configured vector collection instance.
     */
    fun vectorCollection(
        name: String,
        configure: com.pankaj.koredb.core.VectorCollectionConfig.Builder.() -> Unit
    ): KoreVectorCollection {
        return vectorCollections.getOrPut(name) {
            val config = com.pankaj.koredb.core.VectorCollectionConfig.Builder().apply(configure).build()
            KoreVectorCollection(name, engine, config)
        }
    }

    /**
     * Creates or retrieves a [KoreVectorCollection] specifying dimensions and metric.
     */
    fun vectorCollection(
        name: String,
        dimensions: Int = 0,
        metric: com.pankaj.koredb.hnsw.DistanceMetric = com.pankaj.koredb.hnsw.DistanceMetric.COSINE
    ): KoreVectorCollection = vectorCollection(name) {
        if (dimensions > 0) this.dimensions = dimensions
        this.metric = metric
    }

    val mvccManager = com.pankaj.koredb.engine.mvcc.MvccManager()

    /**
     * Begins an explicit MVCC transaction under Snapshot Isolation.
     */
    fun beginTransaction(): com.pankaj.koredb.engine.mvcc.MvccTransaction {
        val snapshot = mvccManager.beginSnapshot()
        return com.pankaj.koredb.engine.mvcc.MvccTransaction(this, snapshot, mvccManager)
    }

    /**
     * Executes a transactional block under Snapshot Isolation.
     * Writes are buffered and atomically committed upon block return.
     * Throws [com.pankaj.koredb.engine.mvcc.MvccConflictException] if a concurrent transaction
     * committed conflicting mutations on the same keys.
     */
    fun <R> transaction(block: (com.pankaj.koredb.engine.mvcc.MvccTransaction) -> R): R {
        val tx = beginTransaction()
        try {
            val result = block(tx)
            tx.commit()
            return result
        } catch (e: Exception) {
            tx.rollback()
            throw e
        }
    }

    /**
     * Executes a comprehensive database integrity check across all active SSTables,
     * sparse indices, and WAL logs (analogous to SQLite's `PRAGMA integrity_check`).
     *
     * @return An [IntegrityReport] containing health status and diagnostics.
     */
    fun verifyIntegrity(): com.pankaj.koredb.engine.IntegrityReport =
        com.pankaj.koredb.engine.IntegrityVerifier.verify(directory)

    /**
     * Executes a full leveled compaction across all SSTables, purging tombstones
     * and reclaiming disk storage (analogous to SQLite's `VACUUM`).
     */
    fun compact() {
        engine.compact()
    }

    /**
     * Closes the database and releases all underlying resources.
     *
     * After calling this method, further operations on the database or its
     * collections may fail or result in undefined behavior.
     */
    fun close(flushMemTable: Boolean = true) {
        onCloseCallback?.invoke()
        onCloseCallback = null
        vectorCollections.values.forEach { it.close() }
        vectorCollections.clear()
        engine.close(flushMemTable)
    }

    fun graph(): GraphStorage {
        return GraphStorage(engine)
    }

    /**
     * Creates a unified Graph + Vector query bridge.
     *
     * This is KoreDB's **unique differentiator** — no other database offers
     * combined graph traversal + vector similarity in a single query.
     *
     * ```kotlin
     * val bridge = db.graphVectorBridge(vectorCollection)
     * val results = bridge.vectorSearch(query, limit = 50)
     *     .filterByGraph { graph.getOutboundTargetIds(it, "CATEGORY").contains("shoes") }
     * ```
     */
    fun graphVectorBridge(vectorCollection: com.pankaj.koredb.core.KoreVectorCollection): com.pankaj.koredb.bridge.GraphVectorBridge {
        return com.pankaj.koredb.bridge.GraphVectorBridge(graph(), vectorCollection)
    }

    /**
     * Creates or retrieves a 4-Layer Cognitive Memory Pyramid ([com.pankaj.koredb.memory.KoreMemoryPyramid]).
     * Coordinates L0 raw LZ4 evidence, L1 dual-indexed atomic facts, L2 scenario clusters, and L3 persona/invariants.
     */
    fun memoryPyramid(
        name: String = "default"
    ): com.pankaj.koredb.memory.KoreMemoryPyramid {
        val evidenceCol = binaryCollection<com.pankaj.koredb.memory.L0Evidence>("koredb_memory_${name}_l0_evidence")
        val vecCol = vectorCollection("koredb_memory_${name}_vectors") {
            dimensions = 0
            metric = com.pankaj.koredb.hnsw.DistanceMetric.COSINE
        }
        val fts = com.pankaj.koredb.fts.FtsIndex("koredb_memory_${name}_facts", engine)
        return com.pankaj.koredb.memory.KoreMemoryPyramid(
            db = this,
            graph = graph(),
            evidenceCollection = evidenceCol,
            vectorCollection = vecCol,
            ftsIndex = fts
        )
    }

    /**
     * Creates an Inspection Mirror ([com.pankaj.koredb.memory.KoreMemoryInspector]) for generating Markdown reports
     * and live Mermaid visualizations of the cognitive memory state.
     */
    fun memoryInspector(
        pyramid: com.pankaj.koredb.memory.KoreMemoryPyramid? = null
    ): com.pankaj.koredb.memory.KoreMemoryInspector {
        return com.pankaj.koredb.memory.KoreMemoryInspector(pyramid ?: memoryPyramid())
    }

    /**
     * Creates or retrieves an On-Device Semantic LLM Response Cache ([com.pankaj.koredb.ai.KoreSemanticCache]).
     *
     * @param name Logical name for the semantic cache (default `"semantic_cache"`).
     * @param dimensions Embedding dimensionality (optional; required if `quantization = true`).
     * @param similarityThreshold Minimum cosine similarity for a cache hit (default `0.90f`).
     * @param defaultTtlMs Optional default TTL in milliseconds for cached LLM responses.
     */
    fun semanticCache(
        name: String = "semantic_cache",
        dimensions: Int = 0,
        similarityThreshold: Float = 0.90f,
        defaultTtlMs: Long? = null
    ): com.pankaj.koredb.ai.KoreSemanticCache {
        val entryCol = binaryCollection<com.pankaj.koredb.ai.SemanticCacheEntry>("scache_doc_$name")
        val vecCol = vectorCollection("scache_vec_$name") {
            this.dimensions = dimensions
            this.metric = com.pankaj.koredb.hnsw.DistanceMetric.COSINE
        }
        return com.pankaj.koredb.ai.KoreSemanticCache(
            entryCollection = entryCol,
            vectorCollection = vecCol,
            defaultSimilarityThreshold = similarityThreshold,
            defaultTtlMs = defaultTtlMs
        )
    }

    /**
     * Creates a zero-dependency On-Device Embedding Pipeline ([com.pankaj.koredb.ai.KoreEmbeddingPipeline])
     * with LRU caching, multi-resolution prefix dimension slicing, and 16-lane L2 normalization.
     */
    fun embeddingPipeline(
        targetDimensions: Int? = null,
        normalizeL2: Boolean = true,
        maxCacheEntries: Int = 2048,
        batchEmbedder: (suspend (List<String>) -> List<FloatArray>)? = null,
        embedder: suspend (String) -> FloatArray
    ): com.pankaj.koredb.ai.KoreEmbeddingPipeline {
        return com.pankaj.koredb.ai.KoreEmbeddingPipeline(
            targetDimensions = targetDimensions,
            normalizeL2 = normalizeL2,
            maxCacheEntries = maxCacheEntries,
            batchEmbedder = batchEmbedder,
            embedder = embedder
        )
    }

    /**
     * Retrieves or creates a Key-Value cache instance.
     * Offers the highest performance for storing raw primitives/byte arrays.
     */
    fun keyValue(name: String): com.pankaj.koredb.kv.KoreKeyValue {
        return com.pankaj.koredb.kv.KoreKeyValue(name, engine)
    }

    /**
     * Retrieves or creates a reactive Event Stream for Pub/Sub messaging.
     */
    fun eventStream(topicName: String): com.pankaj.koredb.stream.KoreEventStream {
        return com.pankaj.koredb.stream.KoreEventStream(topicName, engine)
    }

    /**
     * Advanced: Deletes a raw key from the underlying storage.
     * Useful for debugging or manual cleanup.
     */
    suspend fun deleteRaw(key: ByteArray) {
        engine.deleteRaw(key)
    }

    /**
     * Retrieves runtime storage engine and operational metrics.
     */
    fun getMetrics(): com.pankaj.koredb.engine.KoreDBMetrics {
        return engine.getMetrics()
    }

    /**
     * Creates a consistent point-in-time snapshot backup in [destDir].
     */
    suspend fun createBackup(destDir: File): com.pankaj.koredb.engine.BackupMetadata {
        return engine.createBackup(destDir)
    }

    /**
     * Restores database state from a backup directory.
     * Clears all cached collections and reloads the engine.
     */
    suspend fun restoreFromBackup(srcDir: File): Boolean {
        collections.clear()
        return engine.restoreFromBackup(srcDir)
    }

    fun deleteAllRaw() {
        // 1. Clear the cached collection objects
        collections.clear()

        // 2. Tell the engine to wipe the disk
        engine.nuke()
    }
}
