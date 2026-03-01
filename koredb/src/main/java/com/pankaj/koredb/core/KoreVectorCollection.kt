package com.pankaj.koredb.core

import com.pankaj.koredb.engine.KoreDB
import com.pankaj.koredb.hnsw.HNSWIndex
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

/**
 * Manages a collection of vectors for similarity search.
 */
class KoreVectorCollection(
    val name: String,
    private val db: KoreDB
) {
    private val hnsw = HNSWIndex()
    
    // Background worker for HNSW indexing
    private val indexingScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val indexingChannel = Channel<Pair<String, FloatArray>>(Channel.UNLIMITED)

    init {
        // Start the background indexing and hydration worker
        indexingScope.launch {
            // 1. Hydrate from disk first
            hydrateFromDisk()
            
            // 2. Process new incoming vectors
            for ((id, vector) in indexingChannel) {
                hnsw.insert(id, vector, VectorMath.getMagnitude(vector))
            }
        }
    }

    /**
     * Scans the persistent storage to rebuild the HNSW index on startup.
     */
    private suspend fun hydrateFromDisk() = withContext(Dispatchers.IO) {
        val prefix = "vec:$name:".toByteArray(Charsets.UTF_8)
        val rawEntries = db.getKeysByPrefixRaw(prefix) // Get all vector keys
        
        rawEntries.chunked(500).forEach { batchKeys ->
            batchKeys.forEach { keyBytes ->
                val id = String(keyBytes, Charsets.UTF_8).removePrefix("vec:$name:")
                val value = db.getRaw(keyBytes)
                if (value != null && value.isNotEmpty()) {
                    val vector = VectorSerializer.fromByteArray(value)
                    hnsw.insert(id, vector, VectorMath.getMagnitude(vector))
                }
            }
            // Yield to avoid blocking other background tasks
            yield()
        }
    }

    private fun makeKey(id: String) = "vec:$name:$id".toByteArray(Charsets.UTF_8)

    /**
     * Inserts or updates a vector in the collection.
     * Returns in milliseconds as HNSW indexing happens in the background.
     */
    suspend fun insert(id: String, vector: FloatArray) {
        db.putRaw(makeKey(id), VectorSerializer.toByteArray(vector))
        indexingChannel.send(id to vector)
    }

    /**
     * Performs a batch insert of multiple vectors.
     * This is now ultra-fast as it only performs raw I/O.
     */
    suspend fun insertBatch(vectors: Map<String, FloatArray>) = coroutineScope {
        // 1. 🚀 Parallel Serialization: Use all cores to convert floats to bytes
        // This is the "Nitro" path that beats sequential loops.
        val batch = vectors.entries.chunked(2500).map { chunk ->
            async(Dispatchers.Default) {
                chunk.map { (id, vector) ->
                    makeKey(id) to VectorSerializer.toByteArray(vector)
                }
            }
        }.awaitAll().flatten()

        // 2. Write to raw storage (WAL/SSTable) - This is the durable part.
        db.writeBatchRaw(batch)
        
        // 3. Offload HNSW construction to background
        vectors.forEach { (id, vector) ->
            indexingChannel.send(id to vector)
        }
    }

    /**
     * Searches for the most similar vectors.
     * Uses HNSW for high-speed retrieval.
     */
    suspend fun search(query: FloatArray, limit: Int = 5): List<Pair<String, Float>> {
        // Use HNSW index for the bulk of the data
        if (hnsw.size() > 0) {
            return hnsw.search(query, limit)
        }

        // Fallback for cold starts
        val prefix = "vec:$name:".toByteArray(Charsets.UTF_8)
        val rawResults = db.searchVectorsRaw(prefix, query, limit)
        
        return rawResults.map { 
            val id = String(it.first, Charsets.UTF_8).removePrefix("vec:$name:")
            Pair(id, it.second)
        }
    }

    /**
     * Call this in tests or during app shutdown to ensure all vectors are indexed.
     */
    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    suspend fun waitForIndexing() {
        while (!indexingChannel.isEmpty) {
            delay(50)
        }
    }
}
