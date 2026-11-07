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

package com.pankaj.koredb.ai

import com.pankaj.koredb.core.KoreCollection
import com.pankaj.koredb.core.KoreIndex
import com.pankaj.koredb.core.KoreVectorCollection
import com.pankaj.koredb.hnsw.eq
import kotlinx.serialization.Serializable
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * Persisted entry in [KoreSemanticCache].
 */
@Serializable
data class SemanticCacheEntry(
    val id: String,
    val prompt: String,
    val response: String,
    @KoreIndex val contextTag: String = "default",
    val createdAtMs: Long = System.currentTimeMillis()
)

/**
 * Result returned on a semantic cache hit.
 */
data class SemanticCacheHit(
    val entry: SemanticCacheEntry,
    val similarity: Float
)

/**
 * Operational telemetry for [KoreSemanticCache].
 */
data class SemanticCacheStats(
    val hits: Long,
    val misses: Long,
    val hitRate: Double
)

/**
 * On-Device Semantic LLM Response Cache backed by [KoreVectorCollection] and [KoreCollection].
 *
 * Intercepts semantically equivalent user prompts (e.g., *"What's my flight status?"* vs.
 * *"Check the status of my flight"*) before invoking expensive on-device NPU/GPU SLM inference
 * or remote LLM APIs.
 *
 * Features:
 * - **Cosine Similarity Thresholding**: Configurable `defaultSimilarityThreshold` (default `0.90f`).
 * - **Context / Model Tagging**: Partition cache entries by system prompt, persona, or model version (`contextTag`).
 * - **Automatic TTL Expiration**: Entries stored with `ttlMs` automatically expire and are pruned on lookup.
 * - **Get-Or-Generate Helper**: `getOrGenerate(prompt, promptVector) { ... }` for one-line LLM wrapping.
 */
class KoreSemanticCache(
    private val entryCollection: KoreCollection<SemanticCacheEntry>,
    private val vectorCollection: KoreVectorCollection,
    val defaultSimilarityThreshold: Float = 0.90f,
    val defaultTtlMs: Long? = null
) {
    private val hitCounter = AtomicLong(0)
    private val missCounter = AtomicLong(0)

    /**
     * Looks up a cached LLM response whose prompt embedding has similarity $\ge$ [minSimilarity].
     *
     * @param promptVector Embedding of the incoming user prompt.
     * @param minSimilarity Minimum similarity score required for a cache hit (defaults to [defaultSimilarityThreshold]).
     * @param contextTag Optional context/system-prompt partition tag to match.
     * @return [SemanticCacheHit] if a valid, non-expired entry exceeds the threshold, or `null` on a miss.
     */
    suspend fun lookup(
        promptVector: FloatArray,
        minSimilarity: Float = defaultSimilarityThreshold,
        contextTag: String? = null
    ): SemanticCacheHit? {
        vectorCollection.waitForIndexing()

        val candidates = if (contextTag != null) {
            vectorCollection.search(promptVector, limit = 5) {
                where("contextTag", eq(contextTag))
            }
        } else {
            vectorCollection.search(promptVector, limit = 5)
        }

        for ((id, similarity) in candidates) {
            if (similarity < minSimilarity) continue
            val entry = entryCollection.getById(id)
            if (entry == null) {
                // Entry expired via TTL or was deleted; clean up orphaned vector asynchronously
                vectorCollection.delete(id)
                continue
            }
            if (contextTag != null && entry.contextTag != contextTag) {
                continue
            }
            hitCounter.incrementAndGet()
            return SemanticCacheHit(entry = entry, similarity = similarity)
        }

        missCounter.incrementAndGet()
        return null
    }

    /**
     * Stores a prompt-response pair and its embedding in the semantic cache.
     *
     * @param prompt Original prompt text.
     * @param promptVector Embedding vector of the prompt.
     * @param response Generated LLM response to cache.
     * @param contextTag Optional context/model tag (default `"default"`).
     * @param ttlMs Optional time-to-live in milliseconds (defaults to [defaultTtlMs]).
     * @param id Optional deterministic ID (auto-generated UUID if omitted).
     * @return The cache entry ID.
     */
    suspend fun store(
        prompt: String,
        promptVector: FloatArray,
        response: String,
        contextTag: String = "default",
        ttlMs: Long? = defaultTtlMs,
        id: String = UUID.randomUUID().toString()
    ): String {
        val entry = SemanticCacheEntry(
            id = id,
            prompt = prompt,
            response = response,
            contextTag = contextTag,
            createdAtMs = System.currentTimeMillis()
        )
        if (ttlMs != null && ttlMs > 0) {
            entryCollection.insert(id, entry, ttlSeconds = ((ttlMs + 999L) / 1000L).coerceAtLeast(1L))
        } else {
            entryCollection.insert(id, entry)
        }
        vectorCollection.insert(
            id = id,
            vector = promptVector,
            metadata = mapOf("contextTag" to contextTag)
        )
        return id
    }

    /**
     * Returns a cached response if a semantic match $\ge$ [minSimilarity] exists,
     * or executes [generator], caches the result, and returns it.
     */
    suspend inline fun getOrGenerate(
        prompt: String,
        promptVector: FloatArray,
        minSimilarity: Float = defaultSimilarityThreshold,
        contextTag: String = "default",
        ttlMs: Long? = defaultTtlMs,
        crossinline generator: suspend () -> String
    ): Pair<String, Boolean> {
        val hit = lookup(promptVector, minSimilarity = minSimilarity, contextTag = contextTag)
        if (hit != null) {
            return hit.entry.response to true
        }
        val generated = generator()
        store(
            prompt = prompt,
            promptVector = promptVector,
            response = generated,
            contextTag = contextTag,
            ttlMs = ttlMs
        )
        return generated to false
    }

    /**
     * Invalidates a specific cache entry by ID.
     */
    suspend fun invalidate(id: String) {
        entryCollection.delete(id)
        vectorCollection.delete(id)
    }

    /**
     * Invalidates all cached entries matching [contextTag].
     */
    suspend fun invalidateByContext(contextTag: String) {
        val matching = entryCollection.getByIndex("contextTag", contextTag)
        if (matching.isEmpty()) return
        val ids = matching.map { it.id }
        entryCollection.deleteBatch(ids)
        vectorCollection.deleteBatch(ids)
    }

    /**
     * Returns cache hit/miss telemetry.
     */
    fun getStats(): SemanticCacheStats {
        val h = hitCounter.get()
        val m = missCounter.get()
        val total = h + m
        val rate = if (total > 0) h.toDouble() / total.toDouble() else 0.0
        return SemanticCacheStats(hits = h, misses = m, hitRate = rate)
    }
}
