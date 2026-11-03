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

import com.pankaj.koredb.core.SimdVectorMath
import java.util.concurrent.atomic.AtomicLong

/**
 * Operational telemetry for [KoreEmbeddingPipeline].
 */
data class EmbeddingPipelineStats(
    val hits: Long,
    val misses: Long,
    val hitRate: Double,
    val cachedEntries: Int
)

/**
 * Zero-dependency On-Device Embedding Pipeline & Connector (`com.pankaj.koredb.ai`).
 *
 * Wraps any user-supplied embedding lambda (`suspend (String) -> FloatArray` from MediaPipe TextEmbedder,
 * LiteRT / TFLite, ONNX Runtime, Gemini Nano, or remote APIs) and provides:
 * 1. **Thread-Safe LRU Embedding Cache**: Eliminates redundant NPU/CPU inference for repeated query terms,
 *    entity names, and L1 atomic propositions (~15ms model inference reduced to <1µs cache hit).
 * 2. **Matryoshka Representation Learning (MRL) Dimension Slicing**: Optionally truncates high-dimensional
 *    embeddings (e.g., 768 or 1024 dims) to a compact prefix ([targetDimensions], such as 64, 128, 256, or 384)
 *    before indexing.
 * 3. **16-Lane Unrolled L2 Normalization**: Automatically normalizes vectors (or sliced MRL prefixes) to unit
 *    length via [SimdVectorMath.normalizeL2InPlace16] so `COSINE` and `INNER_PRODUCT` run at peak accuracy.
 * 4. **Deduplicated Batch Execution ([embedBatch])**: Resolves cached entries first, deduplicates cache misses
 *    within the batch, and invokes the underlying model only once per unique miss.
 *
 * Usage:
 * ```kotlin
 * val pipeline = KoreEmbeddingPipeline(
 *     targetDimensions = 256,
 *     normalizeL2 = true,
 *     maxCacheEntries = 2048
 * ) { text ->
 *     mediaPipeEmbedder.embed(text)
 * }
 *
 * // Pass directly to GraphVectorBridge or KoreMemoryPyramid:
 * bridge.ingestDocumentWithChunks("doc_1", "Title", bodyText, embedder = pipeline.asEmbedder())
 * ```
 */
class KoreEmbeddingPipeline(
    val targetDimensions: Int? = null,
    val normalizeL2: Boolean = true,
    val maxCacheEntries: Int = 2048,
    val normalizeKeyWhitespace: Boolean = true,
    val defensiveCopy: Boolean = true,
    private val batchEmbedder: (suspend (List<String>) -> List<FloatArray>)? = null,
    private val embedder: suspend (String) -> FloatArray
) {
    init {
        require(targetDimensions == null || targetDimensions > 0) {
            "targetDimensions must be positive when specified, got $targetDimensions"
        }
        require(maxCacheEntries >= 0) {
            "maxCacheEntries must be non-negative, got $maxCacheEntries"
        }
    }

    private val cacheLock = Any()
    private val lruCache: LinkedHashMap<String, FloatArray> = object : LinkedHashMap<String, FloatArray>(
        minOf(maxCacheEntries.coerceAtLeast(16), 256),
        0.75f,
        true
    ) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, FloatArray>?): Boolean {
            return size > maxCacheEntries
        }
    }

    private val hitCounter = AtomicLong(0L)
    private val missCounter = AtomicLong(0L)

    /**
     * Computes (or retrieves from LRU cache) the post-processed embedding for [text].
     */
    suspend fun embed(text: String): FloatArray {
        val internal = embedInternal(text)
        return if (defensiveCopy) internal.clone() else internal
    }

    /**
     * Zero-copy read-only embedding lookup for internal callers that never mutate the returned `FloatArray`.
     */
    suspend fun embedReadOnly(text: String): FloatArray {
        return embedInternal(text)
    }

    /**
     * Operator overload allowing a [KoreEmbeddingPipeline] instance to be invoked directly (`pipeline("query")`).
     */
    suspend operator fun invoke(text: String): FloatArray = embed(text)

    /**
     * Returns a `suspend (String) -> FloatArray` function reference compatible with all KoreDB AI/RAG APIs.
     */
    fun asEmbedder(): suspend (String) -> FloatArray = ::embed

    /**
     * Embeds a batch of [texts], resolving cache hits first and deduplicating cache misses so the underlying
     * model is never invoked twice for the same text.
     */
    suspend fun embedBatch(texts: List<String>): List<FloatArray> {
        if (texts.isEmpty()) return emptyList()

        val results = arrayOfNulls<FloatArray>(texts.size)
        val missingKeyToIndices = LinkedHashMap<String, MutableList<Int>>()
        val missingKeyToRawText = LinkedHashMap<String, String>()

        if (maxCacheEntries > 0) {
            synchronized(cacheLock) {
                for (i in texts.indices) {
                    val raw = texts[i]
                    val key = canonicalizeKey(raw)
                    val cached = lruCache[key]
                    if (cached != null) {
                        hitCounter.incrementAndGet()
                        results[i] = if (defensiveCopy) cached.clone() else cached
                    } else {
                        missingKeyToIndices.getOrPut(key) { ArrayList(1) }.add(i)
                        missingKeyToRawText.putIfAbsent(key, raw)
                    }
                }
            }
        } else {
            for (i in texts.indices) {
                val raw = texts[i]
                val key = canonicalizeKey(raw)
                missingKeyToIndices.getOrPut(key) { ArrayList(1) }.add(i)
                missingKeyToRawText.putIfAbsent(key, raw)
            }
        }

        if (missingKeyToIndices.isEmpty()) {
            @Suppress("UNCHECKED_CAST")
            return results.asList() as List<FloatArray>
        }

        val totalMissingOccurrences = missingKeyToIndices.values.sumOf { it.size }
        val uniqueMissingKeys = missingKeyToRawText.keys.toList()
        val deduplicatedInBatchHits = (totalMissingOccurrences - uniqueMissingKeys.size).coerceAtLeast(0)
        missCounter.addAndGet(uniqueMissingKeys.size.toLong())
        if (deduplicatedInBatchHits > 0) {
            hitCounter.addAndGet(deduplicatedInBatchHits.toLong())
        }

        val uniqueMissingTexts = uniqueMissingKeys.map { missingKeyToRawText[it]!! }

        val rawVectors: List<FloatArray> = if (batchEmbedder != null) {
            val batchOut = batchEmbedder.invoke(uniqueMissingTexts)
            require(batchOut.size == uniqueMissingTexts.size) {
                "batchEmbedder returned ${batchOut.size} vectors for ${uniqueMissingTexts.size} inputs"
            }
            batchOut
        } else {
            uniqueMissingTexts.map { embedder(it) }
        }

        val processedVectors = ArrayList<FloatArray>(rawVectors.size)
        for (rawVec in rawVectors) {
            processedVectors.add(postProcessVector(rawVec))
        }

        if (maxCacheEntries > 0) {
            synchronized(cacheLock) {
                for (kIdx in uniqueMissingKeys.indices) {
                    lruCache[uniqueMissingKeys[kIdx]] = processedVectors[kIdx]
                }
            }
        }

        for (kIdx in uniqueMissingKeys.indices) {
            val key = uniqueMissingKeys[kIdx]
            val processed = processedVectors[kIdx]
            val targetPositions = missingKeyToIndices[key] ?: continue
            for (pos in targetPositions) {
                results[pos] = if (defensiveCopy) processed.clone() else processed
            }
        }

        @Suppress("UNCHECKED_CAST")
        return results.asList() as List<FloatArray>
    }

    /**
     * Pre-warms the LRU cache with pre-computed embeddings (applying MRL slicing and L2 normalization).
     */
    fun putAll(entries: Map<String, FloatArray>) {
        if (maxCacheEntries <= 0 || entries.isEmpty()) return
        synchronized(cacheLock) {
            for ((text, vec) in entries) {
                lruCache[canonicalizeKey(text)] = postProcessVector(vec)
            }
        }
    }

    /**
     * Evicts a specific text key from the LRU cache.
     */
    fun evict(text: String): Boolean {
        if (maxCacheEntries <= 0) return false
        val key = canonicalizeKey(text)
        return synchronized(cacheLock) {
            lruCache.remove(key) != null
        }
    }

    /**
     * Clears all cached embeddings.
     */
    fun clearCache() {
        synchronized(cacheLock) {
            lruCache.clear()
        }
    }

    /**
     * Resets hit/miss telemetry counters.
     */
    fun resetStats() {
        hitCounter.set(0L)
        missCounter.set(0L)
    }

    /**
     * Returns current cache hit/miss telemetry.
     */
    fun getStats(): EmbeddingPipelineStats {
        val h = hitCounter.get()
        val m = missCounter.get()
        val total = h + m
        val rate = if (total > 0L) h.toDouble() / total.toDouble() else 0.0
        val count = synchronized(cacheLock) { lruCache.size }
        return EmbeddingPipelineStats(
            hits = h,
            misses = m,
            hitRate = rate,
            cachedEntries = count
        )
    }

    private suspend fun embedInternal(text: String): FloatArray {
        val key = canonicalizeKey(text)
        if (maxCacheEntries > 0) {
            synchronized(cacheLock) {
                val cached = lruCache[key]
                if (cached != null) {
                    hitCounter.incrementAndGet()
                    return cached
                }
            }
        }

        missCounter.incrementAndGet()
        val rawVector = embedder(text)
        val processed = postProcessVector(rawVector)

        if (maxCacheEntries > 0) {
            synchronized(cacheLock) {
                lruCache[key] = processed
            }
        }
        return processed
    }

    private fun postProcessVector(raw: FloatArray): FloatArray {
        val targetDim = targetDimensions
        if (targetDim != null) {
            require(raw.size >= targetDim) {
                "Embedder returned vector of dimension ${raw.size}, which is smaller than targetDimensions ($targetDim)"
            }
            if (targetDim < raw.size) {
                return SimdVectorMath.sliceAndNormalizeL216(raw, targetDimensions = targetDim, normalize = normalizeL2)
            }
        }
        if (raw.isEmpty()) return FloatArray(0)
        val copy = raw.clone()
        if (normalizeL2) {
            SimdVectorMath.normalizeL2InPlace16(copy, copy.size)
        }
        return copy
    }

    private fun canonicalizeKey(text: String): String {
        if (!normalizeKeyWhitespace) return text
        val trimmed = text.trim()
        if (trimmed.isEmpty()) return ""
        val sb = StringBuilder(trimmed.length)
        var prevSpace = false
        for (i in 0 until trimmed.length) {
            val c = trimmed[i]
            if (c.isWhitespace()) {
                if (!prevSpace) {
                    sb.append(' ')
                    prevSpace = true
                }
            } else {
                sb.append(c)
                prevSpace = false
            }
        }
        return sb.toString()
    }
}
