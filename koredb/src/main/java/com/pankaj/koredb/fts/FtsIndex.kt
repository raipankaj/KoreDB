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

package com.pankaj.koredb.fts

import com.pankaj.koredb.engine.KoreDB
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.PriorityQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * High-performance, embedded full-text inverted index with Okapi BM25 scoring.
 *
 * Features & Optimizations:
 * - Inverted posting lists in RAM with background LSM disk persistence.
 * - Top-K queries evaluated via bounded Min-Heap in O(M log K) without full result set sorting.
 * - Automatic cold-start hydration from underlying LSM storage segments.
 * - Real-time stale candidate purging on updates and deletes (zero phantom hits).
 *
 * @param collectionName The name of the parent collection.
 * @param db The underlying storage engine for persistence.
 * @param scorer The BM25 scoring parameter configuration.
 */
class FtsIndex(
    val collectionName: String,
    private val db: KoreDB,
    private val scorer: BM25Scorer = BM25Scorer()
) {

    // Term -> Map<DocId, TermFrequency>
    private val postings = ConcurrentHashMap<String, ConcurrentHashMap<String, Int>>()
    
    // DocId -> TotalTokenCount
    private val docLengths = ConcurrentHashMap<String, Int>()

    // DocId -> Set<Term> for clean update tracking
    private val docTerms = ConcurrentHashMap<String, MutableSet<String>>()

    // Global corpus statistics
    private val totalTokens = AtomicLong(0)
    private val docCount = AtomicLong(0)

    private val isHydrated = AtomicBoolean(false)

    private val ftsPrefix = "fts:$collectionName:".toByteArray(Charsets.UTF_8)
    private val ftsLenPrefix = "ftslen:$collectionName:".toByteArray(Charsets.UTF_8)

    /**
     * Ensures inverted index is hydrated from disk storage if starting cold.
     */
    fun ensureHydrated() {
        if (isHydrated.get()) return
        synchronized(this) {
            if (isHydrated.get()) return
            try {
                // 1. Hydrate document lengths
                val lenEntries = db.getByPrefixWithKeysRaw(ftsLenPrefix)
                for ((k, v) in lenEntries) {
                    if (v.size >= 4) {
                        val docId = String(k.copyOfRange(ftsLenPrefix.size, k.size), Charsets.UTF_8)
                        val length = ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).getInt()
                        docLengths[docId] = length
                        totalTokens.addAndGet(length.toLong())
                        docCount.incrementAndGet()
                    }
                }

                // 2. Hydrate inverted postings
                val ftsEntries = db.getByPrefixWithKeysRaw(ftsPrefix)
                for ((k, v) in ftsEntries) {
                    if (v.size >= 4) {
                        val keyStr = String(k.copyOfRange(ftsPrefix.size, k.size), Charsets.UTF_8)
                        val colonIdx = keyStr.indexOf(':')
                        if (colonIdx > 0) {
                            val term = keyStr.substring(0, colonIdx)
                            val docId = keyStr.substring(colonIdx + 1)
                            val freq = ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).getInt()
                            postings.getOrPut(term) { ConcurrentHashMap() }[docId] = freq
                            docTerms.getOrPut(docId) { ConcurrentHashMap.newKeySet() }.add(term)
                        }
                    }
                }
            } catch (_: Exception) {
                // Ignore empty or uninitialized collections
            } finally {
                isHydrated.set(true)
            }
        }
    }

    /**
     * Indexes text for a given document.
     *
     * @param docId The unique document identifier.
     * @param text The concatenated text from searchable fields.
     * @param batch Optional list to collect LSM key-value pairs for atomic disk write.
     */
    fun indexDocument(
        docId: String,
        text: String,
        batch: MutableList<Pair<ByteArray, ByteArray>>? = null
    ) {
        val (frequencies, length) = KoreTokenizer.tokenize(text)

        // 1. Purge previous terms if updating existing document
        val newTermSet = ConcurrentHashMap.newKeySet<String>().apply { addAll(frequencies.keys) }
        val oldTerms = docTerms.put(docId, newTermSet)
        if (oldTerms != null) {
            for (oldTerm in oldTerms) {
                if (!frequencies.containsKey(oldTerm)) {
                    postings[oldTerm]?.remove(docId)
                    if (batch != null) {
                        batch.add(makePostingKey(oldTerm, docId) to KoreDB.TOMBSTONE)
                    }
                }
            }
        }

        val oldLength = docLengths.put(docId, length)
        if (oldLength != null) {
            totalTokens.addAndGet((length - oldLength).toLong())
        } else {
            totalTokens.addAndGet(length.toLong())
            docCount.incrementAndGet()
        }

        // 2. Update Inverted Postings in RAM & LSM Batch
        val lenBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(length).array()
        val docLenKey = makeLenKey(docId)
        batch?.add(docLenKey to lenBytes)

        for ((term, freq) in frequencies) {
            val docMap = postings.getOrPut(term) { ConcurrentHashMap() }
            docMap[docId] = freq

            if (batch != null) {
                val freqBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(freq).array()
                val postingKey = makePostingKey(term, docId)
                batch.add(postingKey to freqBytes)
            }
        }
    }

    /**
     * Executes a BM25 full-text keyword query using bounded Min-Heap top-K selection.
     *
     * @param query Raw search query string.
     * @param limit Maximum number of top-matching results to return.
     * @return A list of (docId, bm25Score) pairs sorted by score descending.
     */
    fun search(query: String, limit: Int = 10): List<Pair<String, Float>> {
        ensureHydrated()
        val queryTerms = KoreTokenizer.tokenizeQuery(query)
        if (queryTerms.isEmpty()) return emptyList()

        val nDocs = maxOf(1L, docCount.get())
        val avgDl = if (nDocs > 0) totalTokens.get().toFloat() / nDocs else 1f

        // Document ID -> Accumulated BM25 Score
        val scoreAccumulator = HashMap<String, Float>()

        for (term in queryTerms) {
            val docMap = postings[term] ?: continue
            val idf = scorer.calculateIDF(nDocs, docMap.size.toLong())

            for ((docId, freq) in docMap) {
                val dLen = docLengths[docId] ?: avgDl.toInt()
                val termScore = scorer.scoreTerm(idf, freq, dLen, avgDl)
                scoreAccumulator[docId] = (scoreAccumulator[docId] ?: 0f) + termScore
            }
        }

        if (scoreAccumulator.isEmpty()) return emptyList()

        // Bounded Min-Heap: O(M log K) top-K selection
        val minHeap = PriorityQueue<Pair<String, Float>>(limit + 1, compareBy { it.second })
        for (entry in scoreAccumulator) {
            minHeap.offer(entry.key to entry.value)
            if (minHeap.size > limit) {
                minHeap.poll()
            }
        }

        val results = ArrayList<Pair<String, Float>>(minHeap.size)
        while (minHeap.isNotEmpty()) {
            results.add(minHeap.poll())
        }
        results.reverse() // Descending order
        return results
    }

    /**
     * Removes a document from the full-text index.
     */
    fun removeDocument(docId: String, batch: MutableList<Pair<ByteArray, ByteArray>>? = null) {
        val oldTerms = docTerms.remove(docId)
        if (oldTerms != null) {
            for (term in oldTerms) {
                postings[term]?.remove(docId)
                if (batch != null) {
                    batch.add(makePostingKey(term, docId) to KoreDB.TOMBSTONE)
                }
            }
        }

        val oldLength = docLengths.remove(docId) ?: return
        totalTokens.addAndGet(-oldLength.toLong())
        docCount.decrementAndGet()

        if (batch != null) {
            batch.add(makeLenKey(docId) to KoreDB.TOMBSTONE)
        }
    }

    /**
     * Clears all in-memory inverted postings and corpus statistics.
     */
    fun clear() {
        postings.clear()
        docLengths.clear()
        docTerms.clear()
        totalTokens.set(0)
        docCount.set(0)
    }

    private fun makePostingKey(term: String, docId: String): ByteArray {
        val termBytes = term.toByteArray(Charsets.UTF_8)
        val docIdBytes = docId.toByteArray(Charsets.UTF_8)
        val key = ByteArray(ftsPrefix.size + termBytes.size + 1 + docIdBytes.size)
        System.arraycopy(ftsPrefix, 0, key, 0, ftsPrefix.size)
        var pos = ftsPrefix.size
        System.arraycopy(termBytes, 0, key, pos, termBytes.size)
        pos += termBytes.size
        key[pos++] = ':'.code.toByte()
        System.arraycopy(docIdBytes, 0, key, pos, docIdBytes.size)
        return key
    }

    private fun makeLenKey(docId: String): ByteArray {
        val idBytes = docId.toByteArray(Charsets.UTF_8)
        val key = ByteArray(ftsLenPrefix.size + idBytes.size)
        System.arraycopy(ftsLenPrefix, 0, key, 0, ftsLenPrefix.size)
        System.arraycopy(idBytes, 0, key, ftsLenPrefix.size, idBytes.size)
        return key
    }
}
