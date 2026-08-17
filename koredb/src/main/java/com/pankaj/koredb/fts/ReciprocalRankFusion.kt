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

/**
 * Production-grade Reciprocal Rank Fusion (RRF) algorithm.
 *
 * Combines ranked lists from heterogeneous retrieval systems (e.g. BM25 keyword search
 * and dense vector similarity search) into a single, high-precision unified ranking.
 */
object ReciprocalRankFusion {

    /**
     * Default smoothing constant recommended by research literature (Cormack et al.).
     */
    const val DEFAULT_K: Int = 60

    /**
     * Fuses BM25 and Vector search results using Reciprocal Rank Fusion.
     *
     * @param bm25Results Ranked candidate list from BM25 search (docId to BM25 score).
     * @param vectorResults Ranked candidate list from Vector search (docId to similarity score).
     * @param limit Maximum number of fused results to return.
     * @param k Smoothing constant to control the penalty for lower ranks (default 60).
     * @param bm25Weight Relative weight for BM25 ranking contribution (default 1.0).
     * @param vectorWeight Relative weight for Vector ranking contribution (default 1.0).
     * @return Combined list of (docId, rrfScore) sorted in descending order of fused score.
     */
    fun fuse(
        bm25Results: List<Pair<String, Float>>,
        vectorResults: List<Pair<String, Float>>,
        limit: Int = 10,
        k: Int = DEFAULT_K,
        bm25Weight: Float = 1.0f,
        vectorWeight: Float = 1.0f
    ): List<Pair<String, Float>> {
        val scores = HashMap<String, Float>()

        // 1. Accumulate BM25 rank scores
        for (rank in bm25Results.indices) {
            val (docId, _) = bm25Results[rank]
            val rankPos = rank + 1 // 1-based rank
            val rrfContribution = bm25Weight / (k + rankPos)
            scores[docId] = (scores[docId] ?: 0f) + rrfContribution
        }

        // 2. Accumulate Vector rank scores
        for (rank in vectorResults.indices) {
            val (docId, _) = vectorResults[rank]
            val rankPos = rank + 1 // 1-based rank
            val rrfContribution = vectorWeight / (k + rankPos)
            scores[docId] = (scores[docId] ?: 0f) + rrfContribution
        }

        if (scores.isEmpty()) return emptyList()

        return scores.entries
            .asSequence()
            .map { it.key to it.value }
            .sortedByDescending { it.second }
            .take(limit)
            .toList()
    }
}
