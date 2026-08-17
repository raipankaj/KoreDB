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

import kotlin.math.ln
import kotlin.math.max

/**
 * Production-grade Okapi BM25 scoring algorithm.
 *
 * @property k1 Term frequency saturation parameter (default 1.2).
 * @property b Document length normalization parameter (default 0.75).
 */
class BM25Scorer(
    val k1: Float = 1.2f,
    val b: Float = 0.75f
) {

    /**
     * Calculates the Robertson-Spärck Jones Inverse Document Frequency (IDF) with smoothing.
     *
     * @param totalDocs Total number of documents in the collection (N).
     * @param docsWithTerm Number of documents containing the term (n(q)).
     * @return The IDF weight for the term.
     */
    fun calculateIDF(totalDocs: Long, docsWithTerm: Long): Float {
        val n = max(1L, totalDocs)
        val df = max(0L, docsWithTerm)
        val idf = ln(1.0 + (n - df + 0.5) / (df + 0.5))
        return max(0.01, idf).toFloat()
    }

    /**
     * Computes the BM25 score contribution for a single query term against a document.
     *
     * @param idf Pre-computed IDF value for the query term.
     * @param termFrequency Frequency of the term within the document (f(q, D)).
     * @param docLength Total token count of the document (|D|).
     * @param avgDocLength Average document length across the entire collection (avgdl).
     * @return The BM25 score contribution for this term.
     */
    fun scoreTerm(
        idf: Float,
        termFrequency: Int,
        docLength: Int,
        avgDocLength: Float
    ): Float {
        if (termFrequency <= 0) return 0f
        val effectiveAvgDl = max(1f, avgDocLength)
        val lenNorm = 1.0f - b + b * (docLength / effectiveAvgDl)
        val numerator = termFrequency * (k1 + 1.0f)
        val denominator = termFrequency + k1 * lenNorm
        return idf * (numerator / denominator)
    }
}
