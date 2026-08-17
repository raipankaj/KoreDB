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

package com.pankaj.koredb.bridge

import java.util.concurrent.ConcurrentHashMap

/**
 * Tracks historical query execution statistics and predicate selectivity
 * using an Exponential Moving Average (EMA) to guide adaptive query planning.
 */
class QueryStatsTracker(
    private val smoothingFactor: Float = 0.3f
) {
    // Map of predicateTag -> estimatedSelectivity (0.0 to 1.0)
    private val selectivityMap = ConcurrentHashMap<String, Float>()

    // Map of predicateTag -> executionCount
    private val queryCountMap = ConcurrentHashMap<String, Long>()

    /**
     * Estimates the selectivity (pass rate) of a graph predicate or query tag.
     * Returns a float between 0.0 (filters out everything) and 1.0 (passes everything).
     * Defaults to 0.5 (50% selectivity) if no historical statistics exist.
     */
    fun estimateSelectivity(predicateTag: String?): Float {
        if (predicateTag == null) return DEFAULT_SELECTIVITY
        return selectivityMap[predicateTag] ?: DEFAULT_SELECTIVITY
    }

    /**
     * Records the observed selectivity of an executed query pass.
     * Updates the running EMA for the given predicate tag.
     *
     * @param predicateTag An identifier for the predicate type or query pattern.
     * @param totalCandidates Number of candidates inspected.
     * @param matchedCandidates Number of candidates that passed the predicate.
     */
    fun recordExecution(predicateTag: String?, totalCandidates: Int, matchedCandidates: Int) {
        if (predicateTag == null || totalCandidates <= 0) return

        val observedSelectivity = (matchedCandidates.toFloat() / totalCandidates.toFloat()).coerceIn(0.001f, 1.0f)

        selectivityMap.compute(predicateTag) { _, current ->
            if (current == null) {
                observedSelectivity
            } else {
                // Exponential Moving Average: (smoothing * observed) + ((1 - smoothing) * current)
                (smoothingFactor * observedSelectivity) + ((1f - smoothingFactor) * current)
            }
        }
        queryCountMap.compute(predicateTag) { _, count -> (count ?: 0L) + 1 }
    }

    /**
     * Calculates the recommended initial over-fetch count ($k$) based on the target limit
     * and historical selectivity.
     */
    fun calculateInitialK(targetLimit: Int, predicateTag: String?, maxK: Int = 1000): Int {
        val selectivity = estimateSelectivity(predicateTag)
        val estimatedK = (targetLimit / selectivity.coerceAtLeast(0.01f)).toInt()
        return estimatedK.coerceIn(targetLimit * 2, maxK)
    }

    /**
     * Returns the total recorded query executions for a tag.
     */
    fun getQueryCount(predicateTag: String): Long = queryCountMap[predicateTag] ?: 0L

    /**
     * Clears all recorded statistics.
     */
    fun clear() {
        selectivityMap.clear()
        queryCountMap.clear()
    }

    companion object {
        const val DEFAULT_SELECTIVITY = 0.5f
    }
}
