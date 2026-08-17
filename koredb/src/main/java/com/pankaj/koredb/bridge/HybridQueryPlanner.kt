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

/**
 * Cost-based query optimizer that selects between Graph-First and Vector-First execution
 * paths based on candidate set cardinality, predicate selectivity, and index access costs.
 */
class HybridQueryPlanner(
    private val statsTracker: QueryStatsTracker = QueryStatsTracker()
) {

    /**
     * Estimates costs and chooses the optimal strategy for a hybrid vector + graph query.
     */
    fun chooseStrategy(
        targetLimit: Int,
        candidateNodeCount: Int?,
        predicateTag: String?,
        maxK: Int = 1000
    ): Pair<QueryStrategy, Pair<Double, Double>> {
        val selectivity = statsTracker.estimateSelectivity(predicateTag)
        val initialK = statsTracker.calculateInitialK(targetLimit, predicateTag, maxK)

        // Vector-first cost: HNSW traversal base cost + evaluating graph predicate on K overfetched candidates
        val vectorFirstCost = HNSW_SEARCH_BASE_COST + (initialK * (GRAPH_FILTER_UNIT_COST + VECTOR_DISTANCE_UNIT_COST))

        // Graph-first cost: scoring candidate nodes against the query vector
        val graphFirstCost = if (candidateNodeCount != null && candidateNodeCount > 0) {
            candidateNodeCount * (VECTOR_DISTANCE_UNIT_COST + GRAPH_READ_UNIT_COST)
        } else {
            Double.MAX_VALUE // Graph-first not possible without candidate set
        }

        val strategy = when {
            candidateNodeCount != null && candidateNodeCount <= targetLimit -> {
                // If candidate pool is smaller than or equal to target limit, graph-first is strictly optimal
                QueryStrategy.GRAPH_FIRST
            }
            graphFirstCost < vectorFirstCost -> {
                QueryStrategy.GRAPH_FIRST
            }
            else -> {
                QueryStrategy.VECTOR_FIRST_ADAPTIVE
            }
        }

        return Pair(strategy, Pair(graphFirstCost, vectorFirstCost))
    }

    companion object {
        private const val HNSW_SEARCH_BASE_COST = 15.0
        private const val VECTOR_DISTANCE_UNIT_COST = 0.05
        private const val GRAPH_FILTER_UNIT_COST = 0.20
        private const val GRAPH_READ_UNIT_COST = 0.10
    }
}
