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
 * Execution strategy chosen by the cost-based query planner.
 */
enum class QueryStrategy {
    /**
     * Graph traversal is executed first to obtain a candidate set,
     * followed by batch vector scoring and re-ranking.
     */
    GRAPH_FIRST,

    /**
     * HNSW vector index search is executed first with adaptive over-fetching,
     * followed by batch graph predicate filtering until target limit is reached.
     */
    VECTOR_FIRST_ADAPTIVE,

    /**
     * Seed vectors are retrieved, followed by multi-hop graph expansion and re-ranking.
     */
    GRAPH_RAG
}

/**
 * Details of a planned and executed hybrid query, including cost models,
 * selectivity estimates, and execution profiling metrics.
 */
data class QueryExecutionPlan(
    val strategy: QueryStrategy,
    val estimatedGraphCost: Double,
    val estimatedVectorCost: Double,
    val estimatedSelectivity: Float,
    val initialK: Int,
    val iterations: Int,
    val vectorsScored: Int,
    val nodesInspected: Int,
    val resultCount: Int,
    val actualExecutionTimeMs: Long,
    val results: List<GraphVectorBridge.BridgeResult> = emptyList()
) {
    /**
     * Formats the execution plan into a human-readable EXPLAIN string.
     */
    fun explainString(): String {
        return buildString {
            appendLine("=== KoreDB Hybrid Query Execution Plan ===")
            appendLine("Strategy: $strategy")
            appendLine("Estimated Costs:")
            appendLine("  • Graph-First Cost:  %.2f".format(estimatedGraphCost))
            appendLine("  • Vector-First Cost: %.2f".format(estimatedVectorCost))
            appendLine("Selectivity Estimate:  %.1f%%".format(estimatedSelectivity * 100))
            appendLine("Adaptive Planning:")
            appendLine("  • Initial K:         $initialK")
            appendLine("  • Search Iterations: $iterations")
            appendLine("Execution Metrics:")
            appendLine("  • Execution Time:    ${actualExecutionTimeMs}ms")
            appendLine("  • Vectors Scored:    $vectorsScored")
            appendLine("  • Nodes Inspected:   $nodesInspected")
            appendLine("  • Final Results:     $resultCount")
            appendLine("==========================================")
        }
    }
}
