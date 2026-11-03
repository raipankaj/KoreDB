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

package com.pankaj.koredb.memory

import com.pankaj.koredb.core.KoreCollection
import com.pankaj.koredb.graph.GraphStorage
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.pow

/**
 * Four cognitive lifecycle regimes for episodic and semantic memory in KoreDB.
 */
enum class CognitiveLifecycleRegime {
    /**
     * Fresh or frequently recalled memory (Retention R >= 0.60).
     * Retained in active working memory and fast-path prompt injection.
     */
    ACTIVE,

    /**
     * Fading episodic detail (0.15 <= Retention R < 0.60).
     * Candidate for community clustering into L2 Scenarios.
     */
    CANDIDATE,

    /**
     * Sub-threshold cognitive noise (Retention R < 0.15 and Importance < 0.90).
     * Pruned via atomic cascading deletion in the graph and purged in compaction.
     */
    DECAYED,

    /**
     * Core persona or critical safety invariant (Importance >= 0.90).
     * Permanently immune to decay (Stability = Infinity, Retention = 1.0).
     */
    PERMANENT_INVARIANT
}

/**
 * Mathematical formulation of ACT-R / Ebbinghaus cognitive decay and spaced repetition.
 */
object SpacedRepetition {

    const val DEFAULT_BASE_STABILITY_HOURS: Double = 24.0
    const val DEFAULT_ALPHA: Double = 1.0
    const val DEFAULT_BETA: Double = 0.5
    const val DEFAULT_GAMMA: Double = 0.8
    const val DEFAULT_DELTA: Double = 1.5

    const val THRESHOLD_ACTIVE: Double = 0.60
    const val THRESHOLD_CANDIDATE: Double = 0.15
    const val THRESHOLD_PERMANENT_IMPORTANCE: Float = 0.90f

    /**
     * Computes memory stability S(m) in hours based on importance and retrieval practice:
     * S(m) = S_base * (1 + alpha * I) * (1 + beta * ln(1 + n))^gamma
     */
    fun computeStability(
        importance: Float,
        accessCount: Int,
        baseStabilityHours: Double = DEFAULT_BASE_STABILITY_HOURS,
        alpha: Double = DEFAULT_ALPHA,
        beta: Double = DEFAULT_BETA,
        gamma: Double = DEFAULT_GAMMA
    ): Double {
        if (importance >= THRESHOLD_PERMANENT_IMPORTANCE) {
            return Double.POSITIVE_INFINITY
        }
        val safeCount = max(1, accessCount)
        val safeImportance = importance.coerceIn(0.0f, 1.0f)
        return baseStabilityHours * (1.0 + alpha * safeImportance) * (1.0 + beta * ln(1.0 + safeCount)).pow(gamma)
    }

    /**
     * Computes retention score R(m, t) = exp(-deltaT / S(m)).
     * Returns 1.0 if elapsed hours <= 0, or 0.0 if stability <= 0.
     */
    fun computeRetention(elapsedHours: Double, stabilityHours: Double): Double {
        if (elapsedHours <= 0.0) return 1.0
        if (stabilityHours.isInfinite()) return 1.0
        if (stabilityHours <= 0.0) return 0.0
        return exp(-elapsedHours / stabilityHours)
    }

    /**
     * Evaluates current retention R(m, t) for an [AtomicFact] at timestamp [nowMs].
     */
    fun evaluateRetention(
        fact: AtomicFact,
        nowMs: Long = System.currentTimeMillis(),
        baseStabilityHours: Double = DEFAULT_BASE_STABILITY_HOURS,
        alpha: Double = DEFAULT_ALPHA,
        beta: Double = DEFAULT_BETA,
        gamma: Double = DEFAULT_GAMMA
    ): Double {
        if (fact.importance >= THRESHOLD_PERMANENT_IMPORTANCE) {
            return 1.0
        }
        val elapsedHours = max(0.0, (nowMs - fact.lastAccessedAtMs) / 3600_000.0)
        val stability = computeStability(fact.importance, fact.accessCount, baseStabilityHours, alpha, beta, gamma)
        return computeRetention(elapsedHours, stability)
    }

    /**
     * Applies a spaced-repetition reinforcement upon recall of [fact] at timestamp [accessTimeMs]:
     * R_prior = exp(-deltaT / S)
     * S_new = S * (1 + delta * (1 - R_prior))
     * accessCount = accessCount + 1
     * lastAccessedAtMs = accessTimeMs
     */
    fun reinforceAccess(
        fact: AtomicFact,
        accessTimeMs: Long = System.currentTimeMillis(),
        delta: Double = DEFAULT_DELTA,
        baseStabilityHours: Double = DEFAULT_BASE_STABILITY_HOURS,
        alpha: Double = DEFAULT_ALPHA,
        beta: Double = DEFAULT_BETA,
        gamma: Double = DEFAULT_GAMMA
    ): AtomicFact {
        if (fact.importance >= THRESHOLD_PERMANENT_IMPORTANCE) {
            return fact.copy(
                accessCount = fact.accessCount + 1,
                lastAccessedAtMs = accessTimeMs
            )
        }
        val elapsedHours = max(0.0, (accessTimeMs - fact.lastAccessedAtMs) / 3600_000.0)
        val currentStability = computeStability(fact.importance, fact.accessCount, baseStabilityHours, alpha, beta, gamma)
        val rPrior = computeRetention(elapsedHours, currentStability)
        val boostedStability = currentStability * (1.0 + delta * (1.0 - rPrior))

        return fact.copy(
            accessCount = fact.accessCount + 1,
            lastAccessedAtMs = accessTimeMs,
            stabilityHours = boostedStability
        )
    }

    /**
     * Classifies [fact] into one of the four cognitive regimes.
     */
    fun determineLifecycleRegime(
        fact: AtomicFact,
        nowMs: Long = System.currentTimeMillis(),
        baseStabilityHours: Double = DEFAULT_BASE_STABILITY_HOURS,
        alpha: Double = DEFAULT_ALPHA,
        beta: Double = DEFAULT_BETA,
        gamma: Double = DEFAULT_GAMMA
    ): CognitiveLifecycleRegime {
        if (fact.importance >= THRESHOLD_PERMANENT_IMPORTANCE) {
            return CognitiveLifecycleRegime.PERMANENT_INVARIANT
        }
        val r = evaluateRetention(fact, nowMs, baseStabilityHours, alpha, beta, gamma)
        return when {
            r >= THRESHOLD_ACTIVE -> CognitiveLifecycleRegime.ACTIVE
            r >= THRESHOLD_CANDIDATE -> CognitiveLifecycleRegime.CANDIDATE
            else -> CognitiveLifecycleRegime.DECAYED
        }
    }
}

/**
 * Audit telemetry and result report produced by [KoreMemoryConsolidator].
 */
data class ConsolidationReport(
    val timestampMs: Long = System.currentTimeMillis(),
    val totalEvaluated: Int,
    val activeCount: Int,
    val candidateCount: Int,
    val prunedCount: Int,
    val permanentCount: Int,
    val prunedFactIds: List<String> = emptyList(),
    val candidateFactIds: List<String> = emptyList()
) {
    val retainedCount: Int
        get() = totalEvaluated - prunedCount

    fun toAuditMap(): Map<String, String> = mapOf(
        "timestamp" to timestampMs.toString(),
        "evaluated" to totalEvaluated.toString(),
        "retained" to retainedCount.toString(),
        "pruned" to prunedCount.toString(),
        "active" to activeCount.toString(),
        "candidates" to candidateCount.toString(),
        "permanent" to permanentCount.toString()
    )
}

/**
 * Two-phase memory consolidator for KoreDB.
 *
 * Evaluates memory retention across all L1 Atomic Facts using ACT-R / Ebbinghaus
 * spaced repetition curves. For sub-threshold decayed memories (R < 0.15), executes
 * atomic cascading deletions ([GraphStorage.deleteNode]) to eliminate ghost edges
 * before physical LSM compaction. Clusters consolidation candidates (0.15 <= R < 0.60)
 * and shields permanent invariants (Importance >= 0.90).
 */
class KoreMemoryConsolidator(
    private val graph: GraphStorage,
    private val factCollection: KoreCollection<AtomicFact>? = null,
    private val baseStabilityHours: Double = SpacedRepetition.DEFAULT_BASE_STABILITY_HOURS,
    private val alpha: Double = SpacedRepetition.DEFAULT_ALPHA,
    private val beta: Double = SpacedRepetition.DEFAULT_BETA,
    private val gamma: Double = SpacedRepetition.DEFAULT_GAMMA
) {

    /**
     * Executes a memory consolidation pass at [nowMs].
     *
     * 1. Scans all active facts from [factCollection] (if provided) or [graph].
     * 2. Evaluates retention scores R(m, t) against cognitive thresholds.
     * 3. Executes atomic cascading deletion for decayed memories.
     * 4. Returns a detailed [ConsolidationReport].
     */
    suspend fun consolidate(nowMs: Long = System.currentTimeMillis()): ConsolidationReport {
        // Collect candidate facts from collection or graph
        val facts = if (factCollection != null) {
            factCollection.getAll()
        } else {
            val nodes = graph.getNodesByLabel("L1_Fact").ifEmpty { graph.getNodesByLabel("Fact") }
            nodes.map { AtomicFact.fromGraphNode(it) }
        }

        val prunedIds = mutableListOf<String>()
        val candidateIds = mutableListOf<String>()
        var activeCount = 0
        var permanentCount = 0

        for (fact in facts) {
            val regime = SpacedRepetition.determineLifecycleRegime(
                fact = fact,
                nowMs = nowMs,
                baseStabilityHours = baseStabilityHours,
                alpha = alpha,
                beta = beta,
                gamma = gamma
            )

            when (regime) {
                CognitiveLifecycleRegime.DECAYED -> {
                    prunedIds.add(fact.id)
                    // Cascading delete in graph storage (node, inbound/outbound edges, indices)
                    val targetNodeId = if (graph.getNode("l1:${fact.id}") != null) "l1:${fact.id}" else fact.id
                    graph.deleteNode(targetNodeId)
                    // Delete from document collection if configured
                    factCollection?.delete(fact.id)
                }
                CognitiveLifecycleRegime.CANDIDATE -> {
                    candidateIds.add(fact.id)
                }
                CognitiveLifecycleRegime.ACTIVE -> {
                    activeCount++
                }
                CognitiveLifecycleRegime.PERMANENT_INVARIANT -> {
                    permanentCount++
                }
            }
        }

        return ConsolidationReport(
            timestampMs = nowMs,
            totalEvaluated = facts.size,
            activeCount = activeCount,
            candidateCount = candidateIds.size,
            prunedCount = prunedIds.size,
            permanentCount = permanentCount,
            prunedFactIds = prunedIds,
            candidateFactIds = candidateIds
        )
    }

    /**
     * Pure evaluation helper grouping a list of facts into their cognitive regimes.
     */
    fun evaluateFacts(
        facts: List<AtomicFact>,
        nowMs: Long = System.currentTimeMillis()
    ): Map<CognitiveLifecycleRegime, List<AtomicFact>> {
        return facts.groupBy {
            SpacedRepetition.determineLifecycleRegime(
                fact = it,
                nowMs = nowMs,
                baseStabilityHours = baseStabilityHours,
                alpha = alpha,
                beta = beta,
                gamma = gamma
            )
        }
    }
}
