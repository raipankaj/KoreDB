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

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.engine.BiTemporalCompactionFilter
import com.pankaj.koredb.engine.Compactor
import com.pankaj.koredb.foundation.ByteArrayComparator
import com.pankaj.koredb.foundation.SSTable
import com.pankaj.koredb.foundation.SSTableReader
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.math.exp

/**
 * Adversarial challenger test suite for Milestone 2:
 * ACT-R / Ebbinghaus Spaced Repetition decay dynamics, memory consolidation,
 * cascading graph cleanup, and bi-temporal compactor filtering.
 *
 * Authored by: challenger_m2_sota_2
 */
class M2ConsolidationChallengerTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/challenger_m2_consolidation_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // =========================================================================
    // 1. Extreme Time & Retention Invariants
    // =========================================================================

    @Test
    fun testRetentionAtDeltaZeroIsStrictlyOne() {
        // Test retention R(m, t) behavior at deltaT = 0 across diverse stability baselines
        val stabilities = listOf(0.1, 1.0, 12.0, 24.0, 168.0, 8760.0, Double.POSITIVE_INFINITY)
        for (s in stabilities) {
            val r = SpacedRepetition.computeRetention(elapsedHours = 0.0, stabilityHours = s)
            assertEquals("R must be strictly 1.0 at deltaT = 0 for stability $s", 1.0, r, 0.0)
            assertFalse("R must not be NaN", r.isNaN())
        }

        // Test through evaluateRetention on AtomicFact at exact ingestion timestamp
        val now = 1700000000000L
        val fact = AtomicFact(
            subject = "Subject",
            predicate = "predicate",
            `object` = "Object",
            text = "Testing deltaT = 0",
            lastAccessedAtMs = now,
            importance = 0.5f,
            accessCount = 1
        )
        val rFact = SpacedRepetition.evaluateRetention(fact, nowMs = now)
        assertEquals("evaluateRetention at now == lastAccessedAtMs must be strictly 1.0", 1.0, rFact, 0.0)
    }

    @Test
    fun testRetentionAtAsymptoticInfinityApproachesZeroWithoutNaNOrNegative() {
        // Test retention as deltaT -> infinity across extreme orders of magnitude
        val extremeHours = listOf(
            1e4, 1e6, 1e9, 1e15, 1e50, 1e100, 1e300,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY
        )
        val baseStability = 24.0

        for (dt in extremeHours) {
            val r = SpacedRepetition.computeRetention(elapsedHours = dt, stabilityHours = baseStability)
            assertFalse("R must never be NaN for dt=$dt", r.isNaN())
            assertFalse("R must never be Infinite for dt=$dt", r.isInfinite())
            assertTrue("R must never be negative for dt=$dt (got $r)", r >= 0.0)
            assertTrue("R must never exceed 1.0 for dt=$dt (got $r)", r <= 1.0)
            assertEquals("R must asymptotically reach 0.0 at extreme dt=$dt", 0.0, r, 1e-12)
        }

        // Test through AtomicFact evaluation at a timestamp far in the future
        val past = 1000000L
        val farFuture = past + (100_000_000_000L * 3600_000L) // 100 billion hours
        val fact = AtomicFact(
            subject = "AncientFact",
            predicate = "event",
            `object` = "BigBang",
            text = "Long ago",
            lastAccessedAtMs = past,
            importance = 0.3f
        )
        val rEvaluated = SpacedRepetition.evaluateRetention(fact, nowMs = farFuture)
        assertEquals(0.0, rEvaluated, 1e-15)
        assertEquals(CognitiveLifecycleRegime.DECAYED, SpacedRepetition.determineLifecycleRegime(fact, nowMs = farFuture))
    }

    @Test
    fun testRetentionBackwardClockJumpsClampedToOne() {
        // Clocks adjusting backward or NTP synchronization rollbacks must never inflate retention > 1.0
        val negativeElapsed = listOf(-0.0001, -1.0, -24.0, -8760.0, -1e10, Double.NEGATIVE_INFINITY)
        for (negDt in negativeElapsed) {
            val r = SpacedRepetition.computeRetention(elapsedHours = negDt, stabilityHours = 24.0)
            assertEquals("Backward clock jump ($negDt hrs) must clamp R to strictly 1.0", 1.0, r, 0.0)
            assertFalse("R must not be NaN", r.isNaN())
        }

        // Test via evaluateRetention with backward timestamps
        val now = 1700000000000L
        val fact = AtomicFact(
            subject = "ClockTest",
            predicate = "status",
            `object` = "Active",
            text = "Clock rollback",
            lastAccessedAtMs = now,
            importance = 0.5f
        )

        // Evaluate at timestamps earlier than lastAccessedAtMs
        val rollbacksMs = listOf(now - 1_000L, now - 3600_000L, now - (365L * 24 * 3600_000L))
        for (jumpTime in rollbacksMs) {
            val r = SpacedRepetition.evaluateRetention(fact, nowMs = jumpTime)
            assertEquals("evaluateRetention under backward jump must clamp R to 1.0", 1.0, r, 0.0)
            assertEquals(CognitiveLifecycleRegime.ACTIVE, SpacedRepetition.determineLifecycleRegime(fact, nowMs = jumpTime))
        }

        // Test reinforceAccess under backward clock jump: stability must NOT inflate artificially
        val initialStability = fact.stabilityHours
        val reinforcedBackward = SpacedRepetition.reinforceAccess(fact, accessTimeMs = now - 100_000L)
        // With R_prior = 1.0, bonus multiplier is (1 + delta * (1 - 1.0)) = 1.0
        val expectedBaseStability = SpacedRepetition.computeStability(fact.importance, fact.accessCount)
        assertEquals(
            "Under backward jump, R_prior is 1.0, so reinforced stability equals current unboosted stability",
            expectedBaseStability,
            reinforcedBackward.stabilityHours,
            1e-6
        )
    }

    @Test
    fun testRetentionExtremeStabilityEdgeCases() {
        // Zero or negative stability: computeRetention must safely return 0.0 without throwing
        assertEquals(0.0, SpacedRepetition.computeRetention(10.0, 0.0), 0.0)
        assertEquals(0.0, SpacedRepetition.computeRetention(10.0, -24.0), 0.0)

        // Zero elapsed time with zero stability: elapsedHours <= 0 takes precedence -> 1.0
        assertEquals(1.0, SpacedRepetition.computeRetention(0.0, 0.0), 0.0)

        // Access count <= 0 should be safeguarded by max(1, accessCount)
        val sNegativeCount = SpacedRepetition.computeStability(importance = 0.5f, accessCount = -5)
        val sZeroCount = SpacedRepetition.computeStability(importance = 0.5f, accessCount = 0)
        val sOneCount = SpacedRepetition.computeStability(importance = 0.5f, accessCount = 1)
        assertEquals(sOneCount, sNegativeCount, 1e-9)
        assertEquals(sOneCount, sZeroCount, 1e-9)

        // Importance clamped between 0.0 and 1.0 (sub-0.90)
        val sNegImportance = SpacedRepetition.computeStability(importance = -0.5f, accessCount = 1)
        val sZeroImportance = SpacedRepetition.computeStability(importance = 0.0f, accessCount = 1)
        assertEquals(sZeroImportance, sNegImportance, 1e-9)
    }

    // =========================================================================
    // 2. Permanent Invariant Immunity (Importance >= 0.90)
    // =========================================================================

    @Test
    fun testPermanentInvariantImmunityAtThresholdBoundary() {
        // Exact threshold boundary: 0.90f
        val sJustBelow = SpacedRepetition.computeStability(importance = 0.89999f, accessCount = 1)
        assertTrue("Importance 0.89999 must have finite stability", sJustBelow.isFinite())

        val sBoundary = SpacedRepetition.computeStability(importance = 0.90f, accessCount = 1)
        assertTrue("Importance 0.90 must have POSITIVE_INFINITY stability", sBoundary.isInfinite() && sBoundary > 0)

        val sAbove = SpacedRepetition.computeStability(importance = 0.95f, accessCount = 1)
        assertTrue("Importance 0.95 must have POSITIVE_INFINITY stability", sAbove.isInfinite())

        val sMax = SpacedRepetition.computeStability(importance = 1.0f, accessCount = 1)
        assertTrue("Importance 1.0 must have POSITIVE_INFINITY stability", sMax.isInfinite())
    }

    @Test
    fun testPermanentInvariantImmunityOverExtremeTimeAndAccess() {
        val invariant = AtomicFact(
            id = "core_rule_safety",
            subject = "System",
            predicate = "safetyInvariant",
            `object` = "StrictSecurity",
            text = "Never bypass auth",
            importance = 0.90f,
            accessCount = 1,
            lastAccessedAtMs = 1_000_000L
        )

        // Elapsed time: 10,000 hours, 100,000 hours, 1,000,000 hours
        val testHours = listOf(10_000L, 100_000L, 1_000_000L)
        for (hrs in testHours) {
            val evalTime = 1_000_000L + (hrs * 3600_000L)
            val r = SpacedRepetition.evaluateRetention(invariant, nowMs = evalTime)
            assertEquals("Permanent invariant R must strictly equal 1.0 after $hrs hours", 1.0, r, 0.0)
            val regime = SpacedRepetition.determineLifecycleRegime(invariant, nowMs = evalTime)
            assertEquals(CognitiveLifecycleRegime.PERMANENT_INVARIANT, regime)
        }

        // Simulate 500 successive reinforcement accesses
        var currentFact = invariant
        var simTime = 1_000_000L
        for (i in 1..500) {
            simTime += 86400_000L // 1 day later
            currentFact = SpacedRepetition.reinforceAccess(currentFact, accessTimeMs = simTime)
        }
        assertEquals(501, currentFact.accessCount)
        assertEquals(1.0, SpacedRepetition.evaluateRetention(currentFact, nowMs = simTime + (50_000L * 3600_000L)), 0.0)
        assertEquals(CognitiveLifecycleRegime.PERMANENT_INVARIANT, SpacedRepetition.determineLifecycleRegime(currentFact, nowMs = simTime))
    }

    @Test
    fun testPermanentInvariantImmunitySurvivesRepeatedConsolidationRuns() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val collection = db.collection<AtomicFact>("permanent_store")

        val baseTime = 1000000000000L
        val tenYearsAgo = baseTime - (10L * 365 * 24 * 3600_000L)

        // Create 5 permanent invariants with varying high importance (>= 0.90)
        val invariants = listOf(
            AtomicFact(id = "inv_1", subject = "S1", predicate = "P", `object` = "O", text = "Rule 1", importance = 0.90f, lastAccessedAtMs = tenYearsAgo),
            AtomicFact(id = "inv_2", subject = "S2", predicate = "P", `object` = "O", text = "Rule 2", importance = 0.91f, lastAccessedAtMs = tenYearsAgo),
            AtomicFact(id = "inv_3", subject = "S3", predicate = "P", `object` = "O", text = "Rule 3", importance = 0.95f, lastAccessedAtMs = tenYearsAgo),
            AtomicFact(id = "inv_4", subject = "S4", predicate = "P", `object` = "O", text = "Rule 4", importance = 0.99f, lastAccessedAtMs = tenYearsAgo),
            AtomicFact(id = "inv_5", subject = "S5", predicate = "P", `object` = "O", text = "Rule 5", importance = 1.00f, lastAccessedAtMs = tenYearsAgo)
        )

        for (inv in invariants) {
            graph.putNode(inv.toGraphNode())
            collection.insert(inv.id, inv)
        }

        val consolidator = KoreMemoryConsolidator(graph, factCollection = collection)

        // Run 5 consecutive consolidation passes across 5 advancing time horizons
        for (step in 1..5) {
            val passTime = baseTime + (step * 365L * 24 * 3600_000L)
            val report = consolidator.consolidate(nowMs = passTime)

            assertEquals("All 5 must be evaluated", 5, report.totalEvaluated)
            assertEquals("0 pruned in pass $step", 0, report.prunedCount)
            assertEquals("5 retained in pass $step", 5, report.retainedCount)
            assertEquals("5 permanent in pass $step", 5, report.permanentCount)
            assertTrue("prunedFactIds must be empty", report.prunedFactIds.isEmpty())

            // Verify they still exist in both GraphStorage and KoreCollection
            for (inv in invariants) {
                assertNotNull("Node l1:${inv.id} must exist in graph", graph.getNode("l1:${inv.id}"))
                assertNotNull("Fact ${inv.id} must exist in collection", collection.getById(inv.id))
            }
        }

        db.close()
    }

    // =========================================================================
    // 3. Consolidation Pruning & Ghost-Edge Elimination
    // =========================================================================

    @Test
    fun testCascadingCleanupEliminatesAllOrphanedAndGhostEdges() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val now = 1000000000000L
        val hourMs = 3600_000L

        // Decayed fact (600 hours old, low importance -> R < 0.15)
        val decayedFact = AtomicFact(
            id = "decayed_center",
            subject = "EphemeralMemory",
            predicate = "status",
            `object` = "Transitory",
            text = "Decayed proposition",
            lastAccessedAtMs = now - (600 * hourMs),
            importance = 0.2f
        )
        val decayedNodeId = "l1:decayed_center"

        // External nodes connected to the decayed fact
        val extTarget1 = Node("concept:target1", setOf("Concept"), mapOf("name" to "Target One"))
        val extTarget2 = Node("concept:target2", setOf("Concept"), mapOf("name" to "Target Two"))
        val extTarget3 = Node("concept:target3", setOf("Concept"), mapOf("name" to "Target Three"))
        val extSource1 = Node("concept:source1", setOf("Concept"), mapOf("name" to "Source One"))
        val extSource2 = Node("concept:source2", setOf("Concept"), mapOf("name" to "Source Two"))

        graph.putNode(decayedFact.toGraphNode())
        graph.putNode(extTarget1)
        graph.putNode(extTarget2)
        graph.putNode(extTarget3)
        graph.putNode(extSource1)
        graph.putNode(extSource2)

        // 3 Outbound edges from decayed fact
        graph.putEdge(Edge(decayedNodeId, extTarget1.id, "MENTIONS", mapOf("weight" to "1.0")))
        graph.putEdge(Edge(decayedNodeId, extTarget2.id, "LOCATED_IN", mapOf("weight" to "2.0")))
        graph.putEdge(Edge(decayedNodeId, extTarget3.id, "SUPERSEDES", mapOf("weight" to "3.0")))

        // 2 Inbound edges into decayed fact
        graph.putEdge(Edge(extSource1.id, decayedNodeId, "DERIVED_FROM", mapOf("weight" to "1.0")))
        graph.putEdge(Edge(extSource2.id, decayedNodeId, "REFERENCES", mapOf("weight" to "2.0")))

        // Independent edge between external nodes (must NOT be pruned)
        graph.putEdge(Edge(extSource1.id, extTarget1.id, "CROSS_LINK", mapOf("weight" to "5.0")))

        // Verify initial topology
        assertEquals(3, graph.getOutboundEdges(decayedNodeId, "MENTIONS").size +
                graph.getOutboundEdges(decayedNodeId, "LOCATED_IN").size +
                graph.getOutboundEdges(decayedNodeId, "SUPERSEDES").size)
        assertEquals(1, graph.getInboundEdges(extTarget1.id, "MENTIONS").size)
        assertEquals(1, graph.getInboundEdges(extTarget2.id, "LOCATED_IN").size)
        assertEquals(1, graph.getInboundEdges(extTarget3.id, "SUPERSEDES").size)
        assertEquals(1, graph.getOutboundEdges(extSource1.id, "DERIVED_FROM").size)
        assertEquals(1, graph.getOutboundEdges(extSource2.id, "REFERENCES").size)

        // Execute consolidation
        val consolidator = KoreMemoryConsolidator(graph)
        val report = consolidator.consolidate(nowMs = now)

        assertEquals(1, report.prunedCount)
        assertEquals(listOf("decayed_center"), report.prunedFactIds)

        // 1. Decayed node itself must be completely gone
        assertNull("Decayed node must be null in GraphStorage", graph.getNode(decayedNodeId))

        // 2. All outbound edges from decayed node must be empty
        assertTrue("Outbound MENTIONS must be empty", graph.getOutboundEdges(decayedNodeId, "MENTIONS").isEmpty())
        assertTrue("Outbound LOCATED_IN must be empty", graph.getOutboundEdges(decayedNodeId, "LOCATED_IN").isEmpty())
        assertTrue("Outbound SUPERSEDES must be empty", graph.getOutboundEdges(decayedNodeId, "SUPERSEDES").isEmpty())

        // 3. All inbound edges to decayed node must be empty
        assertTrue("Inbound DERIVED_FROM must be empty", graph.getInboundEdges(decayedNodeId, "DERIVED_FROM").isEmpty())
        assertTrue("Inbound REFERENCES must be empty", graph.getInboundEdges(decayedNodeId, "REFERENCES").isEmpty())

        // 4. Neighbor perspective: reciprocal ghost edges must be purged!
        assertTrue("extTarget1 must have zero inbound MENTIONS", graph.getInboundEdges(extTarget1.id, "MENTIONS").isEmpty())
        assertTrue("extTarget2 must have zero inbound LOCATED_IN", graph.getInboundEdges(extTarget2.id, "LOCATED_IN").isEmpty())
        assertTrue("extTarget3 must have zero inbound SUPERSEDES", graph.getInboundEdges(extTarget3.id, "SUPERSEDES").isEmpty())
        assertTrue("extSource1 must have zero outbound DERIVED_FROM", graph.getOutboundEdges(extSource1.id, "DERIVED_FROM").isEmpty())
        assertTrue("extSource2 must have zero outbound REFERENCES", graph.getOutboundEdges(extSource2.id, "REFERENCES").isEmpty())

        // 5. Independent edge must remain completely intact
        val preservedEdges = graph.getOutboundEdges(extSource1.id, "CROSS_LINK")
        assertEquals("Independent edge must remain intact", 1, preservedEdges.size)
        assertEquals(extTarget1.id, preservedEdges[0].targetId)

        // 6. External nodes themselves must remain intact
        assertNotNull(graph.getNode(extTarget1.id))
        assertNotNull(graph.getNode(extTarget2.id))
        assertNotNull(graph.getNode(extTarget3.id))
        assertNotNull(graph.getNode(extSource1.id))
        assertNotNull(graph.getNode(extSource2.id))

        db.close()
    }

    @Test
    fun testMultiNodeConsolidationCascadeAcrossRegimes() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val now = 1000000000000L
        val hourMs = 3600_000L

        // 3 Decayed facts
        val d1 = AtomicFact(id = "d1", subject = "S", predicate = "P", `object` = "1", text = "d1", lastAccessedAtMs = now - (500 * hourMs), importance = 0.1f)
        val d2 = AtomicFact(id = "d2", subject = "S", predicate = "P", `object` = "2", text = "d2", lastAccessedAtMs = now - (500 * hourMs), importance = 0.1f)
        val d3 = AtomicFact(id = "d3", subject = "S", predicate = "P", `object` = "3", text = "d3", lastAccessedAtMs = now - (500 * hourMs), importance = 0.1f)

        // 2 Candidate facts
        val c1 = AtomicFact(id = "c1", subject = "S", predicate = "P", `object` = "c1", text = "c1", lastAccessedAtMs = now - (35 * hourMs), importance = 0.5f)
        val c2 = AtomicFact(id = "c2", subject = "S", predicate = "P", `object` = "c2", text = "c2", lastAccessedAtMs = now - (35 * hourMs), importance = 0.5f)

        // 2 Active facts
        val a1 = AtomicFact(id = "a1", subject = "S", predicate = "P", `object` = "a1", text = "a1", lastAccessedAtMs = now - (2 * hourMs), importance = 0.5f)
        val a2 = AtomicFact(id = "a2", subject = "S", predicate = "P", `object` = "a2", text = "a2", lastAccessedAtMs = now - (2 * hourMs), importance = 0.5f)

        // 2 Permanent invariants
        val p1 = AtomicFact(id = "p1", subject = "S", predicate = "P", `object` = "p1", text = "p1", lastAccessedAtMs = now - (1000 * hourMs), importance = 0.95f)
        val p2 = AtomicFact(id = "p2", subject = "S", predicate = "P", `object` = "p2", text = "p2", lastAccessedAtMs = now - (1000 * hourMs), importance = 0.99f)

        val allFacts = listOf(d1, d2, d3, c1, c2, a1, a2, p1, p2)
        allFacts.forEach { graph.putNode(it.toGraphNode()) }

        // Dense cross-linking across regimes
        graph.putEdge(Edge("l1:d1", "l1:d2", "CHAIN"))
        graph.putEdge(Edge("l1:d2", "l1:d3", "CHAIN"))
        graph.putEdge(Edge("l1:a1", "l1:d1", "SUPERSEDES"))
        graph.putEdge(Edge("l1:d1", "l1:a1", "SUPERSEDED_BY"))
        graph.putEdge(Edge("l1:c1", "l1:d2", "CORRELATES"))
        graph.putEdge(Edge("l1:p1", "l1:a1", "GOVERNS"))
        graph.putEdge(Edge("l1:a1", "l1:c1", "SUPPORTS"))

        val consolidator = KoreMemoryConsolidator(graph)
        val report = consolidator.consolidate(nowMs = now)

        assertEquals(9, report.totalEvaluated)
        assertEquals(3, report.prunedCount)
        assertEquals(2, report.candidateCount)
        assertEquals(2, report.activeCount)
        assertEquals(2, report.permanentCount)
        assertEquals(6, report.retainedCount)

        // Decayed nodes gone
        assertNull(graph.getNode("l1:d1"))
        assertNull(graph.getNode("l1:d2"))
        assertNull(graph.getNode("l1:d3"))

        // Surviving nodes intact
        assertNotNull(graph.getNode("l1:c1"))
        assertNotNull(graph.getNode("l1:c2"))
        assertNotNull(graph.getNode("l1:a1"))
        assertNotNull(graph.getNode("l1:a2"))
        assertNotNull(graph.getNode("l1:p1"))
        assertNotNull(graph.getNode("l1:p2"))

        // Dangling edges pointing to/from d1, d2, d3 purged
        assertTrue(graph.getOutboundEdges("l1:a1", "SUPERSEDES").isEmpty())
        assertTrue(graph.getOutboundEdges("l1:c1", "CORRELATES").isEmpty())

        // Edges between surviving nodes remain intact
        assertEquals(1, graph.getOutboundEdges("l1:p1", "GOVERNS").size)
        assertEquals(1, graph.getOutboundEdges("l1:a1", "SUPPORTS").size)

        db.close()
    }

    // =========================================================================
    // 4. Reinforcement Dynamics & Ebbinghaus Spacing Effect
    // =========================================================================

    @Test
    fun testEbbinghausSpacingEffectEmpiricalComparison() {
        // Empirically compare spaced access vs massed (rapid) access:
        // Spaced retrieval when memory has decayed yields dramatically greater stability gains!
        val baseFact = AtomicFact(
            subject = "Learner",
            predicate = "vocabulary",
            `object` = "ubiquitous",
            text = "ubiquitous means everywhere",
            lastAccessedAtMs = 0L,
            accessCount = 1,
            importance = 0.5f,
            stabilityHours = 24.0
        )

        // Track massed (rapid successive access) over 5 iterations: 1-minute intervals
        var massedFact = baseFact
        var massedTime = 0L
        val massedGains = mutableListOf<Double>()
        for (i in 1..5) {
            massedTime += 60_000L // 1 minute later
            val prevS = massedFact.stabilityHours
            massedFact = SpacedRepetition.reinforceAccess(massedFact, accessTimeMs = massedTime)
            val gain = massedFact.stabilityHours - prevS
            massedGains.add(gain)
        }

        // Track spaced access over 5 iterations: spaced at optimal forgetting intervals (elapsed = stability)
        var spacedFact = baseFact
        var spacedTime = 0L
        val spacedGains = mutableListOf<Double>()
        for (i in 1..5) {
            // Space access by current stability hours
            val intervalMs = (spacedFact.stabilityHours * 3600_000L).toLong()
            spacedTime += intervalMs
            val prevS = spacedFact.stabilityHours
            spacedFact = SpacedRepetition.reinforceAccess(spacedFact, accessTimeMs = spacedTime)
            val gain = spacedFact.stabilityHours - prevS
            spacedGains.add(gain)
        }

        // Numerical proof of the Ebbinghaus spacing effect:
        // 1. In every single step, spaced stability gain must strictly exceed massed stability gain
        for (step in 0 until 5) {
            assertTrue(
                "Step $step: Spaced gain (${spacedGains[step]}) must exceed massed gain (${massedGains[step]})",
                spacedGains[step] > massedGains[step]
            )
        }

        // 2. Cumulative stability after 5 spaced accesses must dwarf massed stability
        val finalRatio = spacedFact.stabilityHours / massedFact.stabilityHours
        assertTrue(
            "Final spaced stability (${spacedFact.stabilityHours}) must be substantially greater than massed (${massedFact.stabilityHours}), ratio=$finalRatio",
            finalRatio > 1.8
        )
    }

    @Test
    fun testReinforcementWithZeroRetentionPriorMaxBonus() {
        val fact = AtomicFact(
            subject = "Topic",
            predicate = "detail",
            `object` = "Value",
            text = "Infrequent memory",
            lastAccessedAtMs = 1_000_000L,
            accessCount = 1,
            importance = 0.5f,
            stabilityHours = 24.0
        )

        // Recall after 100,000 hours -> R_prior is practically 0.0
        val recallTime = 1_000_000L + (100_000L * 3600_000L)
        val reinforced = SpacedRepetition.reinforceAccess(fact, accessTimeMs = recallTime, delta = 1.5)

        // With delta = 1.5 and R_prior ~ 0.0, stability multiplier is (1 + 1.5 * 1.0) = 2.5
        val currentS = SpacedRepetition.computeStability(fact.importance, fact.accessCount)
        val expectedS = currentS * 2.5
        assertEquals("When R_prior ~ 0, stability bonus multiplier is (1 + delta)", expectedS, reinforced.stabilityHours, 1e-4)

        // Recall immediately (elapsed = 0) -> R_prior = 1.0, bonus multiplier is 1.0
        val immediateReinforced = SpacedRepetition.reinforceAccess(fact, accessTimeMs = 1_000_000L, delta = 1.5)
        assertEquals("When R_prior = 1.0, stability bonus multiplier is 1.0", currentS, immediateReinforced.stabilityHours, 1e-4)
    }

    // =========================================================================
    // 5. Compactor Filter Execution & Integration
    // =========================================================================

    @Test
    fun testBiTemporalCompactionFilterFlagsExpiredEntriesAcrossTypes() {
        val filter = BiTemporalCompactionFilter(retentionHorizonMs = 30L * 24 * 3600_000L) // 30 days
        val now = 1700000000000L
        val expiredTime = now - (45L * 24 * 3600_000L) // 45 days ago (> 30 days)
        val recentTime = now - (15L * 24 * 3600_000L)  // 15 days ago (< 30 days)

        // 1. Graph Node keys (g:v:...)
        val nodeActive = "g:v:l1:active".toByteArray()
        val nodeActiveVal = """{"id":"l1:active","properties":{"validUntilMs":"${Long.MAX_VALUE}"}}""".toByteArray()
        assertFalse("Active node must not be dropped", filter.shouldDrop(nodeActive, nodeActiveVal, now))

        val nodeRecent = "g:v:l1:recent".toByteArray()
        val nodeRecentVal = """{"id":"l1:recent","properties":{"validUntilMs":"$recentTime"}}""".toByteArray()
        assertFalse("Recent node within horizon must not be dropped", filter.shouldDrop(nodeRecent, nodeRecentVal, now))

        val nodeExpired = "g:v:l1:expired".toByteArray()
        val nodeExpiredVal = """{"id":"l1:expired","properties":{"validUntilMs":"$expiredTime"}}""".toByteArray()
        assertTrue("Expired node past horizon must be dropped", filter.shouldDrop(nodeExpired, nodeExpiredVal, now))

        // 2. Document Collection keys (doc:...)
        val docActive = "doc:l1_facts:fact_active".toByteArray()
        val docActiveVal = """{"id":"fact_active","validUntilMs":${Long.MAX_VALUE}}""".toByteArray()
        assertFalse("Active doc must not be dropped", filter.shouldDrop(docActive, docActiveVal, now))

        val docExpired = "doc:l1_facts:fact_expired".toByteArray()
        val docExpiredVal = """{"id":"fact_expired","validUntilMs":$expiredTime}""".toByteArray()
        assertTrue("Expired doc past horizon must be dropped", filter.shouldDrop(docExpired, docExpiredVal, now))

        // 3. Graph Edge keys (g:e:...) with _validUntilMs
        val edgeActive = "g:e:out:f1:SUPERSEDES:f0".toByteArray()
        val edgeActiveVal = """{"sourceId":"f1","targetId":"f0","_validUntilMs":"${Long.MAX_VALUE}"}""".toByteArray()
        assertFalse("Active edge must not be dropped", filter.shouldDrop(edgeActive, edgeActiveVal, now))

        val edgeExpired = "g:e:out:f2:SUPERSEDES:f1".toByteArray()
        val edgeExpiredVal = """{"sourceId":"f2","targetId":"f1","_validUntilMs":"$expiredTime"}""".toByteArray()
        assertTrue("Expired edge past horizon must be dropped", filter.shouldDrop(edgeExpired, edgeExpiredVal, now))

        // 4. Inbound edge key (g:e:in:...)
        val inEdgeExpired = "g:e:in:f1:SUPERSEDES:f2".toByteArray()
        val inEdgeExpiredVal = """{"sourceId":"f2","targetId":"f1","_validUntilMs":"$expiredTime"}""".toByteArray()
        assertTrue("Expired inbound edge must be dropped", filter.shouldDrop(inEdgeExpired, inEdgeExpiredVal, now))

        // 5. Non-bi-temporal keys must never be dropped by this filter
        val indexKey = "idx:l1_facts:subject:Alice:fact1".toByteArray()
        val indexVal = "fact1".toByteArray()
        assertFalse("Index key must not be dropped", filter.shouldDrop(indexKey, indexVal, now))

        // 6. Tombstone / empty value must not be dropped by this filter
        assertFalse("Empty value must not be dropped", filter.shouldDrop(nodeExpired, ByteArray(0), now))
    }

    @Test
    fun testBiTemporalCompactionFilterBoundaryExactEdgeCases() {
        val horizon = 30L * 24 * 3600_000L
        val filter = BiTemporalCompactionFilter(retentionHorizonMs = horizon)
        val now = 1700000000000L
        val exactBoundary = now - horizon

        val nodeExactKey = "g:v:exact".toByteArray()
        val nodeExactVal = """{"validUntilMs":"$exactBoundary"}""".toByteArray()
        // Boundary condition: untilMs < now - horizon is strictly less than, so exact boundary is preserved
        assertFalse("Exact boundary must NOT be dropped", filter.shouldDrop(nodeExactKey, nodeExactVal, now))

        val nodePastOneMsKey = "g:v:past_one_ms".toByteArray()
        val nodePastOneMsVal = """{"validUntilMs":"${exactBoundary - 1}"}""".toByteArray()
        assertTrue("1ms past horizon MUST be dropped", filter.shouldDrop(nodePastOneMsKey, nodePastOneMsVal, now))
    }

    @Test
    fun testEndToEndSSTableCompactionWithBiTemporalFilter() {
        val sstFile = File(testDir, "test_input.sst")
        val compactedFile = File(testDir, "test_compacted.sst")
        val now = System.currentTimeMillis()
        val horizon = 30L * 24 * 3600_000L
        val expiredPast = now - (60L * 24 * 3600_000L) // 60 days ago (> 30 days)
        val expiredRecent = now - (10L * 24 * 3600_000L) // 10 days ago (< 30 days)

        // Construct 6 entries
        val rawEntries = listOf(
            "doc:facts:fact_active".toByteArray() to """{"id":"fact_active","validUntilMs":${Long.MAX_VALUE}}""".toByteArray(),
            "doc:facts:fact_expired_old".toByteArray() to """{"id":"fact_expired_old","validUntilMs":$expiredPast}""".toByteArray(),
            "doc:facts:fact_expired_recent".toByteArray() to """{"id":"fact_expired_recent","validUntilMs":$expiredRecent}""".toByteArray(),
            "g:e:out:f1:MENTIONS:f2".toByteArray() to """{"_validUntilMs":"$expiredPast"}""".toByteArray(),
            "g:v:l1:active_node".toByteArray() to """{"properties":{"validUntilMs":"${Long.MAX_VALUE}"}}""".toByteArray(),
            "g:v:l1:expired_node".toByteArray() to """{"properties":{"validUntilMs":"$expiredPast"}}""".toByteArray()
        )

        // SSTable requires entries strictly sorted by ByteArrayComparator
        val sortedEntries = rawEntries.sortedWith { a, b -> ByteArrayComparator.compare(a.first, b.first) }

        // 1. Write the initial SSTable directly
        SSTable.writeSortedEntries(sortedEntries.asSequence(), sstFile)
        assertTrue("Initial SSTable must exist", sstFile.exists() && sstFile.length() > 0)

        // 2. Verify all 6 entries exist in the initial SSTable
        SSTableReader(sstFile).use { reader ->
            for (entry in rawEntries) {
                assertNotNull("Key ${String(entry.first)} must exist in uncompacted SSTable", reader.find(entry.first))
            }
        }

        // 3. Compact with BiTemporalCompactionFilter
        val filter = BiTemporalCompactionFilter(retentionHorizonMs = horizon)
        SSTableReader(sstFile).use { reader ->
            Compactor.compact(
                readers = listOf(reader),
                outputFile = compactedFile,
                compactionFilter = filter,
                dropTombstones = true
            )
        }
        assertTrue("Compacted SSTable must exist", compactedFile.exists() && compactedFile.length() > 0)

        // 4. Verify that expired entries past the horizon were pruned while active/recent were preserved
        SSTableReader(compactedFile).use { reader ->
            // Preserved entries
            assertNotNull("Active doc must be preserved", reader.find("doc:facts:fact_active".toByteArray()))
            assertNotNull("Recently expired doc must be preserved", reader.find("doc:facts:fact_expired_recent".toByteArray()))
            assertNotNull("Active node must be preserved", reader.find("g:v:l1:active_node".toByteArray()))

            // Pruned entries
            assertNull("Old expired doc must be pruned", reader.find("doc:facts:fact_expired_old".toByteArray()))
            assertNull("Old expired edge must be pruned", reader.find("g:e:out:f1:MENTIONS:f2".toByteArray()))
            assertNull("Old expired node must be pruned", reader.find("g:v:l1:expired_node".toByteArray()))
        }
    }

    // =========================================================================
    // 6. Lifecycle Regime Boundaries & Edge Conditions
    // =========================================================================

    @Test
    fun testConsolidatorLifecycleRegimeBoundaryTransitions() {
        val s = 24.0
        // S(m) = 24.0.
        // ACTIVE threshold: R >= 0.60
        // R = exp(-dt / 24) >= 0.60 => -dt / 24 >= ln(0.60) => dt <= -24 * ln(0.60) ~ 12.2598 hours
        val tActiveBoundary = -s * kotlin.math.ln(0.60)
        val rAtActive = SpacedRepetition.computeRetention(tActiveBoundary, s)
        assertEquals(0.60, rAtActive, 1e-6)

        // CANDIDATE threshold: 0.15 <= R < 0.60
        // R = exp(-dt / 24) >= 0.15 => dt <= -24 * ln(0.15) ~ 45.531 hours
        val tCandidateBoundary = -s * kotlin.math.ln(0.15)
        val rAtCandidate = SpacedRepetition.computeRetention(tCandidateBoundary, s)
        assertEquals(0.15, rAtCandidate, 1e-6)

        // Just inside active regime (12.0 hours)
        val fActive = AtomicFact(subject = "S", predicate = "P", `object` = "O", text = "T", lastAccessedAtMs = 0L, importance = 0.0f, accessCount = 1)
        val sActive = SpacedRepetition.computeStability(fActive.importance, fActive.accessCount) // 24.0
        val r12 = SpacedRepetition.computeRetention(12.0, sActive)
        assertTrue(r12 >= 0.60)

        // Just inside candidate regime (20.0 hours)
        val r20 = SpacedRepetition.computeRetention(20.0, sActive)
        assertTrue(r20 in 0.15..0.60)

        // Decayed regime (75.0 hours, > -30.41 * ln(0.15) ~ 57.7 hrs)
        val r75 = SpacedRepetition.computeRetention(75.0, sActive)
        assertTrue(r75 < 0.15)
    }

    @Test
    fun testConsolidatorEmptyAndBoundaryWorkloads() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val consolidator = KoreMemoryConsolidator(graph)

        // 1. Empty database consolidation
        val emptyReport = consolidator.consolidate()
        assertEquals(0, emptyReport.totalEvaluated)
        assertEquals(0, emptyReport.prunedCount)
        assertEquals(0, emptyReport.retainedCount)
        assertEquals(0, emptyReport.activeCount)
        assertEquals(0, emptyReport.candidateCount)
        assertEquals(0, emptyReport.permanentCount)
        assertEquals("0", emptyReport.toAuditMap()["evaluated"])

        // 2. All active facts
        val now = 1000000000000L
        for (i in 1..10) {
            val fact = AtomicFact(id = "act_$i", subject = "S", predicate = "P", `object` = "$i", text = "Act $i", lastAccessedAtMs = now, importance = 0.5f)
            graph.putNode(fact.toGraphNode())
        }
        val allActiveReport = consolidator.consolidate(nowMs = now)
        assertEquals(10, allActiveReport.totalEvaluated)
        assertEquals(0, allActiveReport.prunedCount)
        assertEquals(10, allActiveReport.retainedCount)
        assertEquals(10, allActiveReport.activeCount)

        db.close()
    }
}
