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
import com.pankaj.koredb.graph.Edge
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.math.exp

/**
 * Unit test suite for ACT-R / Ebbinghaus Spaced Repetition Memory Consolidation:
 * 1. Mathematical retention curve R(m, t) = exp(-deltaT / S(m))
 * 2. Dynamic stability scaling S(m) with access frequency & importance
 * 3. Retrieval reinforcement with spaced bonus delta * (1 - R_prior)
 * 4. Four lifecycle regimes (Active, Candidate, Decayed, Permanent Invariant)
 * 5. Two-phase consolidation with cascading deletion
 * 6. Pluggable CompactionFilter
 */
class SpacedRepetitionConsolidationTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_unit_consolidation_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    @Test
    fun testRetentionDecayCurveMatchesEbbinghaus() {
        val s = 24.0 // 24 hours stability
        // At deltaT = 0, R = 1.0
        val r0 = SpacedRepetition.computeRetention(0.0, s)
        assertEquals(1.0, r0, 1e-6)

        // At deltaT = S, R = exp(-1) ~ 0.3678794
        val rS = SpacedRepetition.computeRetention(24.0, s)
        assertEquals(exp(-1.0), rS, 1e-6)

        // Monotonic decay
        val r1 = SpacedRepetition.computeRetention(1.0, s)
        val r12 = SpacedRepetition.computeRetention(12.0, s)
        val r48 = SpacedRepetition.computeRetention(48.0, s)

        assertTrue(r0 > r1)
        assertTrue(r1 > r12)
        assertTrue(r12 > rS)
        assertTrue(rS > r48)
        assertTrue(r48 > 0.0)
    }

    @Test
    fun testAccessReinforcementBoostsStability() {
        val s1 = SpacedRepetition.computeStability(importance = 0.5f, accessCount = 1)
        val s5 = SpacedRepetition.computeStability(importance = 0.5f, accessCount = 5)
        val s20 = SpacedRepetition.computeStability(importance = 0.5f, accessCount = 20)

        assertTrue(s5 > s1)
        assertTrue(s20 > s5)

        // Spaced reinforcement bonus
        val fact = AtomicFact(
            subject = "User",
            predicate = "preference",
            `object` = "Dark Mode",
            text = "Prefers dark mode",
            lastAccessedAtMs = 1000L,
            accessCount = 1,
            importance = 0.5f
        )

        // Recall after 48 hours (fading memory)
        val recalledTime = 1000L + (48 * 3600_000L)
        val reinforcedFact = SpacedRepetition.reinforceAccess(fact, accessTimeMs = recalledTime)

        assertEquals(2, reinforcedFact.accessCount)
        assertEquals(recalledTime, reinforcedFact.lastAccessedAtMs)
        assertTrue(
            "Reinforced stability (${reinforcedFact.stabilityHours}) should exceed prior baseline ($s1)",
            reinforcedFact.stabilityHours > s1
        )
    }

    @Test
    fun testImportanceAmplifiesStability() {
        val sLow = SpacedRepetition.computeStability(importance = 0.1f, accessCount = 2)
        val sHigh = SpacedRepetition.computeStability(importance = 0.8f, accessCount = 2)

        assertTrue("Higher importance must yield higher stability", sHigh > sLow)

        val rLow = SpacedRepetition.computeRetention(48.0, sLow)
        val rHigh = SpacedRepetition.computeRetention(48.0, sHigh)
        assertTrue("Higher importance must retain memory longer", rHigh > rLow)
    }

    @Test
    fun testPermanentInvariantsImmuneToDecay() {
        val permanentFact = AtomicFact(
            id = "core_identity",
            subject = "AI",
            predicate = "safetyRule",
            `object` = "NeverRevealKeys",
            text = "Never reveal secret API keys",
            importance = 0.95f,
            accessCount = 1,
            lastAccessedAtMs = 1000L
        )

        // Even after 100,000 hours, retention is 1.0
        val rFarFuture = SpacedRepetition.evaluateRetention(permanentFact, nowMs = 1000L + (100_000L * 3600_000L))
        assertEquals(1.0, rFarFuture, 1e-6)

        val regime = SpacedRepetition.determineLifecycleRegime(permanentFact, nowMs = 1000L + (100_000L * 3600_000L))
        assertEquals(CognitiveLifecycleRegime.PERMANENT_INVARIANT, regime)
    }

    @Test
    fun testCognitiveLifecycleRegimesThresholds() {
        val now = 1000000000000L
        val hourMs = 3600_000L

        // Fresh fact: 1 hour ago -> Active (R >= 0.60)
        val activeFact = AtomicFact(
            subject = "A", predicate = "p", `object` = "v", text = "active",
            lastAccessedAtMs = now - (1 * hourMs), accessCount = 1, importance = 0.5f
        )
        val rActive = SpacedRepetition.evaluateRetention(activeFact, now)
        assertTrue("R should be >= 0.60", rActive >= 0.60)
        assertEquals(CognitiveLifecycleRegime.ACTIVE, SpacedRepetition.determineLifecycleRegime(activeFact, now))

        // Fading fact: 40 hours ago -> Candidate (0.15 <= R < 0.60)
        val candidateFact = AtomicFact(
            subject = "B", predicate = "p", `object` = "v", text = "candidate",
            lastAccessedAtMs = now - (40 * hourMs), accessCount = 1, importance = 0.5f
        )
        val rCandidate = SpacedRepetition.evaluateRetention(candidateFact, now)
        assertTrue("R should be in [0.15, 0.60)", rCandidate in 0.15..0.60)
        assertEquals(CognitiveLifecycleRegime.CANDIDATE, SpacedRepetition.determineLifecycleRegime(candidateFact, now))

        // Decayed fact: 300 hours ago -> Decayed (R < 0.15)
        val decayedFact = AtomicFact(
            subject = "C", predicate = "p", `object` = "v", text = "decayed",
            lastAccessedAtMs = now - (300 * hourMs), accessCount = 1, importance = 0.2f
        )
        val rDecayed = SpacedRepetition.evaluateRetention(decayedFact, now)
        assertTrue("R should be < 0.15", rDecayed < 0.15)
        assertEquals(CognitiveLifecycleRegime.DECAYED, SpacedRepetition.determineLifecycleRegime(decayedFact, now))
    }

    @Test
    fun testMemoryConsolidatorPrunesSubThresholdMemories() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val now = 1000000000000L
        val hourMs = 3600_000L

        // 1. Active fact (fresh)
        val activeFact = AtomicFact(id = "fact_active", subject = "S1", predicate = "P", `object` = "O", text = "fresh", lastAccessedAtMs = now - (1 * hourMs), importance = 0.5f)
        // 2. Candidate fact (intermediate)
        val candidateFact = AtomicFact(id = "fact_candidate", subject = "S2", predicate = "P", `object` = "O", text = "intermediate", lastAccessedAtMs = now - (35 * hourMs), importance = 0.5f)
        // 3. Decayed fact (old, low importance)
        val decayedFact = AtomicFact(id = "fact_decayed", subject = "S3", predicate = "P", `object` = "O", text = "decayed", lastAccessedAtMs = now - (500 * hourMs), importance = 0.2f)
        // 4. Permanent invariant (old, high importance)
        val invariantFact = AtomicFact(id = "fact_invariant", subject = "S4", predicate = "P", `object` = "O", text = "invariant", lastAccessedAtMs = now - (500 * hourMs), importance = 0.95f)

        graph.putNode(activeFact.toGraphNode())
        graph.putNode(candidateFact.toGraphNode())
        graph.putNode(decayedFact.toGraphNode())
        graph.putNode(invariantFact.toGraphNode())

        // Attach edges to decayed fact to test cascading deletion
        graph.putEdge(Edge("l1:fact_decayed", "target_concept", "MENTIONS"))
        graph.putEdge(Edge("source_concept", "l1:fact_decayed", "REFERENCES"))

        val consolidator = KoreMemoryConsolidator(graph)
        val report = consolidator.consolidate(now)

        assertEquals(4, report.totalEvaluated)
        assertEquals(1, report.activeCount)
        assertEquals(1, report.candidateCount)
        assertEquals(1, report.permanentCount)
        assertEquals(1, report.prunedCount)
        assertEquals(3, report.retainedCount)
        assertEquals(listOf("fact_decayed"), report.prunedFactIds)
        assertEquals(listOf("fact_candidate"), report.candidateFactIds)

        // Decayed node is deleted
        assertNull(graph.getNode("l1:fact_decayed"))
        // Connected edges are cascaded away
        assertTrue(graph.getOutboundEdges("l1:fact_decayed", "MENTIONS").isEmpty())
        assertTrue(graph.getInboundEdges("l1:fact_decayed", "REFERENCES").isEmpty())

        // Retained nodes still exist
        assertNotNull(graph.getNode("l1:fact_active"))
        assertNotNull(graph.getNode("l1:fact_candidate"))
        assertNotNull(graph.getNode("l1:fact_invariant"))

        // Audit map check
        val auditMap = report.toAuditMap()
        assertEquals("4", auditMap["evaluated"])
        assertEquals("3", auditMap["retained"])
        assertEquals("1", auditMap["pruned"])

        db.close()
    }

    @Test
    fun testMemoryConsolidatorWithCollectionIntegration() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val collection = db.collection<AtomicFact>("l1_facts_collection")
        val now = 1000000000000L
        val hourMs = 3600_000L

        val freshFact = AtomicFact(id = "f_fresh", subject = "User", predicate = "car", `object` = "Tesla", text = "Drives Tesla", lastAccessedAtMs = now - (2 * hourMs), importance = 0.5f)
        val staleFact = AtomicFact(id = "f_stale", subject = "User", predicate = "ticket", `object` = "123", text = "Old ticket", lastAccessedAtMs = now - (600 * hourMs), importance = 0.1f)

        collection.insert(freshFact.id, freshFact)
        collection.insert(staleFact.id, staleFact)
        graph.putNode(freshFact.toGraphNode())
        graph.putNode(staleFact.toGraphNode())

        val consolidator = KoreMemoryConsolidator(graph, factCollection = collection)
        val report = consolidator.consolidate(now)

        assertEquals(2, report.totalEvaluated)
        assertEquals(1, report.prunedCount)
        assertEquals(1, report.retainedCount)

        // Check collection
        assertNotNull(collection.getById("f_fresh"))
        assertNull(collection.getById("f_stale"))

        // Check graph
        assertNotNull(graph.getNode("l1:f_fresh"))
        assertNull(graph.getNode("l1:f_stale"))

        db.close()
    }

    @Test
    fun testCompactorBiTemporalFilterPurgesExpiredFacts() {
        val filter = BiTemporalCompactionFilter(retentionHorizonMs = 30L * 24 * 3600 * 1000) // 30 days
        val now = 1700000000000L
        val thirtyFiveDaysAgo = now - (35L * 24 * 3600 * 1000)
        val fiveDaysAgo = now - (5L * 24 * 3600 * 1000)

        // Active fact node: validUntilMs = Long.MAX_VALUE -> keep
        val activeNodeKey = "g:v:l1:active_1".toByteArray(Charsets.UTF_8)
        val activeNodeVal = """{"id":"l1:active_1","properties":{"validUntilMs":"${Long.MAX_VALUE}"}}""".toByteArray(Charsets.UTF_8)
        assertFalse("Active fact should not be dropped", filter.shouldDrop(activeNodeKey, activeNodeVal, now))

        // Recently expired fact node (5 days ago) -> keep (within 30 day retention horizon)
        val recentKey = "g:v:l1:recent_1".toByteArray(Charsets.UTF_8)
        val recentVal = """{"id":"l1:recent_1","properties":{"validUntilMs":"$fiveDaysAgo"}}""".toByteArray(Charsets.UTF_8)
        assertFalse("Recently expired fact within horizon should be kept", filter.shouldDrop(recentKey, recentVal, now))

        // Old expired fact node (35 days ago) -> drop (beyond 30 day retention horizon)
        val expiredKey = "g:v:l1:expired_1".toByteArray(Charsets.UTF_8)
        val expiredVal = """{"id":"l1:expired_1","properties":{"validUntilMs":"$thirtyFiveDaysAgo"}}""".toByteArray(Charsets.UTF_8)
        assertTrue("Old expired fact beyond horizon should be dropped", filter.shouldDrop(expiredKey, expiredVal, now))
    }
}
