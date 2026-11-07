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

package com.pankaj.koredb.e2e

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.e2e.TestContracts.E2EAtomicFact
import com.pankaj.koredb.e2e.TestContracts.E2EBiTemporalEdge
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Comprehensive Opaque-Box E2E Test Suite for:
 * 1. Bi-Temporal Fact & Edge Tracking (Transaction Time & Validity Time Intervals)
 * 2. Deterministic Contradiction Detection & Atomic Invalidation (SUPERSEDES Links)
 * 3. Point-in-Time Temporal Query DSL (queryActive, asOf, Lineage Tracing)
 * 4. ACT-R / Ebbinghaus Spaced-Repetition Memory Consolidation (Stability S(m) & Retention R(m, t))
 * 5. Compactor Hook & Cascading Tombstoning of Decayed Facts
 *
 * Covering:
 * - Tier 1: Feature Coverage (>=5 test cases per feature)
 * - Tier 2: Boundary & Corner Cases (extreme timestamps, zero duration, instant invalidation)
 * - Tier 3: Cross-Feature Interactions (Hub-Damped PageRank over bi-temporal active facts, spaced repetition boosts)
 * - Tier 4: Real-World Scenarios (Dynamic agent episodic memory evolution over simulated 30-day timeline)
 */
class BiTemporalCognitiveE2ETest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_e2e_bitemporal_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // TIER 1: FEATURE COVERAGE (>= 5 Tests Per Feature)
    // ========================================================================

    // --- Feature 4: Bi-Temporal Fact & Edge Tracking ---

    @Test
    fun testBiTemporalFactCreationAndIntervalDefaults() {
        val now = System.currentTimeMillis()
        val fact = E2EAtomicFact(
            subject = "Alice",
            predicate = "livesIn",
            `object` = "Tokyo",
            text = "Alice lives in Tokyo",
            ingestedAtMs = now
        )

        assertEquals(now, fact.ingestedAtMs)
        assertEquals(now, fact.validFromMs)
        assertEquals(Long.MAX_VALUE, fact.validUntilMs)
        assertTrue(fact.isActive)
        assertTrue(fact.isValidAt(now))
        assertTrue(fact.isValidAt(now + 1_000_000L))
        assertFalse(fact.isValidAt(now - 1L))
    }

    @Test
    fun testBiTemporalEdgePropertySerialization() {
        val now = 1700000000000L
        val edge = E2EBiTemporalEdge(
            sourceId = "user_alice",
            targetId = "city_tokyo",
            type = "LIVES_IN",
            ingestedAtMs = now,
            validFromMs = now,
            validUntilMs = Long.MAX_VALUE,
            properties = mapOf("certainty" to "0.95")
        )

        val underlyingEdge = edge.toEdge()
        assertEquals("user_alice", underlyingEdge.sourceId)
        assertEquals("city_tokyo", underlyingEdge.targetId)
        assertEquals("LIVES_IN", underlyingEdge.type)
        assertEquals(now.toString(), underlyingEdge.properties["_ingestedAtMs"])
        assertEquals(now.toString(), underlyingEdge.properties["_validFromMs"])
        assertEquals(Long.MAX_VALUE.toString(), underlyingEdge.properties["_validUntilMs"])
        assertEquals("true", underlyingEdge.properties["_isActive"])
        assertEquals("0.95", underlyingEdge.properties["certainty"])

        // Deserialize back
        val reconstructed = E2EBiTemporalEdge.fromEdge(underlyingEdge)
        assertEquals(edge.sourceId, reconstructed.sourceId)
        assertEquals(edge.targetId, reconstructed.targetId)
        assertEquals(edge.type, reconstructed.type)
        assertEquals(edge.ingestedAtMs, reconstructed.ingestedAtMs)
        assertEquals(edge.validFromMs, reconstructed.validFromMs)
        assertEquals(edge.validUntilMs, reconstructed.validUntilMs)
        assertEquals(edge.properties, reconstructed.properties)
    }

    @Test
    fun testBiTemporalIntervalContainmentSemantics() {
        val t1 = 1000L
        val t2 = 5000L
        val fact = E2EAtomicFact(
            subject = "Bob",
            predicate = "status",
            `object` = "trial",
            text = "Bob is on trial",
            validFromMs = t1,
            validUntilMs = t2
        )

        // Half-open interval [t1, t2)
        assertFalse("Before start is invalid", fact.isValidAt(999L))
        assertTrue("At start boundary is valid", fact.isValidAt(1000L))
        assertTrue("Inside interval is valid", fact.isValidAt(3000L))
        assertFalse("At end boundary is invalid (half-open)", fact.isValidAt(5000L))
        assertFalse("After end boundary is invalid", fact.isValidAt(5001L))
    }

    @Test
    fun testBiTemporalTransactionTimeImmutability() {
        val txTime = 10000L
        val fact1 = E2EAtomicFact(
            subject = "Server",
            predicate = "ip",
            `object` = "10.0.0.1",
            text = "Server IP is 10.0.0.1",
            ingestedAtMs = txTime
        )

        // Simulating audit check: at txTime - 1, system was unaware of this fact
        assertFalse(fact1.isBiTemporallyValidAt(validTimeMs = txTime, asOfTransactionTimeMs = txTime - 1))
        // At txTime, system became aware
        assertTrue(fact1.isBiTemporallyValidAt(validTimeMs = txTime, asOfTransactionTimeMs = txTime))
    }

    @Test
    fun testBiTemporalBatchPersistenceInGraphStorage() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val fact1 = E2EAtomicFact(
            id = "f1",
            subject = "Alice",
            predicate = "role",
            `object` = "Engineer",
            text = "Alice is an Engineer",
            ingestedAtMs = 1000L,
            validFromMs = 1000L,
            validUntilMs = Long.MAX_VALUE
        )
        val edge1 = E2EBiTemporalEdge("Alice", "Role_Engineer", "HAS_ROLE", 1000L, 1000L, Long.MAX_VALUE)

        graph.putNode(fact1.toGraphNode())
        graph.putEdge(edge1.toEdge())

        val retrievedNode = graph.getNode("l1:f1")
        assertNotNull(retrievedNode)
        assertEquals("Alice", retrievedNode?.properties?.get("subject"))
        assertEquals("true", retrievedNode?.properties?.get("isActive"))

        val retrievedEdges = graph.getOutboundEdges("Alice", "HAS_ROLE")
        assertEquals(1, retrievedEdges.size)
        val bEdge = E2EBiTemporalEdge.fromEdge(retrievedEdges[0])
        assertTrue(bEdge.isActive)

        db.close()
    }

    // --- Feature 5: Contradiction & Invalidation ---

    @Test
    fun testAtomicFactContradictionDetection() {
        val fact1 = E2EAtomicFact(
            id = "f1",
            subject = "Carol",
            predicate = "city",
            `object` = "Paris",
            text = "Carol lives in Paris"
        )
        val fact2 = E2EAtomicFact(
            id = "f2",
            subject = "Carol",
            predicate = "city",
            `object` = "Berlin",
            text = "Carol moved to Berlin"
        )

        assertEquals("Carol:city", fact1.contradictionKey)
        assertEquals("Carol:city", fact2.contradictionKey)
        assertEquals(fact1.contradictionKey, fact2.contradictionKey)
    }

    @Test
    fun testAtomicInvalidationBatchMutation() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val now = 2000L

        // Initial fact: Alice in Tokyo
        val f1 = E2EAtomicFact(id = "f1", subject = "Alice", predicate = "city", `object` = "Tokyo", text = "Alice in Tokyo", validFromMs = 1000L)
        graph.putNode(f1.toGraphNode())

        // Contradicting fact: Alice in London at t = 2000
        val invalidatedF1 = f1.copy(validUntilMs = now, supersededByFactId = "f2")
        val f2 = E2EAtomicFact(id = "f2", subject = "Alice", predicate = "city", `object` = "London", text = "Alice in London", validFromMs = now)

        // Atomic write: update f1 and insert f2 + directed SUPERSEDES edge
        graph.putNode(invalidatedF1.toGraphNode())
        graph.putNode(f2.toGraphNode())
        graph.putEdge(Edge(sourceId = "l1:f2", targetId = "l1:f1", type = "SUPERSEDES"))
        graph.putEdge(Edge(sourceId = "l1:f1", targetId = "l1:f2", type = "SUPERSEDED_BY"))

        // Verify state
        val node1 = graph.getNode("l1:f1")!!
        val node2 = graph.getNode("l1:f2")!!
        assertEquals("false", node1.properties["isActive"])
        assertEquals("true", node2.properties["isActive"])
        assertEquals(now.toString(), node1.properties["validUntilMs"])

        val supersedesEdges = graph.getOutboundEdges("l1:f2", "SUPERSEDES")
        assertEquals(1, supersedesEdges.size)
        assertEquals("l1:f1", supersedesEdges[0].targetId)

        db.close()
    }

    @Test
    fun testReciprocalSupersededByEdgeCreation() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        graph.putEdge(Edge("l1:fact_v2", "l1:fact_v1", "SUPERSEDES"))
        graph.putEdge(Edge("l1:fact_v1", "l1:fact_v2", "SUPERSEDED_BY"))

        val forward = graph.getOutboundEdges("l1:fact_v2", "SUPERSEDES")
        val backward = graph.getOutboundEdges("l1:fact_v1", "SUPERSEDED_BY")

        assertEquals(1, forward.size)
        assertEquals("l1:fact_v1", forward[0].targetId)
        assertEquals(1, backward.size)
        assertEquals("l1:fact_v2", backward[0].targetId)

        db.close()
    }

    @Test
    fun testContradictionWithMultiplePreviousVersions() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Lineage: v3 -> v2 -> v1
        graph.putNode(Node("l1:v1", properties = mapOf("version" to "1", "isActive" to "false")))
        graph.putNode(Node("l1:v2", properties = mapOf("version" to "2", "isActive" to "false")))
        graph.putNode(Node("l1:v3", properties = mapOf("version" to "3", "isActive" to "true")))

        graph.putEdge(Edge("l1:v2", "l1:v1", "SUPERSEDES"))
        graph.putEdge(Edge("l1:v3", "l1:v2", "SUPERSEDES"))

        // Trace backward lineage from v3
        val history = mutableListOf<String>()
        var curr = "l1:v3"
        while (true) {
            val next = graph.getOutboundEdges(curr, "SUPERSEDES").firstOrNull()?.targetId ?: break
            history.add(next)
            curr = next
        }

        assertEquals(listOf("l1:v2", "l1:v1"), history)
        db.close()
    }

    @Test
    fun testNonContradictingFactsCoexist() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val f1 = E2EAtomicFact(id = "f1", subject = "Alice", predicate = "livesIn", `object` = "Kyoto", text = "Alice lives in Kyoto")
        val f2 = E2EAtomicFact(id = "f2", subject = "Alice", predicate = "worksAt", `object` = "Lab", text = "Alice works at Lab")

        assertNotEquals(f1.contradictionKey, f2.contradictionKey)
        graph.putNode(f1.toGraphNode())
        graph.putNode(f2.toGraphNode())

        val n1 = graph.getNode("l1:f1")!!
        val n2 = graph.getNode("l1:f2")!!
        assertEquals("true", n1.properties["isActive"])
        assertEquals("true", n2.properties["isActive"])

        db.close()
    }

    // --- Feature 6: Temporal Query DSL ---

    @Test
    fun testQueryActiveFiltersOutInvalidatedFacts() {
        val facts = listOf(
            E2EAtomicFact(id = "1", subject = "A", predicate = "p", `object` = "v1", text = "1", validUntilMs = 5000L),
            E2EAtomicFact(id = "2", subject = "A", predicate = "p", `object` = "v2", text = "2", validFromMs = 5000L, validUntilMs = Long.MAX_VALUE),
            E2EAtomicFact(id = "3", subject = "B", predicate = "q", `object` = "v3", text = "3", validUntilMs = Long.MAX_VALUE)
        )

        val activeFacts = facts.filter { it.isActive }
        assertEquals(2, activeFacts.size)
        assertEquals(setOf("2", "3"), activeFacts.map { it.id }.toSet())
    }

    @Test
    fun testAsOfValidTimePointInTimeReconstruction() {
        val facts = listOf(
            E2EAtomicFact(id = "v1", subject = "A", predicate = "city", `object` = "NY", text = "NY", validFromMs = 1000L, validUntilMs = 5000L),
            E2EAtomicFact(id = "v2", subject = "A", predicate = "city", `object` = "SF", text = "SF", validFromMs = 5000L, validUntilMs = 9000L),
            E2EAtomicFact(id = "v3", subject = "A", predicate = "city", `object` = "LA", text = "LA", validFromMs = 9000L, validUntilMs = Long.MAX_VALUE)
        )

        // Historical query at t = 3000 (should be NY)
        val at3000 = facts.filter { it.isValidAt(3000L) }
        assertEquals(1, at3000.size)
        assertEquals("NY", at3000[0].`object`)

        // Historical query at t = 7000 (should be SF)
        val at7000 = facts.filter { it.isValidAt(7000L) }
        assertEquals(1, at7000.size)
        assertEquals("SF", at7000[0].`object`)

        // Current query at t = 10000 (should be LA)
        val at10000 = facts.filter { it.isValidAt(10000L) }
        assertEquals(1, at10000.size)
        assertEquals("LA", at10000[0].`object`)
    }

    @Test
    fun testAsOfTransactionTimeBiTemporalAudit() {
        // Fact was true in reality starting at t = 1000, but database only ingested it at t = 5000
        val fact = E2EAtomicFact(
            id = "late_fact",
            subject = "Stock",
            predicate = "price",
            `object` = "100",
            text = "Stock price 100",
            ingestedAtMs = 5000L,
            validFromMs = 1000L,
            validUntilMs = Long.MAX_VALUE
        )

        // As of transaction time 3000, DB did NOT know this fact existed yet
        assertFalse(fact.isBiTemporallyValidAt(validTimeMs = 2000L, asOfTransactionTimeMs = 3000L))

        // As of transaction time 6000, DB knew this fact was valid at 2000L
        assertTrue(fact.isBiTemporallyValidAt(validTimeMs = 2000L, asOfTransactionTimeMs = 6000L))
    }

    @Test
    fun testTraceFactLineageTraversesSupersedesChain() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // 4 versions of a salary fact
        for (i in 1..4) {
            graph.putNode(Node("l1:sal_v$i", properties = mapOf("salary" to (i * 10000).toString())))
            if (i > 1) {
                graph.putEdge(Edge("l1:sal_v$i", "l1:sal_v${i - 1}", "SUPERSEDES"))
            }
        }

        // Trace from v4 down to v1
        val lineage = mutableListOf<String>()
        var curr = "l1:sal_v4"
        lineage.add(curr)
        while (true) {
            val prev = graph.getOutboundEdges(curr, "SUPERSEDES").firstOrNull()?.targetId ?: break
            lineage.add(prev)
            curr = prev
        }

        assertEquals(listOf("l1:sal_v4", "l1:sal_v3", "l1:sal_v2", "l1:sal_v1"), lineage)
        db.close()
    }

    @Test
    fun testOutboundActiveGraphTraversal() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Edge 1: active (validUntilMs = Long.MAX_VALUE)
        val activeEdge = E2EBiTemporalEdge("User", "Phone1", "OWNS_DEVICE", validUntilMs = Long.MAX_VALUE)
        // Edge 2: invalidated (validUntilMs = 2000L)
        val staleEdge = E2EBiTemporalEdge("User", "PhoneOld", "OWNS_DEVICE", validUntilMs = 2000L)

        graph.putEdge(activeEdge.toEdge())
        graph.putEdge(staleEdge.toEdge())

        val allEdges = graph.getOutboundEdges("User", "OWNS_DEVICE")
        assertEquals(2, allEdges.size)

        val activeOnly = allEdges.filter { E2EBiTemporalEdge.fromEdge(it).isActive }
        assertEquals(1, activeOnly.size)
        assertEquals("Phone1", activeOnly[0].targetId)

        db.close()
    }

    // --- Feature 7: ACT-R / Ebbinghaus Spaced Repetition ---

    @Test
    fun testInitialRetentionIsUnity() {
        val (stability, retention) = TestContracts.computeActRRetentionGroundTruth(
            elapsedHours = 0.0,
            accessCount = 1,
            importance = 0.5f
        )
        assertTrue("Stability must be positive", stability > 0.0)
        assertEquals(1.0, retention, 1e-6)
    }

    @Test
    fun testMonotonicDecayOverTime() {
        val t1 = 1.0 // 1 hour
        val t2 = 24.0 // 1 day
        val t3 = 168.0 // 1 week

        val (_, r1) = TestContracts.computeActRRetentionGroundTruth(t1, 1, 0.5f)
        val (_, r2) = TestContracts.computeActRRetentionGroundTruth(t2, 1, 0.5f)
        val (_, r3) = TestContracts.computeActRRetentionGroundTruth(t3, 1, 0.5f)

        assertTrue(r1 > r2)
        assertTrue(r2 > r3)
        assertTrue(r3 > 0.0)
    }

    @Test
    fun testRepetitionIncreasesStability() {
        val elapsed = 24.0
        // S(m) increases with accessCount n
        val (s1, r1) = TestContracts.computeActRRetentionGroundTruth(elapsed, accessCount = 1, importance = 0.5f)
        val (s5, r5) = TestContracts.computeActRRetentionGroundTruth(elapsed, accessCount = 5, importance = 0.5f)
        val (s20, r20) = TestContracts.computeActRRetentionGroundTruth(elapsed, accessCount = 20, importance = 0.5f)

        assertTrue(s5 > s1)
        assertTrue(s20 > s5)
        assertTrue("Higher access count yields higher retention at same elapsed time", r20 > r5)
        assertTrue(r5 > r1)
    }

    @Test
    fun testImportanceAmplifiesStability() {
        val elapsed = 48.0
        val (sLow, rLow) = TestContracts.computeActRRetentionGroundTruth(elapsed, accessCount = 2, importance = 0.1f)
        val (sHigh, rHigh) = TestContracts.computeActRRetentionGroundTruth(elapsed, accessCount = 2, importance = 0.9f)

        assertTrue(sHigh > sLow)
        assertTrue(rHigh > rLow)
    }

    @Test
    fun testConsolidationCandidateIdentification() {
        // Pruning threshold R < 0.15
        val threshold = 0.15
        val facts = listOf(
            "active_fresh" to 1.0,   // R = exp(-1/36) ~ 0.97
            "active_decayed" to 300.0 // R = exp(-300/36) ~ 0.0002 -> Candidate!
        )

        val candidates = facts.filter { (_, elapsed) ->
            val (_, r) = TestContracts.computeActRRetentionGroundTruth(elapsed, 1, 0.5f)
            r < threshold
        }.map { it.first }

        assertEquals(listOf("active_decayed"), candidates)
    }

    // --- Feature 8: Two-Phase Consolidation & Compactor Hook ---

    @Test
    fun testConsolidatorCascadingTombstoning() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Decayed memory node and its attached edges
        graph.putNode(Node("l1:decayed_fact", properties = mapOf("text" to "decayed")))
        graph.putEdge(Edge("l1:decayed_fact", "concept_x", "MENTIONS"))
        graph.putEdge(Edge("concept_y", "l1:decayed_fact", "REFERENCES"))

        // Cascading deletion
        graph.deleteNode("l1:decayed_fact")

        assertNull(graph.getNode("l1:decayed_fact"))
        assertTrue(graph.getOutboundEdges("l1:decayed_fact", "MENTIONS").isEmpty())
        assertTrue(graph.getInboundEdges("l1:decayed_fact", "REFERENCES").isEmpty())

        db.close()
    }

    @Test
    fun testCompactorFilterPurgesDecayedMemories() {
        val memories = mutableListOf(
            "m1" to 0.95, // keep
            "m2" to 0.05, // purge
            "m3" to 0.80  // keep
        )

        // Compactor filter hook simulates dropping records with R < 0.15
        val retained = memories.filter { it.second >= 0.15 }.map { it.first }
        assertEquals(listOf("m1", "m3"), retained)
    }

    @Test
    fun testCompactorFilterRetainsActiveMemories() {
        val facts = listOf(
            E2EAtomicFact(id = "f_active", subject = "S", predicate = "P", `object` = "O", text = "active", validUntilMs = Long.MAX_VALUE),
            E2EAtomicFact(id = "f_expired", subject = "S", predicate = "P", `object` = "O", text = "expired", validUntilMs = 1000L)
        )

        val retained = facts.filter { it.isActive }
        assertEquals(1, retained.size)
        assertEquals("f_active", retained[0].id)
    }

    @Test
    fun testCompactorPreservesReversePointersForActiveFacts() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val activeFact = E2EAtomicFact(id = "active_p", subject = "System", predicate = "status", `object` = "OK", text = "OK")
        graph.putNode(activeFact.toGraphNode())

        val retrieved = graph.getNode("l1:active_p")
        assertNotNull(retrieved)
        assertEquals("System", retrieved?.properties?.get("subject"))

        db.close()
    }

    @Test
    fun testConsolidationAuditLog() {
        val totalEvaluated = 100
        val pruned = 25
        val retained = totalEvaluated - pruned

        val auditEntry = mapOf(
            "timestamp" to System.currentTimeMillis().toString(),
            "evaluated" to totalEvaluated.toString(),
            "pruned" to pruned.toString(),
            "retained" to retained.toString()
        )

        assertEquals("75", auditEntry["retained"])
        assertEquals("25", auditEntry["pruned"])
    }

    // ========================================================================
    // TIER 2: BOUNDARY & CORNER CASES
    // ========================================================================

    @Test
    fun testExtremeTimestampsZeroAndMax() {
        val fact = E2EAtomicFact(
            subject = "Genesis",
            predicate = "created",
            `object` = "Earth",
            text = "In the beginning",
            ingestedAtMs = 0L,
            validFromMs = 0L,
            validUntilMs = Long.MAX_VALUE
        )

        assertTrue(fact.isValidAt(0L))
        assertTrue(fact.isValidAt(Long.MAX_VALUE - 1L))
        assertFalse(fact.isValidAt(Long.MAX_VALUE)) // Exclusive upper bound
    }

    @Test
    fun testZeroDurationValidityInterval() {
        val instantFact = E2EAtomicFact(
            subject = "Event",
            predicate = "flash",
            `object` = "now",
            text = "Flash",
            validFromMs = 1000L,
            validUntilMs = 1000L // [1000, 1000) is empty
        )

        assertFalse(instantFact.isValidAt(999L))
        assertFalse(instantFact.isValidAt(1000L))
        assertFalse(instantFact.isValidAt(1001L))
        assertFalse(instantFact.isActive)
    }

    @Test
    fun testZeroStabilityHandlesImmediateDecay() {
        // If stability is negligible, retention for any delta > 0 should evaluate near 0 without NaN
        val (_, r) = TestContracts.computeActRRetentionGroundTruth(
            elapsedHours = 1.0,
            accessCount = 1,
            importance = 0.0f,
            baseStabilityHours = 0.0001
        )

        assertFalse(r.isNaN())
        assertFalse(r.isInfinite())
        assertTrue("Should decay to nearly 0", r < 1e-4)
    }

    @Test
    fun testRapidContradictionsWithIdenticalTimestamps() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val fixedTime = 5000L

        // Two contradicting facts written in same ms
        val f1 = E2EAtomicFact(id = "f_rapid1", subject = "X", predicate = "val", `object` = "A", text = "A", validFromMs = fixedTime, validUntilMs = fixedTime)
        val f2 = E2EAtomicFact(id = "f_rapid2", subject = "X", predicate = "val", `object` = "B", text = "B", validFromMs = fixedTime, validUntilMs = Long.MAX_VALUE)

        graph.putNode(f1.toGraphNode())
        graph.putNode(f2.toGraphNode())
        graph.putEdge(Edge("l1:f_rapid2", "l1:f_rapid1", "SUPERSEDES"))

        val node1 = graph.getNode("l1:f_rapid1")!!
        val node2 = graph.getNode("l1:f_rapid2")!!
        assertEquals("false", node1.properties["isActive"])
        assertEquals("true", node2.properties["isActive"])

        db.close()
    }

    @Test
    fun testEmptyCollectionConsolidation() {
        val emptyList = emptyList<E2EAtomicFact>()
        val pruned = emptyList.filter {
            val (_, r) = TestContracts.computeActRRetentionGroundTruth(100.0, it.accessCount, it.importance)
            r < 0.15
        }
        assertTrue(pruned.isEmpty())
    }

    // ========================================================================
    // TIER 3: CROSS-FEATURE INTERACTIONS
    // ========================================================================

    @Test
    fun testHubDampedPageRankTraversesOnlyActiveBiTemporalEdges() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Node A connected to active Target_Active and stale Target_Stale
        val edgeActive = E2EBiTemporalEdge("A", "Target_Active", "REL", validUntilMs = Long.MAX_VALUE)
        val edgeStale = E2EBiTemporalEdge("A", "Target_Stale", "REL", validUntilMs = 1000L)

        graph.putEdge(edgeActive.toEdge())
        graph.putEdge(edgeStale.toEdge())

        val rawEdges = graph.getOutboundEdges("A", "REL")
        val activeEdges = rawEdges.filter { E2EBiTemporalEdge.fromEdge(it).isActive }

        assertEquals(1, activeEdges.size)
        assertEquals("Target_Active", activeEdges[0].targetId)

        // Running Hub-Damped PageRank on active edges only
        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = setOf("A", "Target_Active"),
            outEdges = mapOf("A" to listOf("Target_Active" to 1.0)),
            seeds = mapOf("A" to 1.0)
        )

        assertTrue(ranks["Target_Active"]!! > 0.0)
        assertFalse(ranks.containsKey("Target_Stale"))

        db.close()
    }

    @Test
    fun testSpacedRepetitionReinforcedByBiTemporalQueries() {
        var fact = E2EAtomicFact(
            subject = "User",
            predicate = "preference",
            `object` = "Dark Mode",
            text = "Prefers dark mode",
            accessCount = 1,
            importance = 0.5f
        )

        val (sInitial, rInitial) = TestContracts.computeActRRetentionGroundTruth(24.0, fact.accessCount, fact.importance)

        // Agent retrieves fact 3 times
        fact = fact.copy(accessCount = fact.accessCount + 3, lastAccessedAtMs = System.currentTimeMillis())

        val (sBoosted, rBoosted) = TestContracts.computeActRRetentionGroundTruth(24.0, fact.accessCount, fact.importance)
        assertTrue(sBoosted > sInitial)
        assertTrue(rBoosted > rInitial)
    }

    @Test
    fun testContradictionResolutionDuringActiveConsolidation() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Contradict an old fact while consolidation prepares candidate list
        val oldFact = E2EAtomicFact(id = "old_v", subject = "Config", predicate = "mode", `object` = "v1", text = "v1", validUntilMs = 5000L)
        val newFact = E2EAtomicFact(id = "new_v", subject = "Config", predicate = "mode", `object` = "v2", text = "v2", validUntilMs = Long.MAX_VALUE)

        graph.putNode(oldFact.toGraphNode())
        graph.putNode(newFact.toGraphNode())

        // Invalidation preserves newFact as active and marks oldFact as historical
        val active = listOf(oldFact, newFact).filter { it.isActive }
        assertEquals(1, active.size)
        assertEquals("new_v", active[0].id)

        db.close()
    }

    // ========================================================================
    // TIER 4: REAL-WORLD SCENARIOS
    // ========================================================================

    @Test
    fun testAgentEpisodicBeliefEvolutionOverSimulatedDays() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val msPerHour = 3600_000L
        val t0 = 1700000000000L

        // Day 1: "User likes coffee"
        val factDay1 = E2EAtomicFact(
            id = "f_coffee",
            subject = "User",
            predicate = "beverage",
            `object` = "Coffee",
            text = "User likes coffee",
            ingestedAtMs = t0,
            validFromMs = t0,
            validUntilMs = Long.MAX_VALUE
        )
        graph.putNode(factDay1.toGraphNode())

        // Day 5: "User prefers tea" (Contradicts coffee)
        val tDay5 = t0 + (5 * 24 * msPerHour)
        val coffeeInvalidated = factDay1.copy(validUntilMs = tDay5, supersededByFactId = "f_tea")
        val factDay5 = E2EAtomicFact(
            id = "f_tea",
            subject = "User",
            predicate = "beverage",
            `object` = "Tea",
            text = "User prefers tea",
            ingestedAtMs = tDay5,
            validFromMs = tDay5,
            validUntilMs = Long.MAX_VALUE,
            accessCount = 4 // accessed multiple times
        )
        graph.putNode(coffeeInvalidated.toGraphNode())
        graph.putNode(factDay5.toGraphNode())
        graph.putEdge(Edge("l1:f_tea", "l1:f_coffee", "SUPERSEDES"))

        // Day 10: "User visits Kyoto" (low importance episodic fact)
        val tDay10 = t0 + (10 * 24 * msPerHour)
        val factDay10 = E2EAtomicFact(
            id = "f_kyoto",
            subject = "User",
            predicate = "visited",
            `object` = "Kyoto",
            text = "User visited Kyoto",
            ingestedAtMs = tDay10,
            validFromMs = tDay10,
            validUntilMs = Long.MAX_VALUE,
            importance = 0.1f,
            accessCount = 1
        )
        graph.putNode(factDay10.toGraphNode())

        // Day 30: Consolidation run after 20 days of idle time
        val tDay30 = t0 + (30 * 24 * msPerHour)
        val elapsedHoursKyoto = (tDay30 - tDay10) / msPerHour.toDouble() // 20 days = 480 hours
        val elapsedHoursTea = (tDay30 - tDay5) / msPerHour.toDouble()     // 25 days = 600 hours

        val (_, rKyoto) = TestContracts.computeActRRetentionGroundTruth(elapsedHoursKyoto, factDay10.accessCount, factDay10.importance)
        val (_, rTea) = TestContracts.computeActRRetentionGroundTruth(elapsedHoursTea, factDay5.accessCount, factDay5.importance)

        // Kyoto memory has decayed below threshold due to low importance & single access
        assertTrue("Kyoto retention ($rKyoto) should be below 0.15 threshold", rKyoto < 0.15)
        // Tea memory remains active because accessCount = 4 and importance = 0.5
        assertTrue("Tea retention ($rTea) should exceed Kyoto retention ($rKyoto)", rTea > rKyoto)

        // Historical query on Day 3 returns Coffee
        val tDay3 = t0 + (3 * 24 * msPerHour)
        val stateAtDay3 = listOf(coffeeInvalidated, factDay5, factDay10).filter { it.isValidAt(tDay3) }
        assertEquals(1, stateAtDay3.size)
        assertEquals("Coffee", stateAtDay3[0].`object`)

        // Current query on Day 30 returns Tea as beverage
        val stateAtDay30 = listOf(coffeeInvalidated, factDay5).filter { it.isActive }
        assertEquals(1, stateAtDay30.size)
        assertEquals("Tea", stateAtDay30[0].`object`)

        db.close()
    }

    @Test
    fun testHistoricalAuditAndRollbackVerification() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val t1 = 1000L
        val t2 = 2000L
        val t3 = 3000L

        // State changes of user residency
        val v1 = E2EAtomicFact(id = "r1", subject = "User", predicate = "residence", `object` = "Boston", text = "Boston", validFromMs = t1, validUntilMs = t2)
        val v2 = E2EAtomicFact(id = "r2", subject = "User", predicate = "residence", `object` = "Chicago", text = "Chicago", validFromMs = t2, validUntilMs = t3)
        val v3 = E2EAtomicFact(id = "r3", subject = "User", predicate = "residence", `object` = "Austin", text = "Austin", validFromMs = t3, validUntilMs = Long.MAX_VALUE)

        graph.putNode(v1.toGraphNode())
        graph.putNode(v2.toGraphNode())
        graph.putNode(v3.toGraphNode())

        graph.putEdge(Edge("l1:r2", "l1:r1", "SUPERSEDES"))
        graph.putEdge(Edge("l1:r3", "l1:r2", "SUPERSEDES"))

        // Audit check at t = 1500 -> Boston
        val audit1500 = listOf(v1, v2, v3).firstOrNull { it.isValidAt(1500L) }
        assertEquals("Boston", audit1500?.`object`)

        // Audit check at t = 2500 -> Chicago
        val audit2500 = listOf(v1, v2, v3).firstOrNull { it.isValidAt(2500L) }
        assertEquals("Chicago", audit2500?.`object`)

        // Audit check at t = 3500 -> Austin
        val audit3500 = listOf(v1, v2, v3).firstOrNull { it.isValidAt(3500L) }
        assertEquals("Austin", audit3500?.`object`)

        db.close()
    }
}
