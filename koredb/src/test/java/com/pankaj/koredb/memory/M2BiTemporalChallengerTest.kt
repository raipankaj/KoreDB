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
import com.pankaj.koredb.graph.BiTemporalEdge
import com.pankaj.koredb.graph.query.query
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Adversarial Challenger Test Suite for Milestone 2:
 * 1. Multi-Hop Contradiction Lineage (F1 -> F2 -> F3 -> F4) and bidirectional edge verification.
 * 2. Contradiction Key Isolation across distinct entities, attributes, and special characters.
 * 3. Point-in-Time Historical Reconstruction (asOf) across strict half-open boundary intervals.
 * 4. Active State Filtering (queryActive, outboundActive, inboundActive) edge/node strict exclusion.
 * 5. Deep Revision Stress and Idempotent Assertion.
 */
class M2BiTemporalChallengerTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_challenger_m2_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    /**
     * Challenge 1: Multi-Hop Contradiction Lineage (F1 -> F2 -> F3 -> F4).
     *
     * Ingests 4 sequential contradictory facts under the same contradictionKey.
     * Verifies:
     * - Unbroken chain of forward SUPERSEDES edges: F4 -> F3 -> F2 -> F1.
     * - Unbroken reciprocal chain of backward SUPERSEDED_BY edges: F1 -> F2 -> F3 -> F4.
     * - Exact traceFactLineage results:
     *     trace(F4) == [F4, F3, F2, F1]
     *     trace(F3) == [F3, F2, F1]
     *     trace(F2) == [F2, F1]
     *     trace(F1) == [F1]
     * - Valid until / from timestamps reflect exact handover points.
     */
    @Test
    fun testMultiHopContradictionLineageF1ToF4UnbrokenChain() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val t1 = 1000L
        val t2 = 2000L
        val t3 = 3000L
        val t4 = 4000L

        // F1: Alice lives in London
        val f1 = AtomicFact(
            id = "f1",
            subject = "Alice",
            predicate = "location",
            `object` = "London",
            text = "Alice lives in London",
            validFromMs = t1
        )
        graph.putFact(f1, detectContradictions = true, nowMs = t1)

        // F2: Alice moves to Paris
        val f2 = AtomicFact(
            id = "f2",
            subject = "Alice",
            predicate = "location",
            `object` = "Paris",
            text = "Alice lives in Paris",
            validFromMs = t2
        )
        graph.putFact(f2, detectContradictions = true, nowMs = t2)

        // F3: Alice moves to Berlin
        val f3 = AtomicFact(
            id = "f3",
            subject = "Alice",
            predicate = "location",
            `object` = "Berlin",
            text = "Alice lives in Berlin",
            validFromMs = t3
        )
        graph.putFact(f3, detectContradictions = true, nowMs = t3)

        // F4: Alice moves to Tokyo
        val f4 = AtomicFact(
            id = "f4",
            subject = "Alice",
            predicate = "location",
            `object` = "Tokyo",
            text = "Alice lives in Tokyo",
            validFromMs = t4
        )
        graph.putFact(f4, detectContradictions = true, nowMs = t4)

        // 1. Verify Nodes state
        val n1 = graph.getNode("l1:f1")!!
        val n2 = graph.getNode("l1:f2")!!
        val n3 = graph.getNode("l1:f3")!!
        val n4 = graph.getNode("l1:f4")!!

        assertEquals("false", n1.properties["isActive"])
        assertEquals(t2.toString(), n1.properties["validUntilMs"])
        assertEquals("f2", n1.properties["supersededByFactId"])

        assertEquals("false", n2.properties["isActive"])
        assertEquals(t3.toString(), n2.properties["validUntilMs"])
        assertEquals("f3", n2.properties["supersededByFactId"])

        assertEquals("false", n3.properties["isActive"])
        assertEquals(t4.toString(), n3.properties["validUntilMs"])
        assertEquals("f4", n3.properties["supersededByFactId"])

        assertEquals("true", n4.properties["isActive"])
        assertEquals(Long.MAX_VALUE.toString(), n4.properties["validUntilMs"])
        assertNull(n4.properties["supersededByFactId"])

        // 2. Verify Forward SUPERSEDES edges: F4 -> F3 -> F2 -> F1
        val f4Supersedes = graph.getOutboundEdges("l1:f4", BiTemporalEdge.TYPE_SUPERSEDES)
        assertEquals(1, f4Supersedes.size)
        assertEquals("l1:f3", f4Supersedes[0].targetId)
        assertEquals("contradiction", f4Supersedes[0].properties["reason"])

        val f3Supersedes = graph.getOutboundEdges("l1:f3", BiTemporalEdge.TYPE_SUPERSEDES)
        assertEquals(1, f3Supersedes.size)
        assertEquals("l1:f2", f3Supersedes[0].targetId)

        val f2Supersedes = graph.getOutboundEdges("l1:f2", BiTemporalEdge.TYPE_SUPERSEDES)
        assertEquals(1, f2Supersedes.size)
        assertEquals("l1:f1", f2Supersedes[0].targetId)

        val f1Supersedes = graph.getOutboundEdges("l1:f1", BiTemporalEdge.TYPE_SUPERSEDES)
        assertTrue("Oldest ancestor must have no outbound SUPERSEDES", f1Supersedes.isEmpty())

        // 3. Verify Reciprocal SUPERSEDED_BY edges: F1 -> F2 -> F3 -> F4
        val f1SupersededBy = graph.getOutboundEdges("l1:f1", BiTemporalEdge.TYPE_SUPERSEDED_BY)
        assertEquals(1, f1SupersededBy.size)
        assertEquals("l1:f2", f1SupersededBy[0].targetId)

        val f2SupersededBy = graph.getOutboundEdges("l1:f2", BiTemporalEdge.TYPE_SUPERSEDED_BY)
        assertEquals(1, f2SupersededBy.size)
        assertEquals("l1:f3", f2SupersededBy[0].targetId)

        val f3SupersededBy = graph.getOutboundEdges("l1:f3", BiTemporalEdge.TYPE_SUPERSEDED_BY)
        assertEquals(1, f3SupersededBy.size)
        assertEquals("l1:f4", f3SupersededBy[0].targetId)

        val f4SupersededBy = graph.getOutboundEdges("l1:f4", BiTemporalEdge.TYPE_SUPERSEDED_BY)
        assertTrue("Latest fact must have no outbound SUPERSEDED_BY", f4SupersededBy.isEmpty())

        // 4. Verify traceFactLineage from each version
        val lineageF4 = graph.traceFactLineage("f4")
        assertEquals(listOf("l1:f4", "l1:f3", "l1:f2", "l1:f1"), lineageF4.map { it.id })

        val lineageF3 = graph.traceFactLineage("f3")
        assertEquals(listOf("l1:f3", "l1:f2", "l1:f1"), lineageF3.map { it.id })

        val lineageF2 = graph.traceFactLineage("l1:f2") // prefixed format
        assertEquals(listOf("l1:f2", "l1:f1"), lineageF2.map { it.id })

        val lineageF1 = graph.traceFactLineage("l1:f1")
        assertEquals(listOf("l1:f1"), lineageF1.map { it.id })

        // Non-existent id returns empty
        val lineageMissing = graph.traceFactLineage("non_existent_fact")
        assertTrue(lineageMissing.isEmpty())

        db.close()
    }

    /**
     * Challenge 2: Contradiction Key Isolation.
     *
     * Validates that facts with different subjects or different predicates
     * NEVER invalidate each other even when updated concurrently or sequentially.
     */
    @Test
    fun testContradictionKeyIsolationAcrossAttributesAndEntities() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val t0 = 1000L

        // user:1 facts
        val u1Loc = AtomicFact(id = "u1_loc_v1", subject = "user:1", predicate = "location", `object` = "London", text = "u1 in London", validFromMs = t0)
        val u1Job = AtomicFact(id = "u1_job_v1", subject = "user:1", predicate = "job", `object` = "Engineer", text = "u1 is Engineer", validFromMs = t0)
        val u1Sal = AtomicFact(id = "u1_sal_v1", subject = "user:1", predicate = "salary", `object` = "100k", text = "u1 earns 100k", validFromMs = t0)

        // user:2 facts
        val u2Loc = AtomicFact(id = "u2_loc_v1", subject = "user:2", predicate = "location", `object` = "Paris", text = "u2 in Paris", validFromMs = t0)
        val u2Job = AtomicFact(id = "u2_job_v1", subject = "user:2", predicate = "job", `object` = "Designer", text = "u2 is Designer", validFromMs = t0)
        val u2Sal = AtomicFact(id = "u2_sal_v1", subject = "user:2", predicate = "salary", `object` = "120k", text = "u2 earns 120k", validFromMs = t0)

        graph.putFact(u1Loc, detectContradictions = true, nowMs = t0)
        graph.putFact(u1Job, detectContradictions = true, nowMs = t0)
        graph.putFact(u1Sal, detectContradictions = true, nowMs = t0)
        graph.putFact(u2Loc, detectContradictions = true, nowMs = t0)
        graph.putFact(u2Job, detectContradictions = true, nowMs = t0)
        graph.putFact(u2Sal, detectContradictions = true, nowMs = t0)

        // All 6 must initially be active
        val initialActive = graph.query {
            startingWithIds("l1:u1_loc_v1", "l1:u1_job_v1", "l1:u1_sal_v1", "l1:u2_loc_v1", "l1:u2_job_v1", "l1:u2_sal_v1")
            queryActive(t0)
        }.toNodeList()
        assertEquals(6, initialActive.size)

        // Update user:1:location to Berlin at t1 = 2000L
        val t1 = 2000L
        val u1LocV2 = AtomicFact(id = "u1_loc_v2", subject = "user:1", predicate = "location", `object` = "Berlin", text = "u1 in Berlin", validFromMs = t1)
        graph.putFact(u1LocV2, detectContradictions = true, nowMs = t1)

        // Verify ONLY user:1:location:v1 is superseded
        val nU1LocV1 = graph.getNode("l1:u1_loc_v1")!!
        assertEquals("false", nU1LocV1.properties["isActive"])
        assertEquals(t1.toString(), nU1LocV1.properties["validUntilMs"])

        // user:1:job and user:1:salary MUST still be active
        val nU1Job = graph.getNode("l1:u1_job_v1")!!
        val nU1Sal = graph.getNode("l1:u1_sal_v1")!!
        assertEquals("true", nU1Job.properties["isActive"])
        assertEquals("true", nU1Sal.properties["isActive"])
        assertTrue(graph.getOutboundEdges("l1:u1_job_v1", BiTemporalEdge.TYPE_SUPERSEDES).isEmpty())
        assertTrue(graph.getOutboundEdges("l1:u1_job_v1", BiTemporalEdge.TYPE_SUPERSEDED_BY).isEmpty())

        // user:2:location MUST still be active and untouched
        val nU2Loc = graph.getNode("l1:u2_loc_v1")!!
        assertEquals("true", nU2Loc.properties["isActive"])
        assertEquals("Paris", nU2Loc.properties["object"])
        assertTrue(graph.getOutboundEdges("l1:u2_loc_v1", BiTemporalEdge.TYPE_SUPERSEDES).isEmpty())

        // Update user:2:location to Tokyo at t2 = 3000L
        val t2 = 3000L
        val u2LocV2 = AtomicFact(id = "u2_loc_v2", subject = "user:2", predicate = "location", `object` = "Tokyo", text = "u2 in Tokyo", validFromMs = t2)
        graph.putFact(u2LocV2, detectContradictions = true, nowMs = t2)

        // Lineage isolation check
        val u1LocLineage = graph.traceFactLineage("u1_loc_v2").map { it.id }
        assertEquals(listOf("l1:u1_loc_v2", "l1:u1_loc_v1"), u1LocLineage)

        val u2LocLineage = graph.traceFactLineage("u2_loc_v2").map { it.id }
        assertEquals(listOf("l1:u2_loc_v2", "l1:u2_loc_v1"), u2LocLineage)

        // No cross-entity contamination in lineages
        assertFalse(u1LocLineage.any { it.contains("u2") })
        assertFalse(u2LocLineage.any { it.contains("u1") })

        db.close()
    }

    /**
     * Challenge 3: Point-in-Time Historical Reconstruction (asOf).
     *
     * Validates exact temporal accuracy and half-open boundary intervals:
     *   F1: [1000, 2000) -> State1
     *   F2: [2000, 3000) -> State2
     *   F3: [3000, 4000) -> State3
     *   F4: [4000, Long.MAX_VALUE) -> State4
     *
     * Tests:
     *   t < 1000 (pre-existence)
     *   t = 1000 (exact boundary F1)
     *   t = 1999 (interior F1)
     *   t = 2000 (exact boundary F2, F1 excluded)
     *   t = 2999 (interior F2)
     *   t = 3000 (exact boundary F3, F2 excluded)
     *   t = 3999 (interior F3)
     *   t = 4000 (exact boundary F4, F3 excluded)
     *   t = 10000 (future active)
     *
     * Also tests 2D bi-temporal slicing (asOf with both validTime and transactionTime).
     */
    @Test
    fun testPointInTimeHistoricalReconstructionAsOf() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val t1 = 1000L
        val t2 = 2000L
        val t3 = 3000L
        val t4 = 4000L

        // Ingest 4 versions sequentially
        val f1 = AtomicFact(id = "srv_v1", subject = "Server", predicate = "leader", `object` = "NodeA", text = "Leader NodeA", validFromMs = t1)
        graph.putFact(f1, detectContradictions = true, nowMs = t1)

        val f2 = AtomicFact(id = "srv_v2", subject = "Server", predicate = "leader", `object` = "NodeB", text = "Leader NodeB", validFromMs = t2)
        graph.putFact(f2, detectContradictions = true, nowMs = t2)

        val f3 = AtomicFact(id = "srv_v3", subject = "Server", predicate = "leader", `object` = "NodeC", text = "Leader NodeC", validFromMs = t3)
        graph.putFact(f3, detectContradictions = true, nowMs = t3)

        val f4 = AtomicFact(id = "srv_v4", subject = "Server", predicate = "leader", `object` = "NodeD", text = "Leader NodeD", validFromMs = t4)
        graph.putFact(f4, detectContradictions = true, nowMs = t4)

        val allIds = arrayOf("l1:srv_v1", "l1:srv_v2", "l1:srv_v3", "l1:srv_v4")

        fun queryLeaderAt(t: Long): String? {
            val nodes = graph.query {
                startingWithIds(*allIds)
                asOf(t)
            }.toNodeList()
            return nodes.singleOrNull()?.properties?.get("object")
        }

        // Before t1: no fact was valid
        assertNull("t = 500 is before t1", queryLeaderAt(500L))
        assertNull("t = 999 is before t1", queryLeaderAt(999L))

        // Exact t1: NodeA
        assertEquals("NodeA", queryLeaderAt(1000L))
        // Interior of [1000, 2000): NodeA
        assertEquals("NodeA", queryLeaderAt(1500L))
        assertEquals("NodeA", queryLeaderAt(1999L))

        // Exact boundary t2: NodeB (half-open [1000, 2000) excludes 2000 for NodeA; [2000, 3000) includes 2000 for NodeB)
        assertEquals("NodeB", queryLeaderAt(2000L))
        // Interior of [2000, 3000): NodeB
        assertEquals("NodeB", queryLeaderAt(2500L))
        assertEquals("NodeB", queryLeaderAt(2999L))

        // Exact boundary t3: NodeC
        assertEquals("NodeC", queryLeaderAt(3000L))
        // Interior of [3000, 4000): NodeC
        assertEquals("NodeC", queryLeaderAt(3500L))
        assertEquals("NodeC", queryLeaderAt(3999L))

        // Exact boundary t4: NodeD
        assertEquals("NodeD", queryLeaderAt(4000L))
        // Open future: NodeD
        assertEquals("NodeD", queryLeaderAt(5000L))
        assertEquals("NodeD", queryLeaderAt(1_000_000L))

        // 2D Bi-temporal slicing: Domain validity vs Transaction Time awareness
        // Suppose a retrospective fact was valid in the domain at 1500L, but not ingested until 6000L
        val retroFact = AtomicFact(
            id = "retro_fact",
            subject = "Audit",
            predicate = "finding",
            `object` = "ComplianceBreach",
            text = "Breach at 1500 recorded at 6000",
            ingestedAtMs = 6000L,
            validFromMs = 1500L,
            validUntilMs = Long.MAX_VALUE
        )
        graph.putNode(retroFact.toGraphNode())

        // Query at validTime = 2000L, asOf txTime = 5000L: Database did NOT yet know about this finding
        val resultBeforeIngestion = graph.query {
            startingWithIds("l1:retro_fact")
            asOf(validTimeMs = 2000L, asOfTransactionTimeMs = 5000L)
        }.toNodeList()
        assertTrue("Database was unaware of retro_fact at txTime=5000", resultBeforeIngestion.isEmpty())

        // Query at validTime = 2000L, asOf txTime = 6000L: Database now knows about it
        val resultAtIngestion = graph.query {
            startingWithIds("l1:retro_fact")
            asOf(validTimeMs = 2000L, asOfTransactionTimeMs = 6000L)
        }.toNodeList()
        assertEquals(1, resultAtIngestion.size)
        assertEquals("ComplianceBreach", resultAtIngestion[0].properties["object"])

        db.close()
    }

    /**
     * Challenge 4: Active State Filtering (queryActive, outboundActive, inboundActive).
     *
     * Validates that:
     * - queryActive strictly excludes superseded, past-expired, and future-dated nodes.
     * - outboundActive and inboundActive strictly traverse only edges that are valid at nowMs,
     *   blocking multi-hop traversals when an intermediate edge is expired or future-dated.
     */
    @Test
    fun testActiveStateFilteringStrictExclusion() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val now = 5000L

        // Node state filtering:
        // f_active: [1000, MAX) -> ACTIVE
        // f_expired: [1000, 4000) -> INACTIVE
        // f_future: [7000, MAX) -> INACTIVE at now=5000
        // f_superseded: [1000, 3000) with isActive=false -> INACTIVE
        val fActive = AtomicFact(id = "n_active", subject = "X", predicate = "status", `object` = "Active", text = "A", validFromMs = 1000L, validUntilMs = Long.MAX_VALUE)
        val fExpired = AtomicFact(id = "n_expired", subject = "X", predicate = "status", `object` = "Expired", text = "E", validFromMs = 1000L, validUntilMs = 4000L)
        val fFuture = AtomicFact(id = "n_future", subject = "X", predicate = "status", `object` = "Future", text = "F", validFromMs = 7000L, validUntilMs = Long.MAX_VALUE)
        val fSuperseded = AtomicFact(id = "n_superseded", subject = "X", predicate = "status", `object` = "Super", text = "S", validFromMs = 1000L, validUntilMs = 3000L, supersededByFactId = "n_active")

        graph.putNode(fActive.toGraphNode())
        graph.putNode(fExpired.toGraphNode())
        graph.putNode(fFuture.toGraphNode())
        graph.putNode(fSuperseded.toGraphNode())

        val activeNodes = graph.query {
            startingWithIds("l1:n_active", "l1:n_expired", "l1:n_future", "l1:n_superseded")
            queryActive(now)
        }.toNodeList()

        assertEquals("queryActive must return exactly 1 active node", 1, activeNodes.size)
        assertEquals("l1:n_active", activeNodes[0].id)

        // Edge traversal filtering:
        // Root -> ChildActive (valid [1000, MAX))
        // Root -> ChildExpired (valid [1000, 4000))
        // Root -> ChildFuture (valid [8000, MAX))
        val eActive = BiTemporalEdge("Root", "ChildActive", "LINKS", validFromMs = 1000L, validUntilMs = Long.MAX_VALUE)
        val eExpired = BiTemporalEdge("Root", "ChildExpired", "LINKS", validFromMs = 1000L, validUntilMs = 4000L)
        val eFuture = BiTemporalEdge("Root", "ChildFuture", "LINKS", validFromMs = 8000L, validUntilMs = Long.MAX_VALUE)

        graph.putEdge(eActive.toEdge())
        graph.putEdge(eExpired.toEdge())
        graph.putEdge(eFuture.toEdge())

        val outboundActive = graph.query {
            startingWith("Root")
            outboundActive("LINKS", nowMs = now)
        }.toIdList()

        assertEquals(listOf("ChildActive"), outboundActive)

        // Inbound active traversal check
        val inboundActive = graph.query {
            startingWith("ChildActive")
            inboundActive("LINKS", nowMs = now)
        }.toIdList()
        assertEquals(listOf("Root"), inboundActive)

        val inboundExpired = graph.query {
            startingWith("ChildExpired")
            inboundActive("LINKS", nowMs = now)
        }.toIdList()
        assertTrue("Expired edge must not be traversed by inboundActive at now=5000", inboundExpired.isEmpty())

        // Multi-hop edge traversal with broken chain:
        // A -(active)-> B -(expired)-> C -(active)-> D
        val edgeAB = BiTemporalEdge("A", "B", "FLOW", validFromMs = 1000L, validUntilMs = Long.MAX_VALUE)
        val edgeBC_expired = BiTemporalEdge("B", "C", "FLOW", validFromMs = 1000L, validUntilMs = 3000L)
        val edgeCD = BiTemporalEdge("C", "D", "FLOW", validFromMs = 1000L, validUntilMs = Long.MAX_VALUE)

        graph.putEdge(edgeAB.toEdge())
        graph.putEdge(edgeBC_expired.toEdge())
        graph.putEdge(edgeCD.toEdge())

        // 1 hop from A -> reaches B
        val hop1 = graph.query {
            startingWith("A")
            outboundActive("FLOW", nowMs = now, hops = 1)
        }.toIdList()
        assertEquals(listOf("B"), hop1)

        // 2 hops from A -> should fail to reach C because B->C is expired
        val hop2 = graph.query {
            startingWith("A")
            outboundActive("FLOW", nowMs = now, hops = 2)
        }.toIdList()
        assertTrue("Multi-hop active traversal must stop at expired edge B->C", hop2.isEmpty())

        db.close()
    }

    /**
     * Challenge 5: Deep Revision Stress (10 Revisions) and Idempotent Assertion.
     *
     * Validates:
     * - Ingesting 10 successive revisions under the same contradiction key creates an unbroken 10-node lineage.
     * - Exactly 1 fact remains active (v10).
     * - Re-asserting an identical active fact (same object) does NOT trigger a contradiction,
     *   does not create self-superseding edges, and leaves lineage stable.
     */
    @Test
    fun testDeepRevisionStressAndIdempotentAssertion() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val revisionCount = 10
        for (i in 1..revisionCount) {
            val fact = AtomicFact(
                id = "counter_v$i",
                subject = "System",
                predicate = "counter",
                `object` = "Value-$i",
                text = "Counter is Value-$i",
                validFromMs = i * 1000L
            )
            graph.putFact(fact, detectContradictions = true, nowMs = i * 1000L)
        }

        // Trace from latest
        val lineage = graph.traceFactLineage("counter_v$revisionCount")
        assertEquals(revisionCount, lineage.size)

        // Verify ordering: v10 -> v9 -> ... -> v1
        for (i in 0 until revisionCount) {
            val expectedRevision = revisionCount - i
            assertEquals("l1:counter_v$expectedRevision", lineage[i].id)
            val expectedIsActive = if (expectedRevision == revisionCount) "true" else "false"
            assertEquals(expectedIsActive, lineage[i].properties["isActive"])
        }

        // Idempotency: re-asserting current active state "Value-10"
        val idempotentFact = AtomicFact(
            id = "counter_v10_duplicate",
            subject = "System",
            predicate = "counter",
            `object` = "Value-$revisionCount", // SAME object
            text = "Counter is Value-$revisionCount again",
            validFromMs = (revisionCount + 1) * 1000L
        )
        graph.putFact(idempotentFact, detectContradictions = true, nowMs = (revisionCount + 1) * 1000L)

        // Ensure no self-superseding or duplicate invalidation occurred on counter_v10
        val v10Node = graph.getNode("l1:counter_v$revisionCount")!!
        assertEquals("true", v10Node.properties["isActive"])
        val v10Supersedes = graph.getOutboundEdges("l1:counter_v$revisionCount", BiTemporalEdge.TYPE_SUPERSEDES)
        assertEquals("v10 must still supersede exactly v9", 1, v10Supersedes.size)
        assertEquals("l1:counter_v9", v10Supersedes[0].targetId)

        db.close()
    }

    /**
     * Challenge 6: Delimiter and Special Character Escaping in Contradiction Keys.
     *
     * Validates that subject and predicate containing colons, slashes, hashtags, and spaces
     * (e.g. URI namespaces, complex predicate paths) do not collide or break contradiction detection.
     */
    @Test
    fun testContradictionKeySpecialCharactersAndURIs() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val uriSubject = "urn:koredb:tenant:42/cluster#primary:node-01"
        val complexPredicate = "config://network/interface:eth0/ipv4"

        val t0 = 1000L
        val t1 = 2000L

        val fact1 = AtomicFact(
            id = "net_v1",
            subject = uriSubject,
            predicate = complexPredicate,
            `object` = "192.168.1.100",
            text = "IP is 192.168.1.100",
            validFromMs = t0
        )
        graph.putFact(fact1, detectContradictions = true, nowMs = t0)

        // Verify key formatting
        assertEquals("$uriSubject:$complexPredicate", fact1.contradictionKey)

        // New IP address update
        val fact2 = AtomicFact(
            id = "net_v2",
            subject = uriSubject,
            predicate = complexPredicate,
            `object` = "10.0.0.50",
            text = "IP updated to 10.0.0.50",
            validFromMs = t1
        )
        graph.putFact(fact2, detectContradictions = true, nowMs = t1)

        val node1 = graph.getNode("l1:net_v1")!!
        val node2 = graph.getNode("l1:net_v2")!!

        assertEquals("false", node1.properties["isActive"])
        assertEquals("true", node2.properties["isActive"])
        assertEquals("net_v2", node1.properties["supersededByFactId"])

        val lineage = graph.traceFactLineage("net_v2")
        assertEquals(listOf("l1:net_v2", "l1:net_v1"), lineage.map { it.id })

        db.close()
    }
}
