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
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.query.GraphQuery
import com.pankaj.koredb.graph.query.query
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Unit test suite for Milestone 2:
 * 1. Bi-Temporal Edge & Fact Models
 * 2. Contradiction Detection & Atomic Invalidation Pipeline
 * 3. Temporal Query DSL (activeOnly, outboundActive, inboundActive, asOf)
 * 4. Fact Lineage Tracing via SUPERSEDES links
 */
class BiTemporalFactInvalidationTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_unit_bitemporal_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    @Test
    fun testAtomicFactDefaultIntervalsAndContainment() {
        val now = 1700000000000L
        val fact = AtomicFact(
            id = "f_alice",
            subject = "Alice",
            predicate = "livesIn",
            `object` = "Tokyo",
            text = "Alice lives in Tokyo",
            ingestedAtMs = now,
            validFromMs = now,
            validUntilMs = Long.MAX_VALUE
        )

        assertEquals("Alice:livesIn", fact.contradictionKey)
        assertTrue(fact.isActive)
        assertTrue(fact.isValidAt(now))
        assertTrue(fact.isValidAt(now + 10_000_000L))
        assertFalse(fact.isValidAt(now - 1L))

        // Half-open interval containment
        val boundedFact = fact.copy(validFromMs = 1000L, validUntilMs = 5000L)
        assertFalse(boundedFact.isValidAt(999L))
        assertTrue(boundedFact.isValidAt(1000L))
        assertTrue(boundedFact.isValidAt(4999L))
        assertFalse(boundedFact.isValidAt(5000L)) // Exclusive upper bound
        assertFalse(boundedFact.isActive)
    }

    @Test
    fun testBiTemporalEdgeConversionAndProperties() {
        val now = 1700000000000L
        val bEdge = BiTemporalEdge(
            sourceId = "Alice",
            targetId = "Tokyo",
            type = "LOCATED_IN",
            ingestedAtMs = now,
            validFromMs = now,
            validUntilMs = Long.MAX_VALUE,
            properties = mapOf("source" to "sensor_gps", "confidence" to "0.98")
        )

        val underlyingEdge = bEdge.toEdge()
        assertEquals("Alice", underlyingEdge.sourceId)
        assertEquals("Tokyo", underlyingEdge.targetId)
        assertEquals("LOCATED_IN", underlyingEdge.type)
        assertEquals(now.toString(), underlyingEdge.properties[BiTemporalEdge.PROP_INGESTED_AT])
        assertEquals(now.toString(), underlyingEdge.properties[BiTemporalEdge.PROP_VALID_FROM])
        assertEquals(Long.MAX_VALUE.toString(), underlyingEdge.properties[BiTemporalEdge.PROP_VALID_UNTIL])
        assertEquals("true", underlyingEdge.properties[BiTemporalEdge.PROP_IS_ACTIVE])
        assertEquals("sensor_gps", underlyingEdge.properties["source"])

        // Reconstruct back
        val reconstructed = BiTemporalEdge.fromEdge(underlyingEdge)
        assertEquals(bEdge.sourceId, reconstructed.sourceId)
        assertEquals(bEdge.targetId, reconstructed.targetId)
        assertEquals(bEdge.type, reconstructed.type)
        assertEquals(bEdge.ingestedAtMs, reconstructed.ingestedAtMs)
        assertEquals(bEdge.validFromMs, reconstructed.validFromMs)
        assertEquals(bEdge.validUntilMs, reconstructed.validUntilMs)
        assertEquals(bEdge.properties, reconstructed.properties)
        assertTrue(reconstructed.isActive)
    }

    @Test
    fun testActiveFactQueryReturnsOnlyCurrentFacts() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val now = 5000L

        val fact1 = AtomicFact(id = "f1", subject = "Server", predicate = "status", `object` = "OK", text = "OK", validFromMs = 1000L, validUntilMs = Long.MAX_VALUE)
        val fact2 = AtomicFact(id = "f2", subject = "Server", predicate = "status", `object` = "BOOT", text = "BOOT", validFromMs = 500L, validUntilMs = 1000L)
        val fact3 = AtomicFact(id = "f3", subject = "Server", predicate = "status", `object` = "MAINT", text = "MAINT", validFromMs = 8000L, validUntilMs = Long.MAX_VALUE)

        graph.putNode(fact1.toGraphNode())
        graph.putNode(fact2.toGraphNode())
        graph.putNode(fact3.toGraphNode())

        val activeNodes = graph.query {
            startingWithIds("l1:f1", "l1:f2", "l1:f3")
            activeOnly(now)
        }.toNodeList()

        assertEquals(1, activeNodes.size)
        assertEquals("l1:f1", activeNodes[0].id)
        assertEquals("OK", activeNodes[0].properties["object"])

        db.close()
    }

    @Test
    fun testContradictionAtomicallyInvalidatesAndCreatesSupersedesEdge() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val t0 = 1000L
        val t1 = 2000L

        // Ingest initial fact at t0
        val fact1 = AtomicFact(
            id = "f1",
            subject = "Bob",
            predicate = "residence",
            `object` = "New York",
            text = "Bob lives in New York",
            validFromMs = t0
        )
        graph.putFact(fact1, detectContradictions = true, nowMs = t0)

        val retrieved1 = graph.getNode("l1:f1")!!
        assertEquals("true", retrieved1.properties["isActive"])
        assertEquals(Long.MAX_VALUE.toString(), retrieved1.properties["validUntilMs"])

        // Ingest contradicting fact at t1 (same contradictionKey: Bob:residence)
        val fact2 = AtomicFact(
            id = "f2",
            subject = "Bob",
            predicate = "residence",
            `object` = "San Francisco",
            text = "Bob moved to San Francisco",
            validFromMs = t1
        )
        graph.putFact(fact2, detectContradictions = true, nowMs = t1)

        // Verify old fact is invalidated
        val updatedNode1 = graph.getNode("l1:f1")!!
        assertEquals("false", updatedNode1.properties["isActive"])
        assertEquals(t1.toString(), updatedNode1.properties["validUntilMs"])
        assertEquals("f2", updatedNode1.properties["supersededByFactId"])

        // Verify new fact is active
        val newNode2 = graph.getNode("l1:f2")!!
        assertEquals("true", newNode2.properties["isActive"])
        assertEquals(t1.toString(), newNode2.properties["validFromMs"])
        assertEquals(Long.MAX_VALUE.toString(), newNode2.properties["validUntilMs"])

        // Verify directed SUPERSEDES edge: f2 -> f1
        val supersedesEdges = graph.getOutboundEdges("l1:f2", BiTemporalEdge.TYPE_SUPERSEDES)
        assertEquals(1, supersedesEdges.size)
        assertEquals("l1:f1", supersedesEdges[0].targetId)

        // Verify reciprocal SUPERSEDED_BY edge: f1 -> f2
        val supersededByEdges = graph.getOutboundEdges("l1:f1", BiTemporalEdge.TYPE_SUPERSEDED_BY)
        assertEquals(1, supersededByEdges.size)
        assertEquals("l1:f2", supersededByEdges[0].targetId)

        db.close()
    }

    @Test
    fun testHistoricalAsOfQueryReconstructsPastTruth() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val v1 = AtomicFact(id = "car_v1", subject = "Car", predicate = "color", `object` = "Red", text = "Car is Red", validFromMs = 1000L, validUntilMs = 5000L)
        val v2 = AtomicFact(id = "car_v2", subject = "Car", predicate = "color", `object` = "Blue", text = "Car is Blue", validFromMs = 5000L, validUntilMs = 9000L)
        val v3 = AtomicFact(id = "car_v3", subject = "Car", predicate = "color", `object` = "Green", text = "Car is Green", validFromMs = 9000L, validUntilMs = Long.MAX_VALUE)

        graph.putNode(v1.toGraphNode())
        graph.putNode(v2.toGraphNode())
        graph.putNode(v3.toGraphNode())

        val allIds = arrayOf("l1:car_v1", "l1:car_v2", "l1:car_v3")

        // At t = 3000 -> Red
        val at3000 = graph.query {
            startingWith(*allIds)
            asOf(3000L)
        }.toNodeList()
        assertEquals(1, at3000.size)
        assertEquals("Red", at3000[0].properties["object"])

        // At t = 7000 -> Blue
        val at7000 = graph.query {
            startingWith(*allIds)
            asOf(7000L)
        }.toNodeList()
        assertEquals(1, at7000.size)
        assertEquals("Blue", at7000[0].properties["object"])

        // At t = 12000 -> Green
        val at12000 = graph.query {
            startingWith(*allIds)
            asOf(12000L)
        }.toNodeList()
        assertEquals(1, at12000.size)
        assertEquals("Green", at12000[0].properties["object"])

        db.close()
    }

    @Test
    fun testBiTemporalAuditAsOfTransactionTime() {
        // Real-world validity starts at 1000L, but ingested into KoreDB at 5000L
        val fact = AtomicFact(
            id = "audit_fact",
            subject = "Contract",
            predicate = "status",
            `object` = "Signed",
            text = "Contract signed",
            ingestedAtMs = 5000L,
            validFromMs = 1000L,
            validUntilMs = Long.MAX_VALUE
        )

        // As of txTime = 3000L, system was unaware of this fact
        assertFalse(fact.isBiTemporallyValidAt(validTimeMs = 2000L, asOfTransactionTimeMs = 3000L))

        // As of txTime = 5000L, system was aware
        assertTrue(fact.isBiTemporallyValidAt(validTimeMs = 2000L, asOfTransactionTimeMs = 5000L))
        assertTrue(fact.isBiTemporallyValidAt(validTimeMs = 10000L, asOfTransactionTimeMs = 6000L))
    }

    @Test
    fun testLineageTracingTraversesEntireHistory() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Chain 4 versions: v1 -> v2 -> v3 -> v4
        var previousFact: AtomicFact? = null
        for (i in 1..4) {
            val fact = AtomicFact(
                id = "salary_v$i",
                subject = "Employee",
                predicate = "salary",
                `object` = "${i * 10000}",
                text = "Salary is ${i * 10000}",
                validFromMs = i * 1000L
            )
            if (previousFact != null) {
                graph.invalidateAndSupersedeFact(previousFact.id, fact, nowMs = i * 1000L)
            } else {
                graph.putNode(fact.toGraphNode())
            }
            previousFact = fact
        }

        val lineage = graph.traceFactLineage("salary_v4")
        assertEquals(4, lineage.size)
        assertEquals("l1:salary_v4", lineage[0].id)
        assertEquals("l1:salary_v3", lineage[1].id)
        assertEquals("l1:salary_v2", lineage[2].id)
        assertEquals("l1:salary_v1", lineage[3].id)

        db.close()
    }

    @Test
    fun testOutboundActiveAndInboundActiveTraversals() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val now = 5000L

        // Alice has 2 devices: Phone (active) and Pager (expired at 3000)
        val activeEdge = BiTemporalEdge("Alice", "Phone", "OWNS", ingestedAtMs = 1000L, validFromMs = 1000L, validUntilMs = Long.MAX_VALUE)
        val expiredEdge = BiTemporalEdge("Alice", "Pager", "OWNS", ingestedAtMs = 1000L, validFromMs = 1000L, validUntilMs = 3000L)

        graph.putEdge(activeEdge.toEdge())
        graph.putEdge(expiredEdge.toEdge())

        val outboundActiveTargets = graph.query {
            startingWith("Alice")
            outboundActive("OWNS", now)
        }.toIdList()

        assertEquals(listOf("Phone"), outboundActiveTargets)

        // Reverse traversal
        val inboundActiveSources = graph.query {
            startingWith("Phone")
            inboundActive("OWNS", now)
        }.toIdList()

        assertEquals(listOf("Alice"), inboundActiveSources)

        db.close()
    }

    @Test
    fun testNonContradictingFactsCoexist() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val f1 = AtomicFact(id = "f1", subject = "User", predicate = "language", `object` = "Kotlin", text = "Likes Kotlin")
        val f2 = AtomicFact(id = "f2", subject = "User", predicate = "editor", `object` = "AndroidStudio", text = "Uses AS")

        graph.putFact(f1)
        graph.putFact(f2)

        val n1 = graph.getNode("l1:f1")!!
        val n2 = graph.getNode("l1:f2")!!
        assertEquals("true", n1.properties["isActive"])
        assertEquals("true", n2.properties["isActive"])
        assertTrue(graph.getOutboundEdges("l1:f2", BiTemporalEdge.TYPE_SUPERSEDES).isEmpty())

        db.close()
    }
}
