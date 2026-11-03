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
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.Random
import java.util.UUID
import kotlin.math.sqrt

/**
 * Comprehensive Unit Test Suite for [KoreMemoryPyramid]:
 * - L0 LZ4 Evidence Ingestion & Decompression
 * - L1 Atomic Fact Dual Indexing & Evidence Linking
 * - L2 Zero-LLM Louvain Community Clustering & Centroid Embeddings
 * - L3 Persona & Invariant Governance
 * - Hybrid Retrieval with Reciprocal Rank Fusion & Evidence Enrichment
 * - Hierarchical Multi-Hop Drill-Down
 */
class KoreMemoryPyramidTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase
    private lateinit var pyramid: KoreMemoryPyramid

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_unit_pyramid_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
        pyramid = db.memoryPyramid()
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    private fun generateUnitVector(dim: Int, seed: Long): FloatArray {
        val rand = Random(seed)
        val vec = FloatArray(dim) { rand.nextGaussian().toFloat() }
        var normSq = 0.0f
        for (v in vec) normSq += v * v
        val norm = sqrt(normSq)
        for (i in 0 until dim) vec[i] /= norm
        return vec
    }

    // --- L0 Raw Evidence ---

    @Test
    fun testRecordEvidenceCompressesPayloadAndRestoresFidelity() = runBlocking {
        val text = "KoreDB provides high-speed embedded transactional graph and vector capabilities."
        val evId = pyramid.recordEvidence(
            content = text,
            mimeType = "text/plain",
            sourceUri = "test://session/1",
            metadata = mapOf("author" to "Alice")
        )

        val decompressed = pyramid.getDecompressedEvidence(evId)
        assertNotNull(decompressed)
        assertEquals(evId, decompressed?.evidenceId)
        assertEquals(text, decompressed?.rawContent)
        assertEquals("text/plain", decompressed?.mimeType)
        assertEquals("test://session/1", decompressed?.sourceUri)
        assertEquals("Alice", decompressed?.metadata?.get("author"))
        assertTrue((decompressed?.compressedSize ?: 0) > 0)
        assertEquals(text.toByteArray(Charsets.UTF_8).size, decompressed?.originalSize)

        // Check Graph Mirror Node
        val mirrorNode = pyramid.graph.getNode("l0:$evId")
        assertNotNull(mirrorNode)
        assertEquals(setOf(MemoryLabels.L0_EVIDENCE), mirrorNode?.labels)
        assertEquals(evId, mirrorNode?.properties?.get("evidenceId"))
        assertEquals("text/plain", mirrorNode?.properties?.get("mimeType"))
    }

    @Test
    fun testGetDecompressedEvidenceNonExistentReturnsNull() = runBlocking {
        val result = pyramid.getDecompressedEvidence("ev_non_existent")
        assertNull(result)
    }

    // --- L1 Atomic Facts ---

    @Test
    fun testAddFactDualIndexesBM25AndHNSWAndLinksEvidence() = runBlocking {
        val evId = pyramid.recordEvidence("System log: database started successfully.")
        val vec = generateUnitVector(64, 42L)

        val factId = pyramid.addFact(
            text = "Database started at normal operational speed",
            evidenceIds = listOf(evId),
            embedding = vec,
            confidence = 0.95f,
            subject = "Database",
            predicate = "status",
            `object` = "started"
        )

        // Graph node
        val factNode = pyramid.graph.getNode("l1:$factId")
        assertNotNull(factNode)
        assertEquals(setOf(MemoryLabels.L1_FACT), factNode?.labels)
        assertEquals("Database", factNode?.properties?.get("subject"))
        assertEquals("0.95", factNode?.properties?.get("confidence"))

        // Evidence link
        val inboundSources = pyramid.graph.getInboundSourceIds("l1:$factId", MemoryEdgeTypes.EVIDENCE_FOR)
        assertEquals(listOf("l0:$evId"), inboundSources)

        // BM25 Search
        val ftsHits = pyramid.ftsIndex.search("operational speed")
        assertTrue(ftsHits.any { it.first.contains(factId) })

        // Vector Search
        val vecHits = pyramid.vectorCollection.search(vec, limit = 1)
        assertEquals("l1:$factId", vecHits[0].first)
        assertTrue(vecHits[0].second >= 0.99f)
    }

    @Test
    fun testLinkFactsEstablishesWeightedGraphEdge() = runBlocking {
        val f1 = pyramid.addFact("Fact A: Memory caching is enabled")
        val f2 = pyramid.addFact("Fact B: Read latency is reduced")

        pyramid.linkFacts(f1, f2, relationType = MemoryEdgeTypes.CAUSES, weight = 2.5)

        val outbound = pyramid.graph.getOutboundEdges("l1:$f1", MemoryEdgeTypes.CAUSES)
        assertEquals(1, outbound.size)
        assertEquals("l1:$f2", outbound[0].targetId)
        assertEquals("2.5", outbound[0].properties["weight"])
    }

    // --- L2 Scenario Clustering ---

    @Test
    fun testClusterScenariosFormsCommunitiesAndComputesNormalizedCentroid() = runBlocking {
        // Form cluster 1: Network facts
        val f1 = pyramid.addFact("Host is reachable via TCP", embedding = generateUnitVector(64, 101L))
        val f2 = pyramid.addFact("Ping latency is under 5ms", embedding = generateUnitVector(64, 102L))
        pyramid.linkFacts(f1, f2, MemoryEdgeTypes.RELATES_TO)

        // Form cluster 2: Storage facts
        val f3 = pyramid.addFact("Disk I/O is normal", embedding = generateUnitVector(64, 201L))
        val f4 = pyramid.addFact("Free disk space is 80%", embedding = generateUnitVector(64, 202L))
        pyramid.linkFacts(f3, f4, MemoryEdgeTypes.RELATES_TO)

        val clusters = pyramid.clusterScenarios(iterations = 10, minClusterSize = 2)
        assertTrue(clusters.size >= 2)

        for (c in clusters) {
            assertTrue(c.memberFactIds.size >= 2)
            assertNotNull(c.centroidEmbedding)

            // Verify L2 normalization: ||c||_2 == 1.0
            var normSq = 0.0f
            for (v in c.centroidEmbedding!!) normSq += v * v
            assertEquals(1.0f, sqrt(normSq), 1e-4f)

            // Check Graph Node & Edges
            val clusterNode = pyramid.graph.getNode("l2:cluster_${c.clusterId}")
            assertNotNull(clusterNode)
            assertEquals(setOf(MemoryLabels.L2_SCENARIO), clusterNode?.labels)

            val memberEdges = pyramid.graph.getOutboundEdges("l2:cluster_${c.clusterId}", MemoryEdgeTypes.CLUSTERS_FACT)
            assertEquals(c.memberFactIds.size, memberEdges.size)
        }
    }

    @Test
    fun testClusterScenariosWithNoFactsReturnsEmpty() = runBlocking {
        val clusters = pyramid.clusterScenarios()
        assertTrue(clusters.isEmpty())
    }

    // --- L3 Persona & Invariants ---

    @Test
    fun testSetPersonaAndAddInvariantWithGovernance() = runBlocking {
        val pId = pyramid.setPersona(
            name = "KoreAssistant",
            traits = listOf("Rigorous", "Precise"),
            bio = "On-device cognitive database agent"
        )
        val personaNode = pyramid.graph.getNode(pId)
        assertNotNull(personaNode)
        assertEquals("KoreAssistant", personaNode?.properties?.get("name"))

        val f1 = pyramid.addFact("Fact under governance: User privacy verified")
        val invId = pyramid.addInvariant(
            statement = "Never log raw user passwords in clear text",
            priority = InvariantPriority.CRITICAL,
            governedNodeIds = listOf("l1:$f1")
        )

        val invNode = pyramid.graph.getNode(invId)
        assertNotNull(invNode)
        assertEquals("CRITICAL", invNode?.properties?.get("priority"))

        val governed = pyramid.graph.getOutboundEdges(invId, MemoryEdgeTypes.GOVERNS)
        assertEquals(1, governed.size)
        assertEquals("l1:$f1", governed[0].targetId)
    }

    // --- Hybrid Search & Evidence Enrichment ---

    @Test
    fun testSearchFactsReturnsFactsEnrichedWithDecompressedEvidence() = runBlocking {
        val ev1 = pyramid.recordEvidence("Sensor temperature readout is 42 degrees Celsius.")
        val vec1 = generateUnitVector(64, 501L)

        val factId = pyramid.addFact(
            text = "Sensor temperature is slightly elevated",
            evidenceIds = listOf(ev1),
            embedding = vec1
        )

        val hits = pyramid.searchFacts(
            query = "temperature elevated",
            queryVector = vec1,
            limit = 5
        )

        assertTrue(hits.isNotEmpty())
        val topFact = hits.first { it.factId == factId }
        assertEquals("Sensor temperature is slightly elevated", topFact.text)
        assertEquals(1, topFact.evidenceList.size)
        assertEquals("Sensor temperature readout is 42 degrees Celsius.", topFact.evidenceList[0].rawContent)
    }

    // --- Multi-Hop Drill-Down ---

    @Test
    fun testDrillDownNavigatesFromL3ToL0() = runBlocking {
        val evId = pyramid.recordEvidence("Raw chat: user requested dark theme mode.")
        val factId = pyramid.addFact("User prefers dark theme", evidenceIds = listOf(evId))
        val clusters = pyramid.clusterScenarios()
        val clusterId = clusters.firstOrNull()?.clusterId ?: "0"

        val invId = pyramid.addInvariant(
            statement = "Theme preferences must persist",
            governedNodeIds = listOf("l2:cluster_$clusterId")
        )

        val result = pyramid.drillDown(invId)
        assertEquals(invId, result.rootNodeId)
        assertEquals(MemoryLevel.L3_INVARIANT, result.rootLevel)
        assertTrue(result.atomicFacts.isNotEmpty())
        assertTrue(result.totalEvidenceCount >= 1)

        val contextStr = result.toContextString()
        assertTrue(contextStr.contains("DrillDown Trace"))
        assertTrue(contextStr.contains("User prefers dark theme"))
    }
}
