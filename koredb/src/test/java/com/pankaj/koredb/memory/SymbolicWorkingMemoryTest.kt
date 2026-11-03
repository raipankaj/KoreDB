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
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.GraphExport
import com.pankaj.koredb.graph.Node
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Unit Test Suite for Symbolic Working Memory:
 * 1. [GraphExport.toMermaid]: Syntax sanitization, label escaping, layer subgraphs, and layout directions.
 * 2. [com.pankaj.koredb.bridge.GraphVectorBridge.drillDown]: Hierarchical drill-down from L3/L2/L1 down to L0 raw evidence.
 */
class SymbolicWorkingMemoryTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_unit_symbolic_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    // --- GraphExport.toMermaid ---

    @Test
    fun testToMermaidEmptyGraphReturnsCleanComment() {
        val storage = db.graph()
        val mermaid = GraphExport.toMermaid(storage, emptyList(), direction = "TD")
        assertTrue(mermaid.contains("graph TD"))
        assertTrue(mermaid.contains("%% empty"))
    }

    @Test
    fun testToMermaidNodeIdSanitization() = runBlocking {
        val storage = db.graph()
        storage.putNode(Node("l1:user-profile:alpha 42", properties = mapOf("text" to "Alpha profile")))

        val mermaid = GraphExport.toMermaid(storage, listOf("l1:user-profile:alpha 42"), groupBySubgraphs = false)
        assertTrue(mermaid.contains("l1_user_profile_alpha_42"))
        assertFalse(mermaid.contains("l1:user-profile:alpha 42["))
    }

    @Test
    fun testToMermaidCharacterEscapingAndTruncation() = runBlocking {
        val storage = db.graph()
        val longWithSpecial = "<b>KoreDB</b> provides \"high speed\" data storage and indexing."
        storage.putNode(Node("n1", properties = mapOf("text" to longWithSpecial)))

        val mermaid = GraphExport.toMermaid(storage, listOf("n1"), maxLabelLength = 25, groupBySubgraphs = false)
        assertTrue(mermaid.contains("#lt;b#gt;KoreDB"))
        assertTrue(mermaid.contains("..."))
        assertFalse(mermaid.contains("\"high speed\""))
    }

    @Test
    fun testToMermaidLayerSubgraphsGrouping() = runBlocking {
        val storage = db.graph()
        storage.putNode(Node("l3:persona_1", labels = setOf(MemoryLabels.L3_PERSONA), properties = mapOf("name" to "PersonaA")))
        storage.putNode(Node("l3:inv_1", labels = setOf(MemoryLabels.L3_INVARIANT), properties = mapOf("statement" to "RuleA")))
        storage.putNode(Node("l2:cluster_1", labels = setOf(MemoryLabels.L2_SCENARIO), properties = mapOf("title" to "ScenarioA")))
        storage.putNode(Node("l1:fact_1", labels = setOf(MemoryLabels.L1_FACT), properties = mapOf("text" to "FactA")))
        storage.putNode(Node("l0:ev_1", labels = setOf(MemoryLabels.L0_EVIDENCE), properties = mapOf("summary" to "EvidenceA")))

        val mermaid = GraphExport.toMermaid(
            storage = storage,
            nodeIds = listOf("l3:persona_1", "l3:inv_1", "l2:cluster_1", "l1:fact_1", "l0:ev_1"),
            groupBySubgraphs = true
        )

        assertTrue(mermaid.contains("subgraph L3_Persona"))
        assertTrue(mermaid.contains("subgraph L3_Invariant"))
        assertTrue(mermaid.contains("subgraph L2_Scenario"))
        assertTrue(mermaid.contains("subgraph L1_Fact"))
        assertTrue(mermaid.contains("subgraph L0_Evidence"))
    }

    @Test
    fun testToMermaidEdgesWithWeightsAndFilters() = runBlocking {
        val storage = db.graph()
        storage.putNode(Node("a"))
        storage.putNode(Node("b"))
        storage.putNode(Node("c"))

        storage.putEdge(Edge("a", "b", "RELATES_TO", properties = mapOf("weight" to "1.5")))
        storage.putEdge(Edge("b", "c", "CAUSES"))

        val allEdges = GraphExport.toMermaid(storage, listOf("a", "b", "c"))
        assertTrue(allEdges.contains("a -->|RELATES_TO (1.5)| b"))
        assertTrue(allEdges.contains("b -->|CAUSES| c"))

        val filteredEdges = GraphExport.toMermaid(storage, listOf("a", "b", "c"), edgeType = "RELATES_TO")
        assertTrue(filteredEdges.contains("a -->|RELATES_TO (1.5)| b"))
        assertFalse(filteredEdges.contains("CAUSES"))
    }

    @Test
    fun testToMermaidDirectionOptions() {
        val storage = db.graph()
        val lr = GraphExport.toMermaid(storage, emptyList(), direction = "LR")
        assertTrue(lr.startsWith("graph LR"))
    }

    // --- GraphVectorBridge.drillDown ---

    @Test
    fun testDrillDownFromL2ClusterDownToDecompressedL0Evidence() = runBlocking {
        val pyramid = db.memoryPyramid()
        val evId = pyramid.recordEvidence("User transcript: please configure biometric authentication.")
        val factId = pyramid.addFact("Biometric authentication enabled", evidenceIds = listOf(evId))
        pyramid.clusterScenarios()

        val result = pyramid.drillDown("l2:cluster_0")
        assertEquals("l2:cluster_0", result.rootNodeId)
        assertEquals(MemoryLevel.L2_SCENARIO, result.rootLevel)
        assertEquals(1, result.scenarioClusters.size)
        assertTrue(result.atomicFacts.isNotEmpty())
        assertEquals(1, result.totalEvidenceCount)

        val fact = result.atomicFacts.first()
        assertEquals(factId, fact.factId)
        assertEquals("Biometric authentication enabled", fact.text)
        assertEquals(1, fact.evidenceList.size)
        assertEquals("User transcript: please configure biometric authentication.", fact.evidenceList[0].rawContent)
    }

    @Test
    fun testDrillDownFromL1FactDirect() = runBlocking {
        val pyramid = db.memoryPyramid()
        val ev1 = pyramid.recordEvidence("First piece of raw context.")
        val ev2 = pyramid.recordEvidence("Second piece of raw context.")
        val factId = pyramid.addFact("Combined insight", evidenceIds = listOf(ev1, ev2))

        val result = pyramid.drillDown("l1:$factId")
        assertEquals("l1:$factId", result.rootNodeId)
        assertEquals(MemoryLevel.L1_FACT, result.rootLevel)
        assertEquals(1, result.atomicFacts.size)
        assertEquals(2, result.totalEvidenceCount)
    }

    @Test
    fun testDrillDownRespectsMaxEvidencePerFact() = runBlocking {
        val pyramid = db.memoryPyramid()
        val evList = (1..10).map { pyramid.recordEvidence("Evidence payload number $it") }
        val factId = pyramid.addFact("Fact with high evidence density", evidenceIds = evList)

        val result = pyramid.drillDown("l1:$factId", maxEvidencePerFact = 3)
        assertEquals(1, result.atomicFacts.size)
        assertEquals(3, result.atomicFacts[0].evidenceList.size)
        assertEquals(3, result.totalEvidenceCount)
    }

    @Test
    fun testDrillDownOnNonExistentNodeReturnsUnknownLevel() = runBlocking {
        val pyramid = db.memoryPyramid()
        val result = pyramid.drillDown("ghost_identifier")
        assertEquals(MemoryLevel.UNKNOWN, result.rootLevel)
        assertTrue(result.scenarioClusters.isEmpty())
        assertTrue(result.atomicFacts.isEmpty())
        assertEquals(0, result.totalEvidenceCount)
    }
}
