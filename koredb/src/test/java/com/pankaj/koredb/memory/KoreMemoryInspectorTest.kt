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
import java.util.UUID

/**
 * Unit Test Suite for [KoreMemoryInspector]:
 * - Markdown Telemetry Mirror Rendering
 * - Layer Overview and Executive Summary Statistics
 * - Active Invariant Tables & Invariant Boundaries
 * - L2 Scenario Cluster Summaries & Tables
 * - Bi-Temporal Fact Statistics (Active vs. Historical)
 * - L0 Compression Metrics & Storage Samples
 * - Live Mermaid Working Memory Graph Embedding
 */
class KoreMemoryInspectorTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase
    private lateinit var pyramid: KoreMemoryPyramid
    private lateinit var inspector: KoreMemoryInspector

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_unit_inspector_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
        pyramid = db.memoryPyramid()
        inspector = db.memoryInspector(pyramid)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun testGenerateMarkdownReportOnEmptyPyramid() {
        val report = inspector.generateMarkdownReport()
        assertTrue(report.startsWith("# KoreDB Cognitive Memory Mirror"))
        assertTrue(report.contains("## Layer Overview"))
        assertTrue(report.contains("Total Nodes: 0"))
        assertTrue(report.contains("| L3 Persona | 0 |"))
        assertTrue(report.contains("| L2 Scenarios | 0 |"))
        assertTrue(report.contains("| L1 Facts | 0 |"))
        assertTrue(report.contains("| L0 Evidence | 0 |"))
        assertTrue(report.contains("*No persona or invariants configured.*"))
        assertTrue(report.contains("No scenario clusters detected yet"))
        assertTrue(report.contains("*No atomic facts recorded.*"))
        assertTrue(report.contains("*No raw evidence recorded.*"))
        assertTrue(report.contains("```mermaid"))
        assertTrue(report.contains("%% empty"))
    }

    @Test
    fun testGenerateMarkdownReportWithPopulatedHierarchy() = runBlocking {
        // L0
        val ev1 = pyramid.recordEvidence("User query log: How do I tune HNSW MRL parameters?", mimeType = "application/json")
        val ev2 = pyramid.recordEvidence("Documentation: MRL uses 64-dimensional prefix short circuiting.", mimeType = "text/markdown")

        // L1
        val f1 = pyramid.addFact("MRL short circuits upper HNSW layers", evidenceIds = listOf(ev1))
        val f2 = pyramid.addFact("Vector search evaluates 64 dimensions first", evidenceIds = listOf(ev2))
        pyramid.linkFacts(f1, f2, MemoryEdgeTypes.RELATES_TO)

        // Invalidate an older fact to test bi-temporal historical facts
        val invalidatedFact = pyramid.addFact(
            text = "Legacy brute force vector scan was used",
            properties = mapOf("validUntilMs" to "5000")
        )

        // L2
        pyramid.clusterScenarios()

        // L3
        pyramid.setPersona(
            name = "KoreAI",
            traits = listOf("Fast", "Rigorous"),
            bio = "On-device cognitive memory system"
        )
        pyramid.addInvariant(
            statement = "Ensure vector recall never drops below 95%",
            priority = InvariantPriority.CRITICAL
        )

        val report = inspector.generateMarkdownReport()

        // Headers
        assertTrue(report.contains("# KoreDB Cognitive Memory Mirror"))
        assertTrue(report.contains("## Layer Overview"))
        assertTrue(report.contains("## 1. Executive Summary & Pyramid Statistics"))
        assertTrue(report.contains("## 2. L3 Persona & Invariants"))
        assertTrue(report.contains("## 3. L2 Scenario Clusters"))
        assertTrue(report.contains("## Bi-Temporal Memory State"))
        assertTrue(report.contains("## 4. L1 Atomic Facts Knowledge Base"))
        assertTrue(report.contains("## 5. L0 Raw Evidence Storage"))
        assertTrue(report.contains("## Active Working Memory Subgraph"))

        // Contents
        assertTrue(report.contains("KoreAI"))
        assertTrue(report.contains("Ensure vector recall never drops below 95%"))
        assertTrue(report.contains("| CRITICAL |") || report.contains("[CRITICAL]"))
        assertTrue(report.contains("Active Facts: 2"))
        assertTrue(report.contains("Historical Facts: 1"))
        assertTrue(report.contains("MRL short circuits upper HNSW layers"))
        assertTrue(report.contains("Total Evidence Nodes: 2"))
        assertTrue(report.contains("application/json"))
        assertTrue(report.contains("```mermaid"))
        assertTrue(report.contains("subgraph L3_Persona"))
    }

    @Test
    fun testGenerateMarkdownReportExcludesMermaidWhenDisabled() {
        val report = inspector.generateMarkdownReport(includeMermaid = false)
        assertFalse(report.contains("```mermaid"))
        assertFalse(report.contains("## Active Working Memory Subgraph"))
    }

    @Test
    fun testGenerateMarkdownReportLimitsFactAndEvidenceRows() = runBlocking {
        for (i in 1..10) {
            val ev = pyramid.recordEvidence("Raw log entry number $i")
            pyramid.addFact("Fact proposition number $i", evidenceIds = listOf(ev))
        }

        val report = inspector.generateMarkdownReport(maxFacts = 3, maxEvidenceSamples = 2)
        assertTrue(report.contains("(Sample 3/10)"))
    }
}
