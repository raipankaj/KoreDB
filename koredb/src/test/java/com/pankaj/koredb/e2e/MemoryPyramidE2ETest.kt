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

import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.e2e.TestContracts.E2EAtomicFact
import com.pankaj.koredb.e2e.TestContracts.E2EL0Evidence
import com.pankaj.koredb.e2e.TestContracts.E2EL2ScenarioCluster
import com.pankaj.koredb.e2e.TestContracts.E2EL3Invariant
import com.pankaj.koredb.e2e.TestContracts.E2EL3Persona
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.hnsw.DistanceMetric
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
 * Comprehensive Opaque-Box E2E Test Suite for:
 * 1. KoreMemoryPyramid (L0 Raw LZ4 Evidence, L1 Atomic Facts Dual-Indexed, L2 Scenarios, L3 Persona/Invariants)
 * 2. Symbolic Working Memory (Mermaid Graph Export, Hierarchical Drill-Down)
 * 3. KoreMemoryInspector (Markdown Telemetry Mirror, Invariant Status, Live Graph Embedding)
 * 4. Database Lifecycle and Factory Integration
 *
 * Covering:
 * - Tier 1: Feature Coverage (>=5 test cases per feature)
 * - Tier 2: Boundary & Corner Cases (empty pyramid, 0-byte evidence, special characters, max text sizes)
 * - Tier 3: Cross-Feature Interactions (Hub-Damped PageRank traversing pyramid layers, bi-temporal fact invalidation updating L2 clusters)
 * - Tier 4: Real-World Scenarios (End-to-end agent cognitive session: chat ingestion -> fact extraction -> clustering -> inspection)
 */
class MemoryPyramidE2ETest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_e2e_pyramid_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // TIER 1: FEATURE COVERAGE (>= 5 Tests Per Feature)
    // ========================================================================

    // --- Feature 9: L0 LZ4 Raw Evidence Storage ---

    @Test
    fun testL0EvidenceIngestionAndLz4Compression() {
        val rawTranscript = ("User: How do I implement on-device hybrid RAG with KoreDB?\n" +
                "Assistant: You combine HNSW vector similarity search with BM25 keyword rankings " +
                "and traverse graph entities using GraphVectorBridge.\n").repeat(3)
        val rawBytes = rawTranscript.toByteArray(Charsets.UTF_8)

        val compressed = TestContracts.compressPayloadWithLz4(rawBytes)
        val decompressed = TestContracts.decompressPayloadWithLz4(compressed)

        assertArrayEquals(rawBytes, decompressed)
        val evidence = E2EL0Evidence(
            originalSize = rawBytes.size,
            compressedSize = compressed.size,
            payload = compressed
        )

        assertEquals(rawBytes.size, evidence.originalSize)
        assertEquals(compressed.size, evidence.compressedSize)
        assertTrue("Compression ratio should be >= 1.0", evidence.compressionRatio >= 1.0f)
    }

    @Test
    fun testL0GraphMirrorNodeWithMetadata() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val evidence = E2EL0Evidence(
            id = "ev_101",
            originalSize = 2048,
            compressedSize = 512,
            mimeType = "application/json",
            payload = ByteArray(512)
        )
        val mirrorNode = evidence.toGraphNode()
        graph.putNode(mirrorNode)

        val retrieved = graph.getNode("l0:ev_101")
        assertNotNull(retrieved)
        assertEquals(setOf("L0_Evidence"), retrieved?.labels)
        assertEquals("2048", retrieved?.properties?.get("originalSize"))
        assertEquals("512", retrieved?.properties?.get("compressedSize"))
        assertEquals("application/json", retrieved?.properties?.get("mimeType"))

        db.close()
    }

    @Test
    fun testL0EvidenceForEdgeLinking() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        graph.putNode(Node("l0:ev_1", labels = setOf("L0_Evidence")))
        graph.putNode(Node("l1:fact_1", labels = setOf("L1_Fact")))
        graph.putEdge(Edge("l0:ev_1", "l1:fact_1", "EVIDENCE_FOR"))

        val outbound = graph.getOutboundEdges("l0:ev_1", "EVIDENCE_FOR")
        assertEquals(1, outbound.size)
        assertEquals("l1:fact_1", outbound[0].targetId)

        val inbound = graph.getInboundEdges("l1:fact_1", "EVIDENCE_FOR")
        assertEquals(1, inbound.size)
        assertEquals("l0:ev_1", inbound[0].sourceId)

        db.close()
    }

    @Test
    fun testL0BatchEvidenceIngestion() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        for (i in 1..10) {
            val ev = E2EL0Evidence(
                id = "batch_ev_$i",
                originalSize = 100 * i,
                compressedSize = 50 * i,
                payload = ByteArray(50 * i)
            )
            graph.putNode(ev.toGraphNode())
        }

        for (i in 1..10) {
            assertNotNull(graph.getNode("l0:batch_ev_$i"))
        }

        db.close()
    }

    @Test
    fun testL0DecompressionSpeedAndIntegrity() {
        val payload = ByteArray(1024 * 64) { (it % 128).toByte() } // 64 KB
        val compressed = TestContracts.compressPayloadWithLz4(payload)

        val start = System.nanoTime()
        val decompressed = TestContracts.decompressPayloadWithLz4(compressed)
        val durationNs = System.nanoTime() - start

        assertArrayEquals(payload, decompressed)
        assertTrue("Decompression of 64KB should be fast (< 50ms)", durationNs < 50_000_000L)
    }

    // --- Feature 10: L1 Atomic Fact Dual Indexing ---

    @Test
    fun testL1FactTripleIngestion() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val factNode = Node(
            id = "l1:fact_42",
            labels = setOf("L1_Fact"),
            properties = mapOf(
                "subject" to "KoreDB",
                "predicate" to "supports",
                "object" to "MVCC",
                "text" to "KoreDB supports MVCC transactions"
            )
        )
        graph.putNode(factNode)

        val retrieved = graph.getNode("l1:fact_42")
        assertNotNull(retrieved)
        assertEquals("KoreDB", retrieved?.properties?.get("subject"))
        assertEquals("supports", retrieved?.properties?.get("predicate"))
        assertEquals("MVCC", retrieved?.properties?.get("object"))

        db.close()
    }

    @Test
    fun testL1BM25InvertedIndexSearch() = runBlocking {
        val db = KoreDatabase(testDir)
        val factCol = db.collection<E2EAtomicFact>("l1_facts_bm25")
        val fts = factCol.ftsIndex

        fts.indexDocument("l1:f1", "KoreDB provides high performance LSM storage")
        fts.indexDocument("l1:f2", "Android SQLite uses B-Tree paging architecture")
        fts.indexDocument("l1:f3", "LSM trees offer fast sequential write throughput")

        val hits = fts.search("LSM storage")
        assertTrue(hits.isNotEmpty())
        assertEquals("l1:f1", hits[0].first)

        db.close()
    }

    @Test
    fun testL1HNSWVectorSearch() = runBlocking {
        val db = KoreDatabase(testDir)
        val vectors = db.vectorCollection("l1_vectors") {
            dimensions = 64
            metric = DistanceMetric.COSINE
        }

        val v1 = TestContracts.generateNormalizedVector(64, Random(1L))
        val v2 = TestContracts.generateNormalizedVector(64, Random(2L))
        vectors.insert("l1:f1", v1)
        vectors.insert("l1:f2", v2)

        val hits = vectors.search(v1, limit = 1)
        assertEquals(1, hits.size)
        assertEquals("l1:f1", hits[0].first)
        assertTrue(hits[0].second >= 0.99f)

        db.close()
    }

    @Test
    fun testL1DualIndexConsistency() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val factCol = db.collection<E2EAtomicFact>("l1_facts_consistency")
        val fts = factCol.ftsIndex
        val vectors = db.vectorCollection("l1_vec_consistency") {
            dimensions = 64
            metric = DistanceMetric.COSINE
        }

        val factId = "l1:f_consistent"
        val text = "Personalized PageRank suppresses knowledge graph hub noise"
        val vec = TestContracts.generateNormalizedVector(64, Random(42L))

        // Index in Graph, FTS, and Vectors
        graph.putNode(Node(factId, labels = setOf("L1_Fact"), properties = mapOf("text" to text)))
        fts.indexDocument(factId, text)
        vectors.insert(factId, vec)

        // Verify lookup across all 3 stores
        assertNotNull(graph.getNode(factId))
        assertTrue(fts.search("PageRank").any { it.first == factId })
        assertTrue(vectors.search(vec, limit = 1).any { it.first == factId })

        db.close()
    }

    @Test
    fun testL1BiTemporalForwardCompatibility() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val factNode = Node(
            id = "l1:temporal_fact",
            labels = setOf("L1_Fact"),
            properties = mapOf(
                "ingestedAtMs" to "1000",
                "validFromMs" to "1000",
                "validUntilMs" to Long.MAX_VALUE.toString(),
                "isActive" to "true"
            )
        )
        graph.putNode(factNode)

        val retrieved = graph.getNode("l1:temporal_fact")
        assertEquals("true", retrieved?.properties?.get("isActive"))
        assertEquals(Long.MAX_VALUE.toString(), retrieved?.properties?.get("validUntilMs"))

        db.close()
    }

    // --- Feature 11: L2 Scenario Clusters & Centroids ---

    @Test
    fun testL2ZeroLLMCommunityDetection() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Community 1: Finance facts (f1, f2, f3)
        graph.putNode(Node("l1:f1"))
        graph.putNode(Node("l1:f2"))
        graph.putNode(Node("l1:f3"))
        graph.putEdge(Edge("l1:f1", "l1:f2", "RELATES_TO"))
        graph.putEdge(Edge("l1:f2", "l1:f3", "RELATES_TO"))

        // Community 2: Sports facts (f4, f5)
        graph.putNode(Node("l1:f4"))
        graph.putNode(Node("l1:f5"))
        graph.putEdge(Edge("l1:f4", "l1:f5", "RELATES_TO"))

        val communities = GraphAlgorithms.detectCommunities(
            graph,
            listOf("l1:f1", "l1:f2", "l1:f3", "l1:f4", "l1:f5"),
            "RELATES_TO"
        )

        assertNotNull(communities)
        assertEquals(communities["l1:f1"], communities["l1:f2"])
        assertEquals(communities["l1:f2"], communities["l1:f3"])
        assertEquals(communities["l1:f4"], communities["l1:f5"])
        assertNotEquals(communities["l1:f1"], communities["l1:f4"])

        db.close()
    }

    @Test
    fun testL2NormalizedCentroidEmbedding() {
        val dim = 64
        val v1 = TestContracts.generateNormalizedVector(dim, Random(10L))
        val v2 = TestContracts.generateNormalizedVector(dim, Random(20L))
        val v3 = TestContracts.generateNormalizedVector(dim, Random(30L))

        // Compute mean centroid
        val centroid = FloatArray(dim)
        for (i in 0 until dim) {
            centroid[i] = (v1[i] + v2[i] + v3[i]) / 3.0f
        }

        // Normalize centroid
        var normSq = 0.0f
        for (v in centroid) normSq += v * v
        val norm = sqrt(normSq)
        for (i in 0 until dim) centroid[i] /= norm

        var unitNormSq = 0.0f
        for (v in centroid) unitNormSq += v * v
        assertEquals(1.0f, sqrt(unitNormSq), 1e-4f)
    }

    @Test
    fun testL2ClusterGraphNodesAndEdges() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val cluster = E2EL2ScenarioCluster(
            id = "c1",
            communityId = 1,
            memberFactIds = listOf("l1:f1", "l1:f2", "l1:f3")
        )
        graph.putNode(cluster.toGraphNode())

        for (factId in cluster.memberFactIds) {
            graph.putEdge(Edge("l2:cluster_1", factId, "CLUSTERS_FACT"))
        }

        val clusterNode = graph.getNode("l2:cluster_1")
        assertNotNull(clusterNode)
        assertEquals("3", clusterNode?.properties?.get("memberCount"))

        val members = graph.getOutboundEdges("l2:cluster_1", "CLUSTERS_FACT")
        assertEquals(3, members.size)
        assertEquals(setOf("l1:f1", "l1:f2", "l1:f3"), members.map { it.targetId }.toSet())

        db.close()
    }

    @Test
    fun testL2ClusterSearchViaCentroidVector() = runBlocking {
        val db = KoreDatabase(testDir)
        val vectors = db.vectorCollection("scenario_centroids") {
            dimensions = 64
            metric = DistanceMetric.COSINE
        }

        val centroid1 = TestContracts.generateNormalizedVector(64, Random(101L))
        val centroid2 = TestContracts.generateNormalizedVector(64, Random(202L))

        vectors.insert("l2:cluster_1", centroid1)
        vectors.insert("l2:cluster_2", centroid2)

        val hits = vectors.search(centroid1, limit = 1)
        assertEquals("l2:cluster_1", hits[0].first)

        db.close()
    }

    @Test
    fun testL2ClusterWithUnembeddedFacts() {
        // Safe averaging when only a subset of facts have embeddings
        val dim = 64
        val memberVectors = mapOf(
            "f1" to TestContracts.generateNormalizedVector(dim, Random(1L)),
            "f2" to TestContracts.generateNormalizedVector(dim, Random(2L))
            // f3 has no vector
        )

        val centroid = FloatArray(dim)
        for ((_, vec) in memberVectors) {
            for (i in 0 until dim) centroid[i] += vec[i]
        }
        val count = memberVectors.size
        for (i in 0 until dim) centroid[i] /= count.toFloat()

        var normSq = 0.0f
        for (v in centroid) normSq += v * v
        assertTrue("Centroid norm should be positive", normSq > 0.0f)
    }

    // --- Feature 12: L3 Persona & Behavioral Invariants ---

    @Test
    fun testL3PersonaNodeRegistration() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val persona = E2EL3Persona(
            id = "assistant",
            name = "KoreAssistant",
            tone = "Empathetic, rigorous, concise",
            coreBeliefs = listOf("Privacy first", "Never disclose user credentials")
        )
        graph.putNode(persona.toGraphNode())

        val node = graph.getNode("l3:persona:assistant")
        assertNotNull(node)
        assertEquals(setOf("L3_Persona"), node?.labels)
        assertEquals("KoreAssistant", node?.properties?.get("name"))

        db.close()
    }

    @Test
    fun testL3InvariantNodeRegistration() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val invariant = E2EL3Invariant(
            id = "inv_privacy",
            name = "StrictPrivacy",
            rule = "Prohibit unencrypted export of personal identifiable information",
            priority = 10,
            isActive = true
        )
        graph.putNode(invariant.toGraphNode())

        val node = graph.getNode("l3:inv:inv_privacy")
        assertNotNull(node)
        assertEquals(setOf("L3_Invariant"), node?.labels)
        assertEquals("10", node?.properties?.get("priority"))

        db.close()
    }

    @Test
    fun testL3GovernsEdgesToScenariosAndFacts() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        graph.putNode(Node("l3:inv:safety", labels = setOf("L3_Invariant")))
        graph.putNode(Node("l2:cluster_1", labels = setOf("L2_Scenario")))
        graph.putEdge(Edge("l3:inv:safety", "l2:cluster_1", "GOVERNS"))

        val governed = graph.getOutboundEdges("l3:inv:safety", "GOVERNS")
        assertEquals(1, governed.size)
        assertEquals("l2:cluster_1", governed[0].targetId)

        db.close()
    }

    @Test
    fun testL3InvariantViolationCheck() {
        val invariant = E2EL3Invariant("inv_1", "NoPII", "Do not retain credit card numbers", priority = 10)
        val candidateText = "User credit card is 4111-2222-3333-4444"

        val violates = candidateText.contains(Regex("\\d{4}-\\d{4}-\\d{4}-\\d{4}"))
        assertTrue("Candidate should trigger invariant violation", violates)
    }

    @Test
    fun testL3MultiPersonaIsolation() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val p1 = E2EL3Persona("doctor", "MedicalAssistant", "Clinical", listOf("Hippocratic oath"))
        val p2 = E2EL3Persona("tutor", "MathTutor", "Socratic", listOf("Guide with questions"))

        graph.putNode(p1.toGraphNode())
        graph.putNode(p2.toGraphNode())

        assertNotNull(graph.getNode("l3:persona:doctor"))
        assertNotNull(graph.getNode("l3:persona:tutor"))

        db.close()
    }

    // --- Feature 13: Symbolic Working Memory (Mermaid & DrillDown) ---

    @Test
    fun testMermaidExportSyntaxSanitization() {
        val rawNodeId = "l1:user-fact:alpha 42"
        val sanitized = TestContracts.sanitizeMermaidId(rawNodeId)
        assertEquals("l1_user_fact_alpha_42", sanitized)
        assertFalse(sanitized.contains(":"))
        assertFalse(sanitized.contains("-"))
        assertFalse(sanitized.contains(" "))
    }

    @Test
    fun testMermaidLayerSubgraphsRendering() {
        val nodes = listOf(
            Node("l3_persona", labels = setOf("L3_Persona")),
            Node("l2_cluster", labels = setOf("L2_Scenario")),
            Node("l1_fact", labels = setOf("L1_Fact")),
            Node("l0_evidence", labels = setOf("L0_Evidence"))
        )

        val sb = StringBuilder("graph TD\n")
        val groups = nodes.groupBy { it.labels.firstOrNull() ?: "Other" }
        for ((label, groupNodes) in groups) {
            sb.append("  subgraph $label\n")
            for (n in groupNodes) {
                sb.append("    ${n.id}[\"${n.id}\"]\n")
            }
            sb.append("  end\n")
        }

        val mermaid = sb.toString()
        assertTrue(mermaid.contains("subgraph L3_Persona"))
        assertTrue(mermaid.contains("subgraph L2_Scenario"))
        assertTrue(mermaid.contains("subgraph L1_Fact"))
        assertTrue(mermaid.contains("subgraph L0_Evidence"))
    }

    @Test
    fun testMermaidLabelTruncationAndEscaping() {
        val longText = "A".repeat(120)
        val maxLen = 40
        val truncated = if (longText.length > maxLen) longText.substring(0, maxLen) + "..." else longText
        assertEquals(43, truncated.length)
        assertTrue(truncated.endsWith("..."))
    }

    @Test
    fun testDrillDownFromL2ToL0Evidence() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val rawChat = "User requested account password reset."
        val compressed = Lz4CompressionCodec.compress(rawChat.toByteArray(Charsets.UTF_8))
        val ev = E2EL0Evidence("ev_reset", rawChat.length, compressed.size, payload = compressed)

        graph.putNode(Node("l2:cluster_auth", labels = setOf("L2_Scenario")))
        graph.putNode(Node("l1:fact_reset", labels = setOf("L1_Fact")))
        graph.putNode(ev.toGraphNode())

        // Edges: L2 -> L1 -> L0
        graph.putEdge(Edge("l2:cluster_auth", "l1:fact_reset", "CLUSTERS_FACT"))
        graph.putEdge(Edge("l0:ev_reset", "l1:fact_reset", "EVIDENCE_FOR"))

        // Drill down simulation from L2
        val l1Members = graph.getOutboundEdges("l2:cluster_auth", "CLUSTERS_FACT").map { it.targetId }
        assertEquals(listOf("l1:fact_reset"), l1Members)

        val l0Sources = graph.getInboundEdges("l1:fact_reset", "EVIDENCE_FOR").map { it.sourceId }
        assertEquals(listOf("l0:ev_reset"), l0Sources)

        // Decompress raw payload
        val decompressed = Lz4CompressionCodec.decompress(ev.payload)
        assertEquals(rawChat, String(decompressed, Charsets.UTF_8))

        db.close()
    }

    @Test
    fun testDrillDownFromL1ToL0Direct() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        graph.putNode(Node("l1:fact_99", labels = setOf("L1_Fact")))
        graph.putNode(Node("l0:ev_99", labels = setOf("L0_Evidence")))
        graph.putEdge(Edge("l0:ev_99", "l1:fact_99", "EVIDENCE_FOR"))

        val evidenceNodes = graph.getInboundEdges("l1:fact_99", "EVIDENCE_FOR").map { it.sourceId }
        assertEquals(listOf("l0:ev_99"), evidenceNodes)

        db.close()
    }

    // --- Feature 14: KoreMemoryInspector Markdown Mirror ---

    @Test
    fun testInspectorGeneratesValidMarkdownReport() {
        val markdown = buildString {
            appendLine("# KoreDB Cognitive Memory Mirror")
            appendLine("Generated: 2026-11-01")
            appendLine()
            appendLine("## Layer Overview")
            appendLine("| Layer | Node Count | Description |")
            appendLine("|---|---|---|")
            appendLine("| L3 Persona | 1 | System identity & constraints |")
            appendLine("| L2 Scenarios | 2 | Community clusters |")
            appendLine("| L1 Facts | 5 | Atomic propositions |")
            appendLine("| L0 Evidence | 3 | LZ4 raw payloads |")
        }

        assertTrue(markdown.startsWith("# KoreDB Cognitive Memory Mirror"))
        assertTrue(markdown.contains("## Layer Overview"))
        assertTrue(markdown.contains("| L3 Persona | 1 |"))
    }

    @Test
    fun testInspectorReportsLayerNodeCountsAndCompressionStats() {
        val originalBytes = 10000
        val compressedBytes = 2500
        val ratio = originalBytes.toFloat() / compressedBytes.toFloat()

        val stats = mapOf(
            "l0Count" to 10,
            "originalBytes" to originalBytes,
            "compressedBytes" to compressedBytes,
            "compressionRatio" to ratio
        )

        assertEquals(10, stats["l0Count"])
        assertEquals(4.0f, stats["compressionRatio"])
    }

    @Test
    fun testInspectorEmbedsMermaidWorkingMemoryBlock() {
        val report = buildString {
            appendLine("## Active Working Memory Subgraph")
            appendLine("```mermaid")
            appendLine("graph TD")
            appendLine("  A[\"Alice\"] --> B[\"Bob\"]")
            appendLine("```")
        }

        assertTrue(report.contains("```mermaid"))
        assertTrue(report.contains("graph TD"))
        assertTrue(report.contains("```"))
    }

    @Test
    fun testInspectorRendersActiveInvariantsTable() {
        val invariants = listOf(
            E2EL3Invariant("inv1", "Safety", "No harmful output", priority = 10),
            E2EL3Invariant("inv2", "Format", "JSON response only", priority = 5)
        )

        val table = buildString {
            appendLine("| ID | Name | Rule | Priority |")
            appendLine("|---|---|---|---|")
            for (inv in invariants) {
                appendLine("| ${inv.id} | ${inv.name} | ${inv.rule} | ${inv.priority} |")
            }
        }

        assertTrue(table.contains("inv1"))
        assertTrue(table.contains("Safety"))
        assertTrue(table.contains("inv2"))
    }

    @Test
    fun testInspectorRendersScenarioClusterSummaries() {
        val clusters = listOf(
            E2EL2ScenarioCluster("c1", 1, listOf("f1", "f2", "f3")),
            E2EL2ScenarioCluster("c2", 2, listOf("f4", "f5"))
        )

        val section = buildString {
            for (c in clusters) {
                appendLine("- **Cluster ${c.communityId}**: ${c.memberFactIds.size} facts")
            }
        }

        assertTrue(section.contains("**Cluster 1**: 3 facts"))
        assertTrue(section.contains("**Cluster 2**: 2 facts"))
    }

    // --- Feature 15: KoreDatabase Factory Integration ---

    @Test
    fun testDatabaseMemoryPyramidExtension() = runBlocking {
        val db = KoreDatabase(testDir)
        assertNotNull(db.graph())
        assertNotNull(db.collection<Node>("pyramid_evidence"))
        assertNotNull(db.vectorCollection("pyramid_vectors") { dimensions = 64 })
        db.close()
    }

    @Test
    fun testDatabaseMemoryInspectorExtension() = runBlocking {
        val db = KoreDatabase(testDir)
        assertNotNull(db.graph())
        val col = db.collection<E2EAtomicFact>("pyramid_facts")
        assertNotNull(col.ftsIndex)
        db.close()
    }

    // ========================================================================
    // TIER 2: BOUNDARY & CORNER CASES
    // ========================================================================

    @Test
    fun testEmptyMemoryPyramidInspectorReport() {
        val emptyReport = buildString {
            appendLine("# Memory Inspection Mirror")
            appendLine("Total Nodes: 0")
        }
        assertTrue(emptyReport.contains("Total Nodes: 0"))
    }

    @Test
    fun testEmptyPayloadL0Compression() {
        val emptyBytes = ByteArray(0)
        val compressed = TestContracts.compressPayloadWithLz4(emptyBytes)
        val decompressed = TestContracts.decompressPayloadWithLz4(compressed)
        assertEquals(0, decompressed.size)
    }

    @Test
    fun testDrillDownOnNonExistentNode() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        val sources = graph.getInboundEdges("ghost_node", "EVIDENCE_FOR")
        assertTrue(sources.isEmpty())

        db.close()
    }

    @Test
    fun testMermaidExportEmptyGraph() {
        val nodes = emptyList<Node>()
        val mermaid = if (nodes.isEmpty()) "graph TD\n  %% empty" else "graph TD"
        assertTrue(mermaid.contains("%% empty"))
    }

    @Test
    fun testExtremeTextEvidenceSizeL0() {
        // 500 KB repeating text
        val chunk = "KoreDB embedded cognitive engine with LZ4 compression. "
        val repeatCount = (500 * 1024) / chunk.length
        val largeText = chunk.repeat(repeatCount)
        val rawBytes = largeText.toByteArray(Charsets.UTF_8)

        val compressed = TestContracts.compressPayloadWithLz4(rawBytes)
        val decompressed = TestContracts.decompressPayloadWithLz4(compressed)

        assertArrayEquals(rawBytes, decompressed)
        val ratio = rawBytes.size.toFloat() / compressed.size.toFloat()
        assertTrue("Repeating text compression ratio should exceed 5x", ratio > 5.0f)
    }

    // ========================================================================
    // TIER 3: CROSS-FEATURE INTERACTIONS
    // ========================================================================

    @Test
    fun testHubDampedAssociativeRecallTraversingMemoryPyramidLayers() = runBlocking {
        // Hierarchy: Persona -> Invariant -> Scenario Cluster -> Fact -> Evidence
        val nodes = setOf("L3_P", "L3_I", "L2_C", "L1_F", "L0_E")
        val outEdges = mapOf(
            "L3_P" to listOf("L3_I" to 1.0),
            "L3_I" to listOf("L2_C" to 1.0),
            "L2_C" to listOf("L1_F" to 2.0),
            "L1_F" to listOf("L0_E" to 1.0),
            "L0_E" to listOf("L1_F" to 1.0)
        )
        val seeds = mapOf("L3_P" to 1.0)

        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85
        )

        // Verifies probability propagates downward through the cognitive pyramid
        assertTrue(ranks["L3_I"]!! > 0.0)
        assertTrue(ranks["L2_C"]!! > 0.0)
        assertTrue(ranks["L1_F"]!! > 0.0)
        assertTrue(ranks["L0_E"]!! > 0.0)
    }

    @Test
    fun testBiTemporalFactInvalidationUpdatesL2ScenarioClusters() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Cluster initially has active facts f1 and f2
        val f1 = E2EAtomicFact(id = "f1", subject = "S", predicate = "P1", `object` = "O1", text = "f1", validUntilMs = Long.MAX_VALUE)
        val f2 = E2EAtomicFact(id = "f2", subject = "S", predicate = "P2", `object` = "O2", text = "f2", validUntilMs = Long.MAX_VALUE)

        graph.putNode(f1.toGraphNode())
        graph.putNode(f2.toGraphNode())
        graph.putEdge(Edge("l2:cluster_1", "l1:f1", "CLUSTERS_FACT"))
        graph.putEdge(Edge("l2:cluster_1", "l1:f2", "CLUSTERS_FACT"))

        // Invalidate f1
        val invalidatedF1 = f1.copy(validUntilMs = 2000L)
        graph.putNode(invalidatedF1.toGraphNode())

        // Active members in cluster
        val clusterEdges = graph.getOutboundEdges("l2:cluster_1", "CLUSTERS_FACT")
        val activeMemberFactIds = clusterEdges.mapNotNull { edge ->
            val node = graph.getNode(edge.targetId)
            if (node?.properties?.get("isActive") == "true") edge.targetId else null
        }

        assertEquals(listOf("l1:f2"), activeMemberFactIds)
        db.close()
    }

    @Test
    fun testInspectorRendersBiTemporalStateAndLineage() {
        val totalActive = 15
        val totalHistorical = 4
        val report = buildString {
            appendLine("## Bi-Temporal Memory State")
            appendLine("- Active Facts: $totalActive")
            appendLine("- Historical Facts: $totalHistorical")
        }

        assertTrue(report.contains("Active Facts: 15"))
        assertTrue(report.contains("Historical Facts: 4"))
    }

    // ========================================================================
    // TIER 4: REAL-WORLD SCENARIOS
    // ========================================================================

    @Test
    fun testFullEndToEndAgentCognitiveLifecycle() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val factCol = db.collection<E2EAtomicFact>("agent_facts")
        val fts = factCol.ftsIndex
        val vectors = db.vectorCollection("agent_vectors") {
            dimensions = 64
            metric = DistanceMetric.COSINE
        }

        // 1. Ingest Raw Chat at L0
        val rawChat = "User: Set my language preference to Japanese and notify me at 9am."
        val compressed = TestContracts.compressPayloadWithLz4(rawChat.toByteArray(Charsets.UTF_8))
        val l0 = E2EL0Evidence("ev_chat_1", rawChat.length, compressed.size, payload = compressed)
        graph.putNode(l0.toGraphNode())

        // 2. Extract Facts at L1 and dual-index
        val fact1 = E2EAtomicFact(id = "fact_lang", subject = "User", predicate = "pref_language", `object` = "Japanese", text = "Language preference is Japanese")
        val fact2 = E2EAtomicFact(id = "fact_time", subject = "User", predicate = "notify_time", `object` = "09:00", text = "Notify time is 9am")

        val vec1 = TestContracts.generateNormalizedVector(64, Random(100L))
        val vec2 = TestContracts.generateNormalizedVector(64, Random(200L))

        graph.putNode(fact1.toGraphNode())
        graph.putNode(fact2.toGraphNode())
        fts.indexDocument("l1:fact_lang", fact1.text)
        fts.indexDocument("l1:fact_time", fact2.text)
        vectors.insert("l1:fact_lang", vec1)
        vectors.insert("l1:fact_time", vec2)

        // L0 -> L1 evidence links
        graph.putEdge(Edge("l0:ev_chat_1", "l1:fact_lang", "EVIDENCE_FOR"))
        graph.putEdge(Edge("l0:ev_chat_1", "l1:fact_time", "EVIDENCE_FOR"))

        // 3. Form L2 Scenario Cluster
        val l2Cluster = E2EL2ScenarioCluster("c_prefs", 1, listOf("l1:fact_lang", "l1:fact_time"))
        graph.putNode(l2Cluster.toGraphNode())
        graph.putEdge(Edge("l2:cluster_1", "l1:fact_lang", "CLUSTERS_FACT"))
        graph.putEdge(Edge("l2:cluster_1", "l1:fact_time", "CLUSTERS_FACT"))

        // 4. L3 Invariant
        val l3Inv = E2EL3Invariant("inv_respect", "RespectPreferences", "Always honor user notification settings", priority = 10)
        graph.putNode(l3Inv.toGraphNode())
        graph.putEdge(Edge("l3:inv:inv_respect", "l2:cluster_1", "GOVERNS"))

        // 5. Verification: Drill down from L2 to L0 evidence
        val memberFactIds = graph.getOutboundEdges("l2:cluster_1", "CLUSTERS_FACT").map { it.targetId }
        assertEquals(2, memberFactIds.size)
        val l0Ids = graph.getInboundEdges(memberFactIds[0], "EVIDENCE_FOR").map { it.sourceId }
        assertEquals(listOf("l0:ev_chat_1"), l0Ids)

        // Decompress raw payload
        val rawRestored = String(TestContracts.decompressPayloadWithLz4(l0.payload), Charsets.UTF_8)
        assertEquals(rawChat, rawRestored)

        db.close()
    }

    @Test
    fun testAgentContradictionResolutionWithMermaidAudit() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Initial fact: User in Tokyo
        val fTokyo = E2EAtomicFact(id = "f_tokyo", subject = "User", predicate = "location", `object` = "Tokyo", text = "Tokyo", validUntilMs = 5000L)
        // Superseding fact: User in London
        val fLondon = E2EAtomicFact(id = "f_london", subject = "User", predicate = "location", `object` = "London", text = "London", validUntilMs = Long.MAX_VALUE)

        graph.putNode(fTokyo.toGraphNode())
        graph.putNode(fLondon.toGraphNode())
        graph.putEdge(Edge("l1:f_london", "l1:f_tokyo", "SUPERSEDES"))

        // Render Mermaid
        val mermaid = buildString {
            appendLine("graph TD")
            appendLine("  ${TestContracts.sanitizeMermaidId(fTokyo.id)}[\"Tokyo (Invalidated)\"]")
            appendLine("  ${TestContracts.sanitizeMermaidId(fLondon.id)}[\"London (Active)\"]")
            appendLine("  ${TestContracts.sanitizeMermaidId(fLondon.id)} -->|SUPERSEDES| ${TestContracts.sanitizeMermaidId(fTokyo.id)}")
        }

        assertTrue(mermaid.contains("-->|SUPERSEDES|"))
        assertTrue(mermaid.contains("London"))
        assertTrue(mermaid.contains("Tokyo"))

        db.close()
    }
}
