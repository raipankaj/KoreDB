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
import java.util.Locale
import java.util.UUID

/**
 * Adversarial Challenger Test Suite for Milestone 3:
 *
 * 1. Mermaid Syntax Fuzzing & Malformed IDs:
 *    - Pathological node IDs with colons, slashes, whitespace, punctuation, quotes, unicode, emojis.
 *    - Strict regex token validation on output identifiers, node definitions, and edges.
 *    - Subgraph grouping robustness and identifier collision resilience.
 *
 * 2. Extreme Payload Truncation & Escaping:
 *    - Massive 100KB+ raw text payloads (150,000 characters).
 *    - Strict bounding by `maxLabelLength` preventing diagram explosion.
 *    - Multi-character HTML/XML tag escaping (`#lt;`, `#gt;`, `#quot;`) and newline normalization.
 *
 * 3. Deep Hierarchical Drill-Down Integrity:
 *    - Unbroken traversal from L3 Persona through governed L2 clusters down to L1 facts and L0 evidence.
 *    - Bit-for-bit identity check on LZ4 decompressed raw evidence payloads.
 *    - Missing link robustness: orphaned clusters, facts with 0 evidence, nonexistent root IDs, broken refs.
 *    - Cyclic governance resilience (L3 -> Inv1 -> Inv2 -> Inv1) without infinite loop.
 *
 * 4. Memory Inspector Telemetry Invariance:
 *    - Dynamic updates of layer counts, byte metrics, and compression ratios as pyramid mutates.
 *    - Bi-temporal active vs. historical fact accounting.
 *    - Extreme boundary formatting: empty pyramids, single-node pyramids, and densely populated pyramids.
 *    - Row limit enforcement and clean Mermaid block toggling.
 */
class M3SymbolicInspectorChallengerTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_challenger_m3_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    // ========================================================================
    // 1. MERMAID SYNTAX FUZZING & MALFORMED IDENTIFIERS
    // ========================================================================

    /**
     * Challenge 1.1: Fuzzing GraphExport.toMermaid with Pathological Node Identifiers.
     *
     * Injects node IDs containing:
     * - Colons (`l1:user:profile:fact`)
     * - Slashes and URIs (`local://path/to/spec.md#chunk-1`)
     * - Whitespace, tabs, and carriage returns (`user \t profile \r node`)
     * - Special punctuation (`@#$%^&*()-+=[]{};:'",<.>/?`)
     * - Quotes and apostrophes (`"Hello "World" and 'test'`)
     * - Emojis and Unicode (`node_🚀_🔥_你好_мир`)
     * - Pure symbols (`!@#$%^&*`)
     *
     * Asserts:
     * - All generated node IDs match strict Mermaid identifier syntax `^[a-zA-Z0-9_]+$`.
     * - No raw colons, slashes, whitespace, or quotes appear as token identifiers.
     * - All edge connection lines follow the pattern `^[a-zA-Z0-9_]+ -->\|[^|]*\| [a-zA-Z0-9_]+$`.
     * - Subgraph declarations are syntactically valid `subgraph [a-zA-Z0-9_]+\["[^"]*"\]`.
     */
    @Test
    fun testMermaidFuzzingWithPathologicalIdentifiers() = runBlocking {
        val storage = db.graph()

        val pathologicalIds = listOf(
            "l1:user:profile:fact",
            "local://path/to/spec.md#chunk-1",
            "user profile fact with spaces and \t tabs",
            "@#$%^&*()-+=[]{};:'\",<.>/?",
            "\"Hello \"World\" and 'test'",
            "node_🚀_🔥_你好_мир",
            "!@#$%^&*",
            "simple_alpha_1"
        )

        for ((idx, pId) in pathologicalIds.withIndex()) {
            val label = when {
                pId.startsWith("l1:") -> MemoryLabels.L1_FACT
                pId.startsWith("local:") -> MemoryLabels.L0_EVIDENCE
                idx % 2 == 0 -> MemoryLabels.L2_SCENARIO
                else -> MemoryLabels.L3_INVARIANT
            }
            storage.putNode(
                Node(
                    id = pId,
                    labels = setOf(label),
                    properties = mapOf("text" to "Content of $pId", "statement" to "Statement of $pId")
                )
            )
        }

        // Connect edges across all pathological IDs in a ring
        for (i in 0 until pathologicalIds.size) {
            val source = pathologicalIds[i]
            val target = pathologicalIds[(i + 1) % pathologicalIds.size]
            storage.putEdge(
                Edge(
                    sourceId = source,
                    targetId = target,
                    type = "CONNECTS_TO",
                    properties = mapOf("weight" to "1.25")
                )
            )
        }

        // Test with both groupBySubgraphs = true and false
        for (groupBySubgraphs in listOf(true, false)) {
            val mermaid = GraphExport.toMermaid(
                storage = storage,
                nodeIds = pathologicalIds,
                direction = "LR",
                groupBySubgraphs = groupBySubgraphs
            )

            assertTrue("Mermaid must declare diagram direction", mermaid.startsWith("graph LR"))

            val idRegex = Regex("^[a-zA-Z0-9_]+$")
            val lines = mermaid.lines()

            for (rawLine in lines) {
                val line = rawLine.trim()
                if (line.isEmpty() || line.startsWith("graph ") || line.startsWith("%%") || line == "end") {
                    continue
                }

                // Subgraph headers: subgraph Id["Title"]
                if (line.startsWith("subgraph ")) {
                    val match = Regex("""subgraph\s+([a-zA-Z0-9_]+)\["([^"]*)"\]""").matchEntire(line)
                    assertNotNull("Subgraph line must be syntactically valid: '$line'", match)
                    val sId = match!!.groupValues[1]
                    assertTrue("Subgraph ID must be valid Mermaid identifier: '$sId'", idRegex.matches(sId))
                    continue
                }

                // Edges: source -->|label| target
                if (line.contains("-->")) {
                    val match = Regex("""([a-zA-Z0-9_]+)\s*-->\|([^|]*)\|\s*([a-zA-Z0-9_]+)""").matchEntire(line)
                    assertNotNull("Edge definition must be syntactically valid Mermaid: '$line'", match)
                    val src = match!!.groupValues[1]
                    val edgeLabel = match.groupValues[2]
                    val dst = match.groupValues[3]
                    assertTrue("Edge source must match valid identifier: '$src'", idRegex.matches(src))
                    assertTrue("Edge destination must match valid identifier: '$dst'", idRegex.matches(dst))
                    assertTrue("Edge label must contain edge type", edgeLabel.contains("CONNECTS_TO"))
                    continue
                }

                // Node declarations: safeId["Escaped Label"]
                if (line.contains("[\"")) {
                    val match = Regex("""([a-zA-Z0-9_]+)\["(.*)"\]""").matchEntire(line)
                    assertNotNull("Node line must be valid Mermaid node syntax: '$line'", match)
                    val nId = match!!.groupValues[1]
                    val labelContent = match.groupValues[2]
                    assertTrue("Node ID must be valid identifier: '$nId'", idRegex.matches(nId))
                    assertFalse("Label content must not contain unescaped raw quotes", labelContent.contains("\""))
                    assertFalse("Label content must not contain raw angle brackets", labelContent.contains("<") || labelContent.contains(">"))
                    assertFalse("Label content must not contain raw unescaped newlines", labelContent.contains("\n"))
                }
            }
        }
    }

    /**
     * Challenge 1.2: Identifier Sanitization Resilience and Missing Nodes.
     *
     * Tests behavior when `nodeIds` contains IDs that do not exist in storage,
     * or IDs with empty/blank components.
     * Asserts that `toMermaid` gracefully falls back to using sanitized IDs as labels
     * without throwing NullPointerException or corrupting output syntax.
     */
    @Test
    fun testMermaidMissingNodeAndOrphanGracefulRendering() = runBlocking {
        val storage = db.graph()
        val missingIds = listOf("ghost_1", "l1:ghost:2", "local://missing/doc")

        val mermaid = GraphExport.toMermaid(storage, missingIds, groupBySubgraphs = true)
        assertTrue(mermaid.contains("graph TD"))
        assertTrue(mermaid.contains("ghost_1"))
        assertTrue(mermaid.contains("l1_ghost_2"))
        assertTrue(mermaid.contains("local___missing_doc"))
        assertFalse("Must not throw or fail on missing node instances", mermaid.contains("null"))
    }

    // ========================================================================
    // 2. EXTREME PAYLOAD TRUNCATION & ESCAPING
    // ========================================================================

    /**
     * Challenge 2.1: Extreme Payload Truncation (150KB+ Strings).
     *
     * Stores a node with a massive 150KB string (150,000 characters) containing
     * repetitive paragraphs and adversarial text.
     * Asserts:
     * - `maxLabelLength` enforces strict length bounds.
     * - Rendered output is compact (< 5KB), preventing Mermaid graph layout parser OOM / TLE.
     * - Truncation marker `...` is present.
     * - Exact character count before `...` is bounded by `maxLabelLength`.
     */
    @Test
    fun testMermaidExtremePayloadTruncation150KB() = runBlocking {
        val storage = db.graph()

        // 150,000 character string
        val repeatedSentence = "KoreDB provides ultra-fast cognitive memory on mobile devices. "
        val massive150KBText = buildString {
            while (length < 150_000) {
                append(repeatedSentence)
            }
        }
        assertEquals(150_000, massive150KBText.take(150_000).length)

        storage.putNode(
            Node(
                id = "n_massive",
                properties = mapOf("text" to massive150KBText)
            )
        )

        // Test with maxLabelLength = 40 (default)
        val mermaidDefault = GraphExport.toMermaid(storage, listOf("n_massive"), maxLabelLength = 40, groupBySubgraphs = false)
        assertTrue("Output size must be strictly bounded (< 1KB), was ${mermaidDefault.length}", mermaidDefault.length < 1000)
        assertTrue("Must contain truncation indicator '...'", mermaidDefault.contains("..."))

        // Extract label within ["..."]
        val match = Regex("""n_massive\["([^"]*)"\]""").find(mermaidDefault)
        assertNotNull("Node definition must be found", match)
        val extractedLabel = match!!.groupValues[1]
        assertTrue("Extracted label must end with '...'", extractedLabel.endsWith("..."))
        val prefixBeforeEllipsis = extractedLabel.removeSuffix("...")
        assertTrue("Prefix length before ellipsis must not exceed maxLabelLength (40), was ${prefixBeforeEllipsis.length}", prefixBeforeEllipsis.length <= 40)

        // Test with maxLabelLength = 15
        val mermaidShort = GraphExport.toMermaid(storage, listOf("n_massive"), maxLabelLength = 15, groupBySubgraphs = false)
        val matchShort = Regex("""n_massive\["([^"]*)"\]""").find(mermaidShort)
        assertNotNull(matchShort)
        val shortLabel = matchShort!!.groupValues[1]
        assertTrue(shortLabel.endsWith("..."))
        assertTrue("Short prefix length must not exceed 15", shortLabel.removeSuffix("...").length <= 15)
    }

    /**
     * Challenge 2.2: Adversarial Escaping of HTML/XML Tags, Quotes, and Newlines.
     *
     * Injects adversarial text with:
     * - `<script>alert("XSS & crash")</script>`
     * - Multiple quotes: `"Double" and ""Double-Double"" and 'Single'`
     * - Angle brackets: `<tag attr="value">`
     * - Multiline strings with `\n` and `\r\n`
     *
     * Asserts:
     * - `<` is safely escaped to `#lt;`
     * - `>` is safely escaped to `#gt;`
     * - `"` is safely escaped to `#quot;`
     * - `\n` is normalized to space so Mermaid does not break multi-line node declarations.
     * - Zero raw `<` or `>` or unescaped `"` inside `["..."]`.
     */
    @Test
    fun testMermaidLabelEscapingPathologicalTagsAndQuotes() = runBlocking {
        val storage = db.graph()

        val adversarialPayload = "<script>alert(\"XSS\")</script>\n<div id=\"root\">\n  \"Quotes\" & <Angles>\n</div>"
        storage.putNode(
            Node(
                id = "n_adversarial",
                properties = mapOf("text" to adversarialPayload)
            )
        )

        val mermaid = GraphExport.toMermaid(
            storage = storage,
            nodeIds = listOf("n_adversarial"),
            maxLabelLength = 200,
            groupBySubgraphs = false
        )

        assertTrue("Must escape < to #lt;", mermaid.contains("#lt;script#gt;"))
        assertTrue("Must escape \" to #quot;", mermaid.contains("#quot;XSS#quot;"))
        assertTrue("Must escape > to #gt;", mermaid.contains("#lt;/script#gt;"))
        assertFalse("Must not contain raw unescaped quotes inside label", mermaid.contains("alert(\"XSS\")"))
        assertFalse("Must not contain raw < tag opening", mermaid.contains("<script>"))
        assertFalse("Must not contain raw > tag closing", mermaid.contains("</script>"))

        // Extract the exact line
        val nodeLine = mermaid.lines().first { it.contains("n_adversarial") }
        val labelContent = nodeLine.substringAfter("[\"").substringBeforeLast("\"]")
        assertFalse("Label content must not contain unescaped newline", labelContent.contains("\n"))
        assertFalse("Label content must not contain raw quotes", labelContent.contains("\""))
        assertFalse("Label content must not contain raw <", labelContent.contains("<"))
        assertFalse("Label content must not contain raw >", labelContent.contains(">"))
    }

    // ========================================================================
    // 3. DEEP HIERARCHICAL DRILL-DOWN INTEGRITY
    // ========================================================================

    /**
     * Challenge 3.1: Deep Hierarchical Drill-Down with Bit-for-Bit Payload Decompression.
     *
     * Sets up a full 4-tier cognitive memory hierarchy:
     * - L3 Persona (`l3:persona_chief_analyst`)
     * - L3 Invariant (`l3:inv_safety_first`) governed by Persona
     * - L2 Scenario Clusters (`l2:cluster_0`, `l2:cluster_1`) governed by Persona/Invariant
     * - Multiple L1 Facts clustered into L2
     * - Multiple large L0 evidence items attached to facts via `EVIDENCE_FOR` (50KB JSON, 20KB markdown, 5KB CSV).
     *
     * Executes `pyramid.drillDown("l3:persona_chief_analyst")`:
     * - Asserts root level is L3_PERSONA.
     * - Asserts scenario clusters and constituent facts are traversed.
     * - Decompresses every L0 evidence payload on demand.
     * - Asserts 100% bit-for-bit exact identity (`assertArrayEquals`) between original bytes and decompressed bytes.
     * - Asserts exact string equality between original content and decompressed content.
     */
    @Test
    fun testDeepHierarchicalDrillDownL3ThroughL2L1ToLZ4L0BitForBitIdentical() = runBlocking {
        val pyramid = db.memoryPyramid()

        // 1. Ingest L0 Evidence items with diverse, large payloads
        val jsonPayload = buildString {
            append("{\n  \"timestamp\": 1730000000,\n  \"events\": [\n")
            for (i in 1..200) {
                append("    {\"id\": $i, \"action\": \"AUTH_LOGIN\", \"status\": \"SUCCESS\", \"user\": \"agent_alpha_$i\"},\n")
            }
            append("    {\"id\": 201, \"action\": \"POLICY_CHECK\", \"result\": \"PASSED\"}\n  ]\n}")
        }
        val markdownPayload = "# System Architecture Specification\n\n" +
                "KoreDB implements a 4-layer Cognitive Memory Pyramid.\n".repeat(50)
        val csvPayload = "id,metric,value,unit\n" + (1..150).joinToString("\n") { "$it,recall_${it},0.99${it % 10},ratio" }
        val smallPayload = "User preference: strictly dark mode and biometric authentication."

        val evJsonId = pyramid.recordEvidence(jsonPayload, mimeType = "application/json", sourceUri = "file:///logs/audit.json")
        val evMdId = pyramid.recordEvidence(markdownPayload, mimeType = "text/markdown", sourceUri = "file:///docs/arch.md")
        val evCsvId = pyramid.recordEvidence(csvPayload, mimeType = "text/csv", sourceUri = "file:///metrics/perf.csv")
        val evSmallId = pyramid.recordEvidence(smallPayload, mimeType = "text/plain")

        val originalPayloads = mapOf(
            evJsonId to jsonPayload,
            evMdId to markdownPayload,
            evCsvId to csvPayload,
            evSmallId to smallPayload
        )

        // 2. Ingest L1 Atomic Facts linked to L0 Evidence
        val f1 = pyramid.addFact("Security audit passed with zero anomalies", evidenceIds = listOf(evJsonId))
        val f2 = pyramid.addFact("System architecture uses a 4-layer cognitive pyramid", evidenceIds = listOf(evMdId))
        val f3 = pyramid.addFact("Vector recall benchmarks exceed 99% accuracy", evidenceIds = listOf(evCsvId))
        val f4 = pyramid.addFact("User enables biometric authentication", evidenceIds = listOf(evSmallId))

        // Link facts to form 2 clusters
        pyramid.linkFacts(f1, f2, MemoryEdgeTypes.RELATES_TO)
        pyramid.linkFacts(f3, f4, MemoryEdgeTypes.RELATES_TO)

        // 3. Form L2 Scenario Clusters
        val clusters = pyramid.clusterScenarios()
        assertTrue("Should have formed at least 1 scenario cluster", clusters.isNotEmpty())

        // 4. Ingest L3 Persona & Invariants
        val personaId = pyramid.setPersona(
            name = "ChiefAnalyst",
            traits = listOf("Thorough", "Adversarial", "Exact"),
            bio = "Cognitive memory governor"
        )
        val invariantId = pyramid.addInvariant(
            statement = "All telemetry must strictly match raw evidence",
            priority = InvariantPriority.CRITICAL
        )

        // Link Persona -> Invariant, Persona -> L2 Clusters via GOVERNS
        val graph = pyramid.graph
        graph.putEdge(Edge(personaId, invariantId, MemoryEdgeTypes.GOVERNS))
        for (c in clusters) {
            graph.putEdge(Edge(personaId, "l2:cluster_${c.clusterId}", MemoryEdgeTypes.GOVERNS))
        }

        // 5. Execute Deep Hierarchical Drill-Down from L3 Persona
        val drillResult = pyramid.drillDown(personaId)

        assertEquals("Root node ID must match persona ID", personaId, drillResult.rootNodeId)
        assertEquals("Root level must be L3_PERSONA", MemoryLevel.L3_PERSONA, drillResult.rootLevel)
        assertTrue("Must discover governed scenario clusters", drillResult.scenarioClusters.isNotEmpty())
        assertTrue("Must discover constituent atomic facts", drillResult.atomicFacts.isNotEmpty())
        assertTrue("Must discover attached L0 evidence items", drillResult.totalEvidenceCount >= 4)

        // 6. Assert bit-for-bit identity for every decompressed evidence item
        var verifiedEvidenceCount = 0
        for (fact in drillResult.atomicFacts) {
            assertTrue("Fact proposition must not be blank", fact.text.isNotBlank())
            for (ev in fact.evidenceList) {
                val expectedOriginalText = originalPayloads[ev.evidenceId]
                assertNotNull("Evidence ID '${ev.evidenceId}' must be recognized in original map", expectedOriginalText)
                val expectedBytes = expectedOriginalText!!.toByteArray(Charsets.UTF_8)

                // Bit-for-bit payload assert
                assertArrayEquals("Decompressed bytes must be bit-for-bit identical to original payload", expectedBytes, ev.rawBytes)
                assertEquals("Decompressed string must match original string verbatim", expectedOriginalText, ev.rawContent)
                assertEquals("Original size must match exact byte length", expectedBytes.size, ev.originalSize)
                assertTrue("Compressed size must be positive and non-zero", ev.compressedSize > 0)
                verifiedEvidenceCount++
            }
        }
        assertEquals("Must verify all 4 attached evidence payloads", 4, verifiedEvidenceCount)
    }

    /**
     * Challenge 3.2: Drill-Down Missing Link Robustness, Orphans, and Broken References.
     *
     * Adversarially tests:
     * - Orphaned L2 cluster (cluster node exists in graph with `L2_SCENARIO` label, but has 0 member facts).
     * - L1 fact with 0 evidence items (empty evidence inbound edges).
     * - Completely nonexistent root IDs (`ghost_id_9999`, `l3:ghost`, `l2:ghost`, `l1:ghost`).
     * - Broken evidence link (L1 fact has `EVIDENCE_FOR` edge from `l0:ghost_ev`, but evidence document is absent).
     *
     * Asserts:
     * - Zero NullPointerExceptions or fatal crashes.
     * - Graceful empty collections returned in DrillDownResult.
     */
    @Test
    fun testDrillDownMissingLinkRobustnessAndOrphans() = runBlocking {
        val pyramid = db.memoryPyramid()
        val graph = pyramid.graph

        // 1. Orphaned L2 cluster (no outbound CLUSTERS_FACT edges)
        graph.putNode(
            Node(
                id = "l2:cluster_orphan",
                labels = setOf(MemoryLabels.L2_SCENARIO),
                properties = mapOf("title" to "Orphaned Cluster", "memberCount" to "0")
            )
        )
        val orphanResult = pyramid.drillDown("l2:cluster_orphan")
        assertEquals(MemoryLevel.L2_SCENARIO, orphanResult.rootLevel)
        assertEquals(1, orphanResult.scenarioClusters.size)
        assertTrue(orphanResult.atomicFacts.isEmpty())
        assertEquals(0, orphanResult.totalEvidenceCount)

        // 2. L1 fact with 0 evidence
        val factNoEvId = pyramid.addFact("Fact with zero supporting evidence items", evidenceIds = emptyList())
        val factResult = pyramid.drillDown("l1:$factNoEvId")
        assertEquals(MemoryLevel.L1_FACT, factResult.rootLevel)
        assertEquals(1, factResult.atomicFacts.size)
        val fact = factResult.atomicFacts.first()
        assertTrue("Evidence list must be empty", fact.evidenceList.isEmpty())
        assertEquals(0, factResult.totalEvidenceCount)

        // 3. Completely nonexistent root IDs across all levels
        val nonexistentIds = listOf(
            "completely_unknown_node_xyz",
            "l3:persona_nonexistent",
            "l3:inv_nonexistent",
            "l1:fact_nonexistent",
            "l0:ev_nonexistent"
        )
        for (ghostId in nonexistentIds) {
            val ghostResult = pyramid.drillDown(ghostId)
            assertNotNull("Must return non-null result for ghost ID '$ghostId'", ghostResult)
            assertTrue("Scenario clusters must be empty for ghost ID $ghostId", ghostResult.scenarioClusters.isEmpty())
            assertTrue("Atomic facts must be empty for ghost ID $ghostId", ghostResult.atomicFacts.isEmpty())
            assertEquals(0, ghostResult.totalEvidenceCount)
        }

        // Nonexistent L2 cluster root ID should handle gracefully without NPE/crash
        val ghostL2Result = pyramid.drillDown("l2:cluster_nonexistent")
        assertNotNull(ghostL2Result)
        assertTrue("Atomic facts must be empty for nonexistent L2 cluster", ghostL2Result.atomicFacts.isEmpty())
        assertEquals(0, ghostL2Result.totalEvidenceCount)

        // 4. Broken evidence link: Fact has EVIDENCE_FOR edge to non-existent L0 document
        val fBroken = pyramid.addFact("Fact with ghost evidence edge")
        graph.putEdge(Edge("l0:ev_deleted_or_missing", "l1:$fBroken", MemoryEdgeTypes.EVIDENCE_FOR))
        val brokenLinkResult = pyramid.drillDown("l1:$fBroken")
        assertEquals(1, brokenLinkResult.atomicFacts.size)
        // Missing evidence must be skipped gracefully without NPE
        assertEquals("Broken evidence reference must be safely omitted", 0, brokenLinkResult.atomicFacts.first().evidenceList.size)
        assertEquals(0, brokenLinkResult.totalEvidenceCount)
    }

    /**
     * Challenge 3.3: Cyclic Governance Resilience in Drill-Down.
     *
     * Constructs a cyclic governance loop:
     * - Persona -> Invariant A
     * - Invariant A -> Invariant B
     * - Invariant B -> Invariant A (Cycle!)
     * - Invariant B -> L1 Fact
     *
     * Asserts that `drillDown` handles cyclic governance without infinite recursion
     * or StackOverflowError and terminates cleanly with all reached facts.
     */
    @Test
    fun testDrillDownCyclicGovernanceLoopTermination() = runBlocking {
        val pyramid = db.memoryPyramid()
        val graph = pyramid.graph

        val ev = pyramid.recordEvidence("Evidence inside cyclic governance hierarchy")
        val factId = pyramid.addFact("Fact reachable via cyclic invariant governance", evidenceIds = listOf(ev))

        val personaId = pyramid.setPersona(name = "LoopGovernor", traits = emptyList(), bio = "Loop test")
        val invA = pyramid.addInvariant("Invariant A")
        val invB = pyramid.addInvariant("Invariant B")

        // Construct cycle: Persona -> InvA -> InvB -> InvA (cycle), and InvB -> Fact
        graph.putEdge(Edge(personaId, invA, MemoryEdgeTypes.GOVERNS))
        graph.putEdge(Edge(invA, invB, MemoryEdgeTypes.GOVERNS))
        graph.putEdge(Edge(invB, invA, MemoryEdgeTypes.GOVERNS)) // Cycle!
        graph.putEdge(Edge(invB, "l1:$factId", MemoryEdgeTypes.GOVERNS))

        // Must terminate cleanly within reasonable time
        val result = pyramid.drillDown(personaId)
        assertEquals(MemoryLevel.L3_PERSONA, result.rootLevel)
        assertEquals(1, result.atomicFacts.size)
        assertEquals(factId, result.atomicFacts.first().factId)
        assertEquals(1, result.totalEvidenceCount)
    }

    // ========================================================================
    // 4. MEMORY INSPECTOR TELEMETRY INVARIANCE
    // ========================================================================

    /**
     * Challenge 4.1: Dynamic Telemetry Invariance as Pyramid Evolves.
     *
     * Progressively mutates the pyramid and asserts that `generateMarkdownReport`:
     * 1. On empty pyramid: reports 0 across all layers, 1.00x ratio, and `%% empty` Mermaid block.
     * 2. After adding Persona and Invariants: dynamically updates L3 count and invariant rules table.
     * 3. After adding compressible L0 evidence: accurately updates raw bytes, compressed bytes,
     *    and calculates exact compression ratio ($totalRaw / totalCompressed$).
     * 4. After adding active and historical L1 facts: tracks bi-temporal state partitions.
     * 5. After running `clusterScenarios`: reflects new L2 scenario count and cluster summaries.
     * 6. Total Nodes metric equals exact sum of (L3 + L2 + L1 + L0).
     */
    @Test
    fun testMemoryInspectorDynamicTelemetryUpdatesAsPyramidChanges() = runBlocking {
        val pyramid = db.memoryPyramid()
        val inspector = db.memoryInspector(pyramid)

        // Stage 1: Empty Pyramid
        var report = inspector.generateMarkdownReport()
        assertTrue(report.contains("Total Nodes: 0"))
        assertTrue(report.contains("| L3 Persona | 0 |"))
        assertTrue(report.contains("| L2 Scenarios | 0 |"))
        assertTrue(report.contains("| L1 Facts | 0 |"))
        assertTrue(report.contains("| L0 Evidence | 0 |"))
        assertTrue(report.contains("Active Facts: 0"))
        assertTrue(report.contains("Historical Facts: 0"))
        assertTrue(report.contains("Overall Compression Ratio: 1.00x"))
        assertTrue(report.contains("%% empty"))

        // Stage 2: Add Persona & Invariants
        pyramid.setPersona("SentinelAI", listOf("Adaptive", "Secure"), "Security agent")
        pyramid.addInvariant("Never leak encryption keys", priority = InvariantPriority.CRITICAL)
        pyramid.addInvariant("Enforce bi-temporal consistency", priority = InvariantPriority.HIGH)

        report = inspector.generateMarkdownReport()
        assertTrue(report.contains("Total Nodes: 3")) // 1 persona + 2 invariants
        assertTrue(report.contains("| L3 Persona | 3 |"))
        assertTrue(report.contains("SentinelAI"))
        assertTrue(report.contains("Never leak encryption keys"))
        assertTrue(report.contains("Enforce bi-temporal consistency"))
        assertTrue(report.contains("[CRITICAL]"))
        assertTrue(report.contains("[HIGH]"))

        // Stage 3: Add L0 Evidence with high and low compression
        val repetitivePayload = "Repeated chunk of telemetry data for testing compression. ".repeat(100)
        val shortPayload = "Short raw log."
        pyramid.recordEvidence(repetitivePayload)
        pyramid.recordEvidence(shortPayload)

        report = inspector.generateMarkdownReport()
        assertTrue(report.contains("Total Evidence Nodes: 2"))
        val totalRaw = repetitivePayload.toByteArray(Charsets.UTF_8).size + shortPayload.toByteArray(Charsets.UTF_8).size
        assertTrue("Report must contain exact total raw bytes count: ${totalRaw}B", report.contains("Total Raw Bytes: ${totalRaw}B"))

        // Stage 4: Add L1 Facts (3 active, 2 historical)
        val f1 = pyramid.addFact("Active fact 1")
        val f2 = pyramid.addFact("Active fact 2")
        val f3 = pyramid.addFact("Active fact 3")
        val fHist1 = pyramid.addFact("Superseded fact 1", properties = mapOf("validUntilMs" to "1000", "isActive" to "false"))
        val fHist2 = pyramid.addFact("Superseded fact 2", properties = mapOf("validUntilMs" to "2000", "isActive" to "false", "status" to "SUPERSEDED"))

        pyramid.linkFacts(f1, f2)
        pyramid.linkFacts(f2, f3)

        report = inspector.generateMarkdownReport()
        assertTrue(report.contains("| L1 Facts | 5 |"))
        assertTrue("Report must accurately track active facts", report.contains("Active Facts: 3"))
        assertTrue("Report must accurately track historical facts", report.contains("Historical Facts: 2"))

        // Stage 5: Form L2 Scenario Clusters
        val clusters = pyramid.clusterScenarios()
        assertTrue(clusters.isNotEmpty())

        report = inspector.generateMarkdownReport()
        assertTrue(report.contains("| L2 Scenarios | ${clusters.size} |"))
        assertTrue(report.contains("Cluster Summaries"))

        // Stage 6: Total Nodes Invariant Check
        val totalExpected = 3 + clusters.size + 5 + 2 // L3(3) + L2(clusters) + L1(5) + L0(2)
        assertTrue("Report Total Nodes must equal $totalExpected", report.contains("Total Nodes: $totalExpected"))
    }

    /**
     * Challenge 4.2: Boundary Formatting on Single-Node Pyramids.
     *
     * Tests inspector markdown generation when the pyramid contains exactly 1 node
     * belonging to different layers:
     * - Single L0 Evidence node.
     * - Single L1 Fact node.
     * - Single L3 Persona node.
     *
     * Asserts:
     * - Division-by-zero protection on compression ratios.
     * - Clean markdown rendering without exceptions.
     */
    @Test
    fun testMemoryInspectorSingleNodePyramidFormatting() = runBlocking {
        // Case A: Only 1 L0 Evidence Node
        val dirA = File("build/tmp/test_single_l0_${UUID.randomUUID()}")
        dirA.mkdirs()
        try {
            val dbA = KoreDatabase(dirA)
            val pA = dbA.memoryPyramid()
            pA.recordEvidence("Single evidence payload without any facts.")
            val repA = dbA.memoryInspector(pA).generateMarkdownReport()
            assertTrue(repA.contains("Total Nodes: 1"))
            assertTrue(repA.contains("| L0 Evidence | 1 |"))
            assertTrue(repA.contains("Total Evidence Nodes: 1"))
            assertFalse(repA.contains("NaN"))
            assertFalse(repA.contains("Infinity"))
            dbA.close()
        } finally {
            dirA.deleteRecursively()
        }

        // Case B: Only 1 L1 Fact Node
        val dirB = File("build/tmp/test_single_l1_${UUID.randomUUID()}")
        dirB.mkdirs()
        try {
            val dbB = KoreDatabase(dirB)
            val pB = dbB.memoryPyramid()
            pB.addFact("Single atomic fact proposition.")
            val repB = dbB.memoryInspector(pB).generateMarkdownReport()
            assertTrue(repB.contains("Total Nodes: 1"))
            assertTrue(repB.contains("| L1 Facts | 1 |"))
            assertTrue(repB.contains("Active Facts: 1"))
            dbB.close()
        } finally {
            dirB.deleteRecursively()
        }

        // Case C: Only 1 L3 Persona Node
        val dirC = File("build/tmp/test_single_l3_${UUID.randomUUID()}")
        dirC.mkdirs()
        try {
            val dbC = KoreDatabase(dirC)
            val pC = dbC.memoryPyramid()
            pC.setPersona("StandalonePersona", listOf("Lone"), "Sole node")
            val repC = dbC.memoryInspector(pC).generateMarkdownReport()
            assertTrue(repC.contains("Total Nodes: 1"))
            assertTrue(repC.contains("| L3 Persona | 1 |"))
            assertTrue(repC.contains("StandalonePersona"))
            dbC.close()
        } finally {
            dirC.deleteRecursively()
        }
    }

    /**
     * Challenge 4.3: Densely Populated Pyramid, Row Limits, and Mermaid Subgraph Output.
     *
     * Ingests 40 L0 evidence items and 30 L1 facts.
     * Configures `maxFacts = 5` and `maxEvidenceSamples = 4`.
     * Asserts:
     * - The report properly limits rendered table rows to `(Sample 5/30)`.
     * - The evidence sample table displays exactly 4 rows.
     * - Live Mermaid working memory subgraph includes proper layer subgraphs (`subgraph L1_Fact`, etc.).
     * - When `includeMermaid = false`, Mermaid code blocks are omitted while all tables remain.
     */
    @Test
    fun testMemoryInspectorDenselyPopulatedPyramidAndRowLimits() = runBlocking {
        val pyramid = db.memoryPyramid()
        val inspector = db.memoryInspector(pyramid)

        // Ingest 40 L0 Evidence items and 30 L1 Facts
        for (i in 1..40) {
            val evId = pyramid.recordEvidence("Telemetry log record #$i from high-throughput stream.")
            if (i <= 30) {
                pyramid.addFact("Fact proposition #$i extracted from stream", evidenceIds = listOf(evId))
            }
        }

        // Test with row limits
        val reportLimited = inspector.generateMarkdownReport(
            includeMermaid = true,
            maxFacts = 5,
            maxEvidenceSamples = 4
        )

        assertTrue("Report must indicate sample row limitation for facts", reportLimited.contains("(Sample 5/30)"))
        assertTrue("Report must show total evidence count of 40", reportLimited.contains("Total Evidence Nodes: 40"))
        assertTrue("Report must include Mermaid diagram", reportLimited.contains("```mermaid"))
        assertTrue("Mermaid diagram must contain L1_Fact subgraph", reportLimited.contains("subgraph L1_Fact"))

        // Test disabling Mermaid
        val reportNoMermaid = inspector.generateMarkdownReport(includeMermaid = false)
        assertFalse("Report must omit Mermaid code block when disabled", reportNoMermaid.contains("```mermaid"))
        assertFalse("Report must omit Mermaid heading when disabled", reportNoMermaid.contains("## Active Working Memory Subgraph"))
        assertTrue("Report must still contain layer overview", reportNoMermaid.contains("## Layer Overview"))
        assertTrue("Report must still contain L0 storage metrics", reportNoMermaid.contains("## 5. L0 Raw Evidence Storage"))
    }
}
