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

import com.pankaj.koredb.graph.GraphExport
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Inspection Mirror: Generates a human-readable and agent-readable Markdown report
 * reflecting the active state of the L0–L3 Cognitive Memory Pyramid.
 */
class KoreMemoryInspector(val pyramid: KoreMemoryPyramid) {

    /**
     * Generates a complete Markdown mirror of the memory hierarchy including
     * layer telemetry, invariants, scenarios, atomic facts, L0 compression metrics,
     * and a live Mermaid working memory subgraph.
     */
    fun generateMarkdownReport(
        includeMermaid: Boolean = true,
        maxFacts: Int = 20,
        maxEvidenceSamples: Int = 5
    ): String = buildString {
        val dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
        val nowStr = dateFormat.format(Date())

        appendLine("# KoreDB Cognitive Memory Mirror")
        appendLine("Generated: $nowStr")
        appendLine()

        // 1. Pyramid Telemetry
        val l3PersonaNodes = pyramid.graph.getNodesByLabel(MemoryLabels.L3_PERSONA)
        val l3InvariantNodes = pyramid.graph.getNodesByLabel(MemoryLabels.L3_INVARIANT)
        val l2ScenarioNodes = pyramid.graph.getNodesByLabel(MemoryLabels.L2_SCENARIO)
        val l1FactNodes = pyramid.graph.getNodesByLabel(MemoryLabels.L1_FACT)
        val l0EvidenceNodes = pyramid.graph.getNodesByLabel(MemoryLabels.L0_EVIDENCE)
        val totalNodes = l3PersonaNodes.size + l3InvariantNodes.size + l2ScenarioNodes.size + l1FactNodes.size + l0EvidenceNodes.size

        appendLine("## Layer Overview")
        appendLine("Total Nodes: $totalNodes")
        appendLine()
        appendLine("| Layer | Node Count | Description |")
        appendLine("|---|---|---|")
        appendLine("| L3 Persona | ${l3PersonaNodes.size + l3InvariantNodes.size} | System identity & constraints |")
        appendLine("| L2 Scenarios | ${l2ScenarioNodes.size} | Community clusters |")
        appendLine("| L1 Facts | ${l1FactNodes.size} | Atomic propositions |")
        appendLine("| L0 Evidence | ${l0EvidenceNodes.size} | LZ4 raw payloads |")
        appendLine()

        appendLine("## 1. Executive Summary & Pyramid Statistics")
        appendLine("| Layer | Semantic Role | Node Count | Key Storage Engine |")
        appendLine("| :--- | :--- | :--- | :--- |")
        appendLine("| **L3** | Persona & Invariants | ${l3PersonaNodes.size + l3InvariantNodes.size} | GraphStorage (`g:v:l3:...`) |")
        appendLine("| **L2** | Scenario Clusters | ${l2ScenarioNodes.size} | Graph + HNSW Centroids |")
        appendLine("| **L1** | Atomic Facts | ${l1FactNodes.size} | BM25 FTS + HNSW Vector + Graph |")
        appendLine("| **L0** | Raw Evidence | ${l0EvidenceNodes.size} | LZ4-Compressed `KoreCollection` |")
        appendLine()

        // 2. L3 Persona & Invariants
        appendLine("## 2. L3 Persona & Invariants")
        if (l3PersonaNodes.isEmpty() && l3InvariantNodes.isEmpty()) {
            appendLine("*No persona or invariants configured.*")
        } else {
            for (p in l3PersonaNodes) {
                appendLine("### Persona: ${p.properties["name"] ?: p.id}")
                appendLine("- **Traits**: ${p.properties["traits"] ?: "N/A"}")
                appendLine("- **Bio**: ${p.properties["bio"] ?: "N/A"}")
            }
            if (l3InvariantNodes.isNotEmpty()) {
                appendLine()
                appendLine("### Active Invariants")
                appendLine("| ID | Name | Rule | Priority |")
                appendLine("|---|---|---|---|")
                for (inv in l3InvariantNodes) {
                    val id = inv.properties["id"] ?: inv.id.removePrefix("l3:inv:").removePrefix("l3:inv_")
                    val name = inv.properties["name"] ?: id
                    val rule = inv.properties["rule"] ?: inv.properties["statement"] ?: ""
                    val priority = inv.properties["priority"] ?: "1"
                    appendLine("| $id | $name | $rule | $priority |")
                }
                appendLine()
                appendLine("### Invariant Boundaries")
                for (inv in l3InvariantNodes) {
                    val p = inv.properties["priority"] ?: "CRITICAL"
                    val s = inv.properties["statement"] ?: inv.properties["rule"] ?: ""
                    appendLine("- `[$p]` **${inv.id}**: $s")
                }
            }
        }
        appendLine()

        // 3. L2 Scenarios
        appendLine("## 3. L2 Scenario Clusters")
        if (l2ScenarioNodes.isEmpty()) {
            appendLine("*No scenario clusters detected yet. Run `clusterScenarios()`.*")
        } else {
            appendLine("### Cluster Summaries")
            for (s in l2ScenarioNodes) {
                val commId = s.properties["communityId"] ?: s.properties["clusterId"] ?: s.id.removePrefix("l2:cluster_")
                val count = s.properties["memberCount"] ?: "0"
                appendLine("- **Cluster $commId**: $count facts")
            }
            appendLine()
            appendLine("| Cluster ID | Scenario Title | Member Facts | Centroid Indexed |")
            appendLine("| :--- | :--- | :--- | :--- |")
            for (s in l2ScenarioNodes) {
                val cid = s.properties["clusterId"] ?: s.id
                val title = s.properties["title"] ?: ""
                val count = s.properties["memberCount"] ?: "0"
                val centroid = s.properties["hasCentroid"] ?: "false"
                appendLine("| `$cid` | $title | $count | $centroid |")
            }
        }
        appendLine()

        // 4. L1 Facts & Bi-Temporal State
        var activeFactsCount = 0
        var historicalFactsCount = 0
        val nowMs = System.currentTimeMillis()
        for (f in l1FactNodes) {
            val status = f.properties["status"]
            val validUntil = f.properties["validUntilMs"]?.toLongOrNull() ?: Long.MAX_VALUE
            val isExplicitlyInactive = f.properties["isActive"] == "false"
            val isSuperseded = status == "SUPERSEDED" || validUntil <= nowMs || isExplicitlyInactive
            val isActive = !isSuperseded && (f.properties["isActive"] == "true" || validUntil == Long.MAX_VALUE)
            if (isActive) activeFactsCount++ else historicalFactsCount++
        }

        appendLine("## Bi-Temporal Memory State")
        appendLine("- Active Facts: $activeFactsCount")
        appendLine("- Historical Facts: $historicalFactsCount")
        appendLine()

        appendLine("## 4. L1 Atomic Facts Knowledge Base (Sample ${l1FactNodes.take(maxFacts).size}/${l1FactNodes.size})")
        if (l1FactNodes.isEmpty()) {
            appendLine("*No atomic facts recorded.*")
        } else {
            appendLine("| Fact ID | Proposition | Confidence | Status | Supporting Ev |")
            appendLine("| :--- | :--- | :--- | :--- | :--- |")
            for (f in l1FactNodes.take(maxFacts)) {
                val fid = f.properties["factId"] ?: f.id
                val text = f.properties["text"] ?: ""
                val conf = f.properties["confidence"] ?: f.properties["certainty"] ?: "1.0"
                val status = f.properties["status"] ?: "ACTIVE"
                val evCount = pyramid.graph.getInboundSourceIds(f.id, MemoryEdgeTypes.EVIDENCE_FOR).size
                appendLine("| `$fid` | $text | $conf | $status | $evCount items |")
            }
        }
        appendLine()

        // 5. L0 Raw Evidence
        var totalOriginal = 0L
        var totalCompressed = 0L
        for (ev in l0EvidenceNodes) {
            totalOriginal += ev.properties["originalSize"]?.toLongOrNull() ?: 0L
            totalCompressed += ev.properties["compressedSize"]?.toLongOrNull() ?: 0L
        }
        val overallRatio = if (totalCompressed > 0) totalOriginal.toFloat() / totalCompressed.toFloat() else 1.0f

        appendLine("## 5. L0 Raw Evidence Storage")
        appendLine("- Total Evidence Nodes: ${l0EvidenceNodes.size}")
        appendLine("- Total Raw Bytes: ${totalOriginal}B")
        appendLine("- Total Compressed Bytes: ${totalCompressed}B")
        appendLine("- Overall Compression Ratio: ${"%.2f".format(Locale.US, overallRatio)}x")
        appendLine()

        if (l0EvidenceNodes.isEmpty()) {
            appendLine("*No raw evidence recorded.*")
        } else {
            appendLine("| Evidence ID | MIME Type | Raw Size | Compressed | Ratio | Summary |")
            appendLine("| :--- | :--- | :--- | :--- | :--- | :--- |")
            for (ev in l0EvidenceNodes.take(maxEvidenceSamples)) {
                val eid = ev.properties["evidenceId"] ?: ev.id
                val mime = ev.properties["mimeType"] ?: "text/plain"
                val orig = ev.properties["originalSize"]?.toIntOrNull() ?: 0
                val comp = ev.properties["compressedSize"]?.toIntOrNull() ?: 0
                val ratio = if (orig > 0) "%.1f%%".format(Locale.US, (comp.toDouble() / orig) * 100) else "N/A"
                val summary = ev.properties["summary"] ?: ""
                appendLine("| `$eid` | `$mime` | ${orig}B | ${comp}B | $ratio | $summary |")
            }
        }
        appendLine()

        // 6. Active Working Memory Subgraph (Mermaid)
        if (includeMermaid) {
            val allNodeIds = (l3PersonaNodes + l3InvariantNodes + l2ScenarioNodes + l1FactNodes.take(maxFacts) + l0EvidenceNodes.take(maxEvidenceSamples)).map { it.id }
            appendLine("## Active Working Memory Subgraph")
            appendLine("```mermaid")
            append(GraphExport.toMermaid(pyramid.graph, allNodeIds, direction = "TD", groupBySubgraphs = true))
            appendLine("```")
        }
    }
}
