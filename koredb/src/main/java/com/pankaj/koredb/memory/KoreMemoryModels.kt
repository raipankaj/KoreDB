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

import kotlinx.serialization.Serializable

/**
 * Cognitive memory hierarchy level.
 */
enum class MemoryLevel {
    L0_EVIDENCE,
    L1_FACT,
    L2_SCENARIO,
    L3_PERSONA,
    L3_INVARIANT,
    UNKNOWN
}

/**
 * Standard edge relationship types across the memory pyramid.
 */
object MemoryEdgeTypes {
    const val EVIDENCE_FOR = "EVIDENCE_FOR"     // L0 -> L1
    const val CLUSTERS_FACT = "CLUSTERS_FACT"   // L2 -> L1
    const val GOVERNS = "GOVERNS"               // L3 -> L2 or L3 -> L1
    const val RELATES_TO = "RELATES_TO"         // L1 -> L1
    const val CAUSES = "CAUSES"                 // L1 -> L1
    const val SUPERSEDES = "SUPERSEDES"         // L1 -> L1 (Bi-Temporal Invalidation)
}

/**
 * Standard node labels across the memory pyramid.
 */
object MemoryLabels {
    const val L0_EVIDENCE = "L0_Evidence"
    const val L1_FACT = "L1_Fact"
    const val L2_SCENARIO = "L2_Scenario"
    const val L3_PERSONA = "L3_Persona"
    const val L3_INVARIANT = "L3_Invariant"
}

/**
 * Priority for L3 invariant constraints.
 */
enum class InvariantPriority {
    CRITICAL,
    HIGH,
    MEDIUM,
    LOW
}

/**
 * Document stored in [com.pankaj.koredb.core.KoreCollection] for L0 raw evidence with LZ4 compression.
 */
@Serializable
data class L0Evidence(
    val id: String,
    val compressedPayload: ByteArray,
    val originalSize: Int,
    val timestampMs: Long = System.currentTimeMillis(),
    val mimeType: String = "text/plain",
    val sourceUri: String? = null,
    val metadata: Map<String, String> = emptyMap()
) {
    val compressionRatio: Float
        get() = if (compressedPayload.isNotEmpty()) originalSize.toFloat() / compressedPayload.size.toFloat() else 1.0f

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is L0Evidence) return false
        if (id != other.id) return false
        if (!compressedPayload.contentEquals(other.compressedPayload)) return false
        if (originalSize != other.originalSize) return false
        if (timestampMs != other.timestampMs) return false
        if (mimeType != other.mimeType) return false
        if (sourceUri != other.sourceUri) return false
        if (metadata != other.metadata) return false
        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + compressedPayload.contentHashCode()
        result = 31 * result + originalSize
        result = 31 * result + timestampMs.hashCode()
        result = 31 * result + mimeType.hashCode()
        result = 31 * result + (sourceUri?.hashCode() ?: 0)
        result = 31 * result + metadata.hashCode()
        return result
    }
}

/**
 * Resolved L0 evidence item decompressed in memory.
 */
data class DecompressedEvidence(
    val evidenceId: String,
    val rawContent: String,
    val rawBytes: ByteArray,
    val mimeType: String,
    val timestampMs: Long,
    val originalSize: Int,
    val compressedSize: Int,
    val sourceUri: String?,
    val metadata: Map<String, String>
) {
    val compressionRatio: Float
        get() = if (compressedSize > 0) originalSize.toFloat() / compressedSize.toFloat() else 1.0f

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DecompressedEvidence) return false
        if (evidenceId != other.evidenceId) return false
        if (rawContent != other.rawContent) return false
        if (!rawBytes.contentEquals(other.rawBytes)) return false
        if (mimeType != other.mimeType) return false
        if (timestampMs != other.timestampMs) return false
        if (originalSize != other.originalSize) return false
        if (compressedSize != other.compressedSize) return false
        if (sourceUri != other.sourceUri) return false
        if (metadata != other.metadata) return false
        return true
    }

    override fun hashCode(): Int {
        var result = evidenceId.hashCode()
        result = 31 * result + rawContent.hashCode()
        result = 31 * result + rawBytes.contentHashCode()
        result = 31 * result + mimeType.hashCode()
        result = 31 * result + timestampMs.hashCode()
        result = 31 * result + originalSize
        result = 31 * result + compressedSize
        result = 31 * result + (sourceUri?.hashCode() ?: 0)
        result = 31 * result + metadata.hashCode()
        return result
    }
}

/**
 * L2 Scenario Cluster descriptor.
 */
data class L2ScenarioCluster(
    val clusterId: String,
    val title: String,
    val memberFactIds: List<String>,
    val centroidEmbedding: FloatArray?,
    val topKeywords: List<String> = emptyList(),
    val createdAtMs: Long = System.currentTimeMillis()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is L2ScenarioCluster) return false
        if (clusterId != other.clusterId) return false
        if (title != other.title) return false
        if (memberFactIds != other.memberFactIds) return false
        if (centroidEmbedding != null) {
            if (other.centroidEmbedding == null) return false
            if (!centroidEmbedding.contentEquals(other.centroidEmbedding)) return false
        } else if (other.centroidEmbedding != null) return false
        if (topKeywords != other.topKeywords) return false
        if (createdAtMs != other.createdAtMs) return false
        return true
    }

    override fun hashCode(): Int {
        var result = clusterId.hashCode()
        result = 31 * result + title.hashCode()
        result = 31 * result + memberFactIds.hashCode()
        result = 31 * result + (centroidEmbedding?.contentHashCode() ?: 0)
        result = 31 * result + topKeywords.hashCode()
        result = 31 * result + createdAtMs.hashCode()
        return result
    }
}

/**
 * Fact enriched with its supporting L0 raw evidence.
 */
data class FactWithEvidence(
    val factId: String,
    val text: String,
    val confidence: Float,
    val properties: Map<String, String>,
    val evidenceList: List<DecompressedEvidence>
)

/**
 * Symbolic working memory drill-down result.
 */
data class DrillDownResult(
    val rootNodeId: String,
    val rootLevel: MemoryLevel,
    val scenarioClusters: List<L2ScenarioCluster> = emptyList(),
    val atomicFacts: List<FactWithEvidence> = emptyList(),
    val totalEvidenceCount: Int = 0
) {
    fun toContextString(): String = buildString {
        appendLine("DrillDown Trace for [$rootNodeId] (Level: $rootLevel):")
        if (scenarioClusters.isNotEmpty()) {
            appendLine("  Scenario Clusters (${scenarioClusters.size}):")
            for (c in scenarioClusters) {
                appendLine("    - Cluster ${c.clusterId}: \"${c.title}\" (${c.memberFactIds.size} facts)")
            }
        }
        appendLine("  Atomic Facts (${atomicFacts.size}):")
        for (f in atomicFacts) {
            appendLine("    - Fact ${f.factId} [conf: ${f.confidence}]: \"${f.text}\"")
            if (f.evidenceList.isNotEmpty()) {
                appendLine("      Supporting Evidence (${f.evidenceList.size}):")
                for (ev in f.evidenceList) {
                    val preview = ev.rawContent.take(80).replace("\n", " ")
                    appendLine("        * Ev ${ev.evidenceId} [${ev.originalSize}B -> ${ev.compressedSize}B]: $preview...")
                }
            }
        }
    }
}
