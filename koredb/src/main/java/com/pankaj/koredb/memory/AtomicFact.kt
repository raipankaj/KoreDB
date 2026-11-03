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

import com.pankaj.koredb.core.KoreIndex
import com.pankaj.koredb.core.KoreNumericIndex
import com.pankaj.koredb.core.KoreSearchable
import com.pankaj.koredb.graph.Node
import kotlinx.serialization.Serializable
import java.util.UUID

/**
 * Reified semantic proposition representing an L1 Atomic Fact in KoreDB's cognitive memory architecture.
 *
 * Tracks both Transaction Time ([ingestedAtMs]) and Validity Time ([validFromMs], [validUntilMs])
 * to support bi-temporal audits, historical reconstruction, and deterministic contradiction invalidation.
 *
 * In addition, tracks ACT-R / Ebbinghaus cognitive decay metadata ([lastAccessedAtMs], [accessCount],
 * [stabilityHours], [importance]) for spaced-repetition consolidation.
 *
 * @property id Unique fact identifier.
 * @property subject Entity subject of the triple (e.g. "Alice").
 * @property predicate Semantic relationship or attribute (e.g. "livesIn").
 * @property object Target entity or scalar value (e.g. "Tokyo").
 * @property text Natural language representation for BM25 and vector indexing.
 * @property certainty Confidence score in [0.0, 1.0].
 * @property ingestedAtMs System transaction timestamp when KoreDB recorded this fact.
 * @property validFromMs Domain timestamp when this fact became true in the real world.
 * @property validUntilMs Domain timestamp when this fact ceased to be true (Long.MAX_VALUE for active).
 * @property lastAccessedAtMs Timestamp of most recent query or retrieval access.
 * @property accessCount Total number of retrievals reinforcing this memory.
 * @property stabilityHours Dynamic Ebbinghaus stability S(m) in hours.
 * @property importance Intrinsic importance / saliency in [0.0, 1.0].
 * @property contradictionKey Key used to detect functional contradictions ("$subject:$predicate").
 * @property supersededByFactId ID of the superseding fact if invalidated.
 * @property properties Extensible property map.
 */
@Serializable
data class AtomicFact(
    val id: String = UUID.randomUUID().toString(),
    @KoreIndex val subject: String,
    @KoreIndex val predicate: String,
    val `object`: String,
    @KoreSearchable val text: String,
    val certainty: Float = 1.0f,
    @KoreNumericIndex val ingestedAtMs: Long = System.currentTimeMillis(),
    @KoreNumericIndex val validFromMs: Long = ingestedAtMs,
    @KoreNumericIndex val validUntilMs: Long = Long.MAX_VALUE,
    @KoreNumericIndex val lastAccessedAtMs: Long = ingestedAtMs,
    val accessCount: Int = 1,
    val stabilityHours: Double = 24.0,
    val importance: Float = 0.5f,
    @KoreIndex val contradictionKey: String = "$subject:$predicate",
    val supersededByFactId: String? = null,
    val properties: Map<String, String> = emptyMap()
) {
    val isActive: Boolean
        get() = validUntilMs == Long.MAX_VALUE

    /**
     * Evaluates domain validity at [timestampMs] within the half-open interval [[validFromMs], [validUntilMs]).
     */
    fun isValidAt(timestampMs: Long): Boolean =
        timestampMs >= validFromMs && timestampMs < validUntilMs

    /**
     * Evaluates 2D bi-temporal validity: was this fact valid in the domain at [validTimeMs],
     * and was the system aware of it as of [asOfTransactionTimeMs]?
     */
    fun isBiTemporallyValidAt(validTimeMs: Long, asOfTransactionTimeMs: Long): Boolean =
        isValidAt(validTimeMs) && ingestedAtMs <= asOfTransactionTimeMs

    /**
     * Serializes this fact into a standard property graph [Node] for storage in [GraphStorage].
     */
    fun toGraphNode(): Node = Node(
        id = "l1:$id",
        labels = setOf("L1_Fact", "Fact"),
        properties = buildMap {
            put("id", id)
            put("subject", subject)
            put("predicate", predicate)
            put("object", `object`)
            put("text", text)
            put("certainty", certainty.toString())
            put("ingestedAtMs", ingestedAtMs.toString())
            put("validFromMs", validFromMs.toString())
            put("validUntilMs", validUntilMs.toString())
            put("lastAccessedAtMs", lastAccessedAtMs.toString())
            put("accessCount", accessCount.toString())
            put("stabilityHours", stabilityHours.toString())
            put("importance", importance.toString())
            put("contradictionKey", contradictionKey)
            put("isActive", isActive.toString())
            supersededByFactId?.let { put("supersededByFactId", it) }
            putAll(properties)
        }
    )

    companion object {
        /**
         * Reconstructs an [AtomicFact] from a graph [Node].
         */
        fun fromGraphNode(node: Node): AtomicFact {
            val factId = if (node.id.startsWith("l1:")) node.id.substring(3) else node.id
            val p = node.properties
            val ingested = p["ingestedAtMs"]?.toLongOrNull() ?: 0L
            val from = p["validFromMs"]?.toLongOrNull() ?: ingested
            val until = p["validUntilMs"]?.toLongOrNull() ?: Long.MAX_VALUE
            val lastAccess = p["lastAccessedAtMs"]?.toLongOrNull() ?: ingested
            val count = p["accessCount"]?.toIntOrNull() ?: 1
            val stability = p["stabilityHours"]?.toDoubleOrNull() ?: 24.0
            val imp = p["importance"]?.toFloatOrNull() ?: 0.5f
            val certaintyVal = p["certainty"]?.toFloatOrNull() ?: 1.0f
            val subj = p["subject"] ?: ""
            val pred = p["predicate"] ?: ""
            val obj = p["object"] ?: ""
            val txt = p["text"] ?: ""
            val key = p["contradictionKey"] ?: "$subj:$pred"
            val supersededBy = p["supersededByFactId"] ?: p["supersededBy"]

            val standardKeys = setOf(
                "id", "subject", "predicate", "object", "text", "certainty",
                "ingestedAtMs", "validFromMs", "validUntilMs", "lastAccessedAtMs",
                "accessCount", "stabilityHours", "importance", "contradictionKey",
                "isActive", "supersededByFactId", "supersededBy"
            )
            val customProps = p.filterKeys { it !in standardKeys }

            return AtomicFact(
                id = p["id"] ?: factId,
                subject = subj,
                predicate = pred,
                `object` = obj,
                text = txt,
                certainty = certaintyVal,
                ingestedAtMs = ingested,
                validFromMs = from,
                validUntilMs = until,
                lastAccessedAtMs = lastAccess,
                accessCount = count,
                stabilityHours = stability,
                importance = imp,
                contradictionKey = key,
                supersededByFactId = supersededBy,
                properties = customProps
            )
        }
    }
}
