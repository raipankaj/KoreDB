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

package com.pankaj.koredb.graph

import kotlinx.serialization.Serializable

/**
 * Represents a bi-temporal directed relationship between two entities in the graph.
 *
 * Tracks both Transaction Time ([ingestedAtMs]) and Validity Time ([validFromMs], [validUntilMs]).
 * Serializes transparently into standard [Edge] properties with `_`-prefixed keys, ensuring
 * seamless backward compatibility with all underlying [GraphStorage] indices and LSM operations.
 *
 * @property sourceId The originating node identifier.
 * @property targetId The destination node identifier.
 * @property type The relationship type (e.g. "LIVES_IN", "SUPERSEDES").
 * @property ingestedAtMs System transaction timestamp when the relationship was recorded.
 * @property validFromMs Domain timestamp when the relationship became effective.
 * @property validUntilMs Domain timestamp when the relationship ceased to be effective.
 * @property properties User metadata properties on this relationship.
 */
@Serializable
data class BiTemporalEdge(
    val sourceId: String,
    val targetId: String,
    val type: String,
    val ingestedAtMs: Long = System.currentTimeMillis(),
    val validFromMs: Long = ingestedAtMs,
    val validUntilMs: Long = Long.MAX_VALUE,
    val properties: Map<String, String> = emptyMap()
) {
    val isActive: Boolean
        get() = validUntilMs == Long.MAX_VALUE

    /**
     * Checks if this relationship was active in the real world at [timestampMs]
     * within the half-open interval [[validFromMs], [validUntilMs]).
     */
    fun isValidAt(timestampMs: Long): Boolean =
        timestampMs >= validFromMs && timestampMs < validUntilMs

    /**
     * Checks if this relationship is valid in 2D bi-temporal space:
     * valid in domain at [validTimeMs] and recorded in system by [asOfTransactionTimeMs].
     */
    fun isBiTemporallyValidAt(validTimeMs: Long, asOfTransactionTimeMs: Long): Boolean =
        isValidAt(validTimeMs) && ingestedAtMs <= asOfTransactionTimeMs

    /**
     * Converts this bi-temporal edge into an underlying [Edge], encoding temporal
     * metadata into reserved property keys.
     */
    fun toEdge(): Edge {
        val mergedProps = HashMap<String, String>(properties.size + 4)
        mergedProps.putAll(properties)
        mergedProps[PROP_INGESTED_AT] = ingestedAtMs.toString()
        mergedProps[PROP_VALID_FROM] = validFromMs.toString()
        mergedProps[PROP_VALID_UNTIL] = validUntilMs.toString()
        mergedProps[PROP_IS_ACTIVE] = isActive.toString()
        return Edge(sourceId, targetId, type, mergedProps)
    }

    companion object {
        const val PROP_INGESTED_AT = "_ingestedAtMs"
        const val PROP_VALID_FROM = "_validFromMs"
        const val PROP_VALID_UNTIL = "_validUntilMs"
        const val PROP_IS_ACTIVE = "_isActive"

        const val TYPE_SUPERSEDES = "SUPERSEDES"
        const val TYPE_SUPERSEDED_BY = "SUPERSEDED_BY"

        /**
         * Reconstructs a [BiTemporalEdge] from a standard [Edge], extracting temporal
         * metadata from reserved property keys.
         */
        fun fromEdge(edge: Edge): BiTemporalEdge {
            val ingested = edge.properties[PROP_INGESTED_AT]?.toLongOrNull() ?: 0L
            val from = edge.properties[PROP_VALID_FROM]?.toLongOrNull() ?: ingested
            val until = edge.properties[PROP_VALID_UNTIL]?.toLongOrNull() ?: Long.MAX_VALUE
            val cleanProps = edge.properties.filterKeys { !it.startsWith("_") }
            return BiTemporalEdge(
                sourceId = edge.sourceId,
                targetId = edge.targetId,
                type = edge.type,
                ingestedAtMs = ingested,
                validFromMs = from,
                validUntilMs = until,
                properties = cleanProps
            )
        }
    }
}
