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
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import kotlinx.serialization.Serializable
import java.util.Random
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt

/**
 * Shared test contracts, mathematical ground truths, and reflection bridge
 * for the KoreDB 2025–2026 SOTA E2E test suites.
 */
object TestContracts {

    // ========================================================================
    // 1. DATA CONTRACTS (Mirrors PROJECT.md and Survey Specifications)
    // ========================================================================

    @Serializable
    data class E2EAtomicFact(
        val id: String = java.util.UUID.randomUUID().toString(),
        val subject: String,
        val predicate: String,
        val `object`: String,
        val text: String,
        val certainty: Float = 1.0f,
        val ingestedAtMs: Long = System.currentTimeMillis(),
        val validFromMs: Long = ingestedAtMs,
        val validUntilMs: Long = Long.MAX_VALUE,
        val lastAccessedAtMs: Long = ingestedAtMs,
        val accessCount: Int = 1,
        val stabilityHours: Double = 24.0,
        val importance: Float = 0.5f,
        val contradictionKey: String = "$subject:$predicate",
        val supersededByFactId: String? = null,
        val properties: Map<String, String> = emptyMap()
    ) {
        val isActive: Boolean
            get() = validUntilMs == Long.MAX_VALUE

        fun isValidAt(timestampMs: Long): Boolean =
            timestampMs >= validFromMs && timestampMs < validUntilMs

        fun isBiTemporallyValidAt(validTimeMs: Long, asOfTransactionTimeMs: Long): Boolean =
            isValidAt(validTimeMs) && ingestedAtMs <= asOfTransactionTimeMs

        fun toGraphNode(): Node = Node(
            id = "l1:$id",
            labels = setOf("L1_Fact"),
            properties = mapOf(
                "subject" to subject,
                "predicate" to predicate,
                "object" to `object`,
                "text" to text,
                "certainty" to certainty.toString(),
                "ingestedAtMs" to ingestedAtMs.toString(),
                "validFromMs" to validFromMs.toString(),
                "validUntilMs" to validUntilMs.toString(),
                "lastAccessedAtMs" to lastAccessedAtMs.toString(),
                "accessCount" to accessCount.toString(),
                "stabilityHours" to stabilityHours.toString(),
                "importance" to importance.toString(),
                "contradictionKey" to contradictionKey,
                "isActive" to isActive.toString()
            ) + properties
        )
    }

    @Serializable
    data class E2EBiTemporalEdge(
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

        fun isValidAt(timestampMs: Long): Boolean =
            timestampMs >= validFromMs && timestampMs < validUntilMs

        fun toEdge(): Edge {
            val props = HashMap<String, String>(properties.size + 4)
            props.putAll(properties)
            props["_ingestedAtMs"] = ingestedAtMs.toString()
            props["_validFromMs"] = validFromMs.toString()
            props["_validUntilMs"] = validUntilMs.toString()
            props["_isActive"] = isActive.toString()
            return Edge(
                sourceId = sourceId,
                targetId = targetId,
                type = type,
                properties = props
            )
        }

        companion object {
            fun fromEdge(edge: Edge): E2EBiTemporalEdge {
                val ingested = edge.properties["_ingestedAtMs"]?.toLongOrNull() ?: 0L
                val validFrom = edge.properties["_validFromMs"]?.toLongOrNull() ?: ingested
                val validUntil = edge.properties["_validUntilMs"]?.toLongOrNull() ?: Long.MAX_VALUE
                val cleanProps = edge.properties.filterKeys { !it.startsWith("_") }
                return E2EBiTemporalEdge(
                    sourceId = edge.sourceId,
                    targetId = edge.targetId,
                    type = edge.type,
                    ingestedAtMs = ingested,
                    validFromMs = validFrom,
                    validUntilMs = validUntil,
                    properties = cleanProps
                )
            }
        }
    }

    @Serializable
    data class E2EL0Evidence(
        val id: String = java.util.UUID.randomUUID().toString(),
        val originalSize: Int,
        val compressedSize: Int,
        val mimeType: String = "text/plain",
        val timestampMs: Long = System.currentTimeMillis(),
        val sourceUri: String = "local://raw",
        val payload: ByteArray
    ) {
        val compressionRatio: Float
            get() = if (compressedSize > 0) originalSize.toFloat() / compressedSize.toFloat() else 1.0f

        fun toGraphNode(): Node = Node(
            id = "l0:$id",
            labels = setOf("L0_Evidence"),
            properties = mapOf(
                "originalSize" to originalSize.toString(),
                "compressedSize" to compressedSize.toString(),
                "compressionRatio" to compressionRatio.toString(),
                "mimeType" to mimeType,
                "timestampMs" to timestampMs.toString(),
                "sourceUri" to sourceUri
            )
        )
    }

    @Serializable
    data class E2EL3Invariant(
        val id: String,
        val name: String,
        val rule: String,
        val priority: Int = 1,
        val isActive: Boolean = true
    ) {
        fun toGraphNode(): Node = Node(
            id = "l3:inv:$id",
            labels = setOf("L3_Invariant"),
            properties = mapOf(
                "name" to name,
                "rule" to rule,
                "priority" to priority.toString(),
                "isActive" to isActive.toString()
            )
        )
    }

    @Serializable
    data class E2EL3Persona(
        val id: String,
        val name: String,
        val tone: String,
        val coreBeliefs: List<String>
    ) {
        fun toGraphNode(): Node = Node(
            id = "l3:persona:$id",
            labels = setOf("L3_Persona"),
            properties = mapOf(
                "name" to name,
                "tone" to tone,
                "coreBeliefs" to coreBeliefs.joinToString("; ")
            )
        )
    }

    @Serializable
    data class E2EL2ScenarioCluster(
        val id: String,
        val communityId: Int,
        val memberFactIds: List<String>,
        val centroid: FloatArray? = null
    ) {
        fun toGraphNode(): Node = Node(
            id = "l2:cluster_$communityId",
            labels = setOf("L2_Scenario"),
            properties = mapOf(
                "communityId" to communityId.toString(),
                "memberCount" to memberFactIds.size.toString(),
                "memberFactIds" to memberFactIds.joinToString(",")
            )
        )
    }

    // ========================================================================
    // 2. MATHEMATICAL GROUND TRUTHS
    // ========================================================================

    /**
     * Mathematical ground truth for Hub-Damped Personalized PageRank.
     * Computes power iteration with hub damping: W_uv = A_uv / (d_in(v))^beta.
     */
    fun computeHubDampedPageRankGroundTruth(
        nodes: Set<String>,
        outEdges: Map<String, List<Pair<String, Double>>>, // u -> list of (v, weight)
        seeds: Map<String, Double>, // v -> seed score
        dampingFactor: Double = 0.85,
        beta: Double = 0.5,
        maxIterations: Int = 30,
        epsilon: Double = 1e-5
    ): Map<String, Double> {
        val totalSeedScore = seeds.values.sum()
        if (nodes.isEmpty() || totalSeedScore <= 0.0) return emptyMap()

        // 1. Teleportation distribution p_0
        val p0 = nodes.associateWith { (seeds[it] ?: 0.0) / totalSeedScore }

        // 2. In-degree calculation d_in(v)
        val inDegree = HashMap<String, Double>()
        for (u in nodes) {
            val edges = outEdges[u] ?: emptyList()
            for ((v, _) in edges) {
                if (v in nodes) {
                    inDegree[v] = (inDegree[v] ?: 0.0) + 1.0
                }
            }
        }

        // 3. Hub-damped transition matrix T_vu
        // W_uv = A_uv / (d_in(v))^beta
        val transition = HashMap<String, MutableMap<String, Double>>() // u -> (v -> T_vu)
        for (u in nodes) {
            val edges = outEdges[u] ?: emptyList()
            val dampedWeights = edges.filter { it.first in nodes }.map { (v, weight) ->
                val dIn = inDegree[v] ?: 1.0
                val damped = weight / Math.pow(max(1.0, dIn), beta)
                v to damped
            }
            val sumW = dampedWeights.sumOf { it.second }
            if (sumW > 0.0) {
                val row = HashMap<String, Double>()
                for ((v, w) in dampedWeights) {
                    row[v] = w / sumW
                }
                transition[u] = row
            }
        }

        // 4. Power iteration
        var p = p0.toMutableMap()
        for (iter in 0 until maxIterations) {
            val nextP = HashMap<String, Double>()
            // Dangling mass sum
            var danglingMass = 0.0
            for (u in nodes) {
                if (!transition.containsKey(u) || transition[u]!!.isEmpty()) {
                    danglingMass += p[u] ?: 0.0
                }
            }

            for (v in nodes) {
                // Incoming mass
                var incoming = 0.0
                for (u in nodes) {
                    val t = transition[u]?.get(v) ?: 0.0
                    if (t > 0.0) {
                        incoming += (p[u] ?: 0.0) * t
                    }
                }
                val teleport = p0[v] ?: 0.0
                val newVal = dampingFactor * (incoming + danglingMass * teleport) + (1.0 - dampingFactor) * teleport
                nextP[v] = newVal
            }

            // Convergence check
            var delta = 0.0
            for (v in nodes) {
                delta += abs((nextP[v] ?: 0.0) - (p[v] ?: 0.0))
            }
            p = nextP
            if (delta < epsilon) break
        }

        return p
    }

    /**
     * Mathematical ground truth for ACT-R / Ebbinghaus cognitive decay:
     * S(m) = S0 * (1 + alpha * importance) * (1 + beta * ln(1 + n))^gamma
     * R(m, t) = exp(-deltaT / S(m))
     */
    fun computeActRRetentionGroundTruth(
        elapsedHours: Double,
        accessCount: Int,
        importance: Float,
        baseStabilityHours: Double = 24.0,
        alpha: Double = 1.0,
        beta: Double = 0.5,
        gamma: Double = 0.8
    ): Pair<Double, Double> {
        val s = baseStabilityHours * (1.0 + alpha * importance) * Math.pow(1.0 + beta * ln(1.0 + accessCount), gamma)
        val r = if (elapsedHours <= 0.0) 1.0 else exp(-elapsedHours / s)
        return Pair(s, r)
    }

    /**
     * Gram-Schmidt orthogonal matrix generation for Rotated Binary Quantization ground truth.
     */
    fun generateOrthogonalMatrixGroundTruth(dim: Int, seed: Long = 42L): Array<FloatArray> {
        val random = Random(seed)
        val matrix = Array(dim) { FloatArray(dim) }

        // 1. Fill with Gaussian entries
        for (i in 0 until dim) {
            for (j in 0 until dim) {
                matrix[i][j] = random.nextGaussian().toFloat()
            }
        }

        // 2. Gram-Schmidt orthogonalization
        for (i in 0 until dim) {
            for (j in 0 until i) {
                var dot = 0.0f
                for (k in 0 until dim) {
                    dot += matrix[i][k] * matrix[j][k]
                }
                for (k in 0 until dim) {
                    matrix[i][k] -= dot * matrix[j][k]
                }
            }
            var norm = 0.0f
            for (k in 0 until dim) {
                norm += matrix[i][k] * matrix[i][k]
            }
            norm = sqrt(max(1e-12f, norm))
            for (k in 0 until dim) {
                matrix[i][k] /= norm
            }
        }
        return matrix
    }

    /**
     * Rotated Binary Quantization symmetric inner product estimation ground truth:
     * Estimator = c_q * c_x * (PI / 2) * (D - 2 * d_H)
     */
    fun estimateRotatedBinaryInnerProductGroundTruth(
        dim: Int,
        hammingDistance: Int,
        cq: Float,
        cx: Float
    ): Float {
        val factor = (Math.PI / 2.0).toFloat()
        return cq * cx * factor * (dim - 2 * hammingDistance)
    }

    // ========================================================================
    // 3. SYNTHETIC DATA GENERATORS & HELPERS
    // ========================================================================

    fun generateNormalizedVector(dim: Int, random: Random = Random(42L)): FloatArray {
        val vec = FloatArray(dim) { random.nextGaussian().toFloat() }
        var norm = 0.0f
        for (v in vec) norm += v * v
        norm = sqrt(max(1e-12f, norm))
        for (i in 0 until dim) vec[i] /= norm
        return vec
    }

    private val lz4Codec = Lz4CompressionCodec()

    fun compressPayloadWithLz4(payload: ByteArray): ByteArray =
        lz4Codec.compress(payload)

    fun decompressPayloadWithLz4(compressed: ByteArray): ByteArray =
        lz4Codec.decompress(compressed)

    /**
     * Sanitizes strings into valid Mermaid IDs.
     */
    fun sanitizeMermaidId(raw: String): String =
        raw.replace(Regex("[^a-zA-Z0-9_]"), "_")

    // ========================================================================
    // 4. REFLECTION BRIDGE (Forward-compatible execution for M1, M2, M3 classes)
    // ========================================================================

    fun isClassAvailable(className: String): Boolean =
        runCatching { Class.forName(className) }.isSuccess

    fun invokeIfExists(target: Any, methodName: String, vararg args: Any?): Any? {
        val methods = target::class.java.methods
        val matched = methods.firstOrNull { it.name == methodName && it.parameterCount == args.size }
            ?: return null
        return runCatching { matched.invoke(target, *args) }.getOrNull()
    }
}
