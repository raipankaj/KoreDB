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

import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.core.KoreCollection
import com.pankaj.koredb.core.KoreVectorCollection
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.fts.FtsIndex
import com.pankaj.koredb.fts.ReciprocalRankFusion
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import java.util.UUID
import kotlin.math.sqrt

/**
 * 4-Layer Cognitive Memory Pyramid (L0–L3) with Symbolic Working Memory.
 *
 * Layers:
 * - L0: Raw Evidence (LZ4-compressed payloads in [KoreCollection], mirror nodes in [GraphStorage], linked via [MemoryEdgeTypes.EVIDENCE_FOR])
 * - L1: Atomic Facts (Semantic propositions dual-indexed in BM25 [FtsIndex] and HNSW [KoreVectorCollection])
 * - L2: Scenario Clusters (Zero-LLM Louvain community detection via [GraphAlgorithms.detectCommunities] + normalized centroid embeddings)
 * - L3: Persona & Invariants (Identity profiles and inviolable behavioral constraints linked via [MemoryEdgeTypes.GOVERNS])
 */
class KoreMemoryPyramid(
    val db: KoreDatabase,
    val graph: GraphStorage = db.graph(),
    val evidenceCollection: KoreCollection<L0Evidence> = db.binaryCollection("koredb_memory_l0_evidence"),
    val vectorCollection: KoreVectorCollection = db.vectorCollection("koredb_memory_vectors") {
        dimensions = 0 // Adaptive
        metric = com.pankaj.koredb.hnsw.DistanceMetric.COSINE
    },
    val ftsIndex: FtsIndex = FtsIndex("koredb_memory_facts", db.engine),
    val lz4Codec: Lz4CompressionCodec = Lz4CompressionCodec()
) {

    // --- L0 RAW EVIDENCE INGESTION ---

    /**
     * Ingests a raw evidence string into L0 storage with pure-Kotlin LZ4 compression.
     * Persists compressed payload in [evidenceCollection] and creates a mirror node in [graph].
     *
     * @return The unique evidence identifier.
     */
    suspend fun recordEvidence(
        content: String,
        mimeType: String = "text/plain",
        sourceUri: String? = null,
        metadata: Map<String, String> = emptyMap(),
        evidenceId: String = "ev_" + UUID.randomUUID().toString().replace("-", "")
    ): String {
        val cleanId = evidenceId.removePrefix("l0:")
        val rawBytes = content.toByteArray(Charsets.UTF_8)
        val originalSize = rawBytes.size
        val compressed = lz4Codec.compress(rawBytes)
        val now = System.currentTimeMillis()

        val evidence = L0Evidence(
            id = cleanId,
            compressedPayload = compressed,
            originalSize = originalSize,
            timestampMs = now,
            mimeType = mimeType,
            sourceUri = sourceUri,
            metadata = metadata
        )
        evidenceCollection.insert(cleanId, evidence)

        val ratio = if (compressed.isNotEmpty()) originalSize.toFloat() / compressed.size.toFloat() else 1.0f

        val nodeProps = mutableMapOf(
            "id" to cleanId,
            "evidenceId" to cleanId,
            "originalSize" to originalSize.toString(),
            "compressedSize" to compressed.size.toString(),
            "compressionRatio" to ratio.toString(),
            "timestampMs" to now.toString(),
            "mimeType" to mimeType,
            "sourceUri" to (sourceUri ?: "local://raw"),
            "summary" to content.take(120).replace("\n", " ")
        )
        nodeProps.putAll(metadata)

        val node = Node(
            id = "l0:$cleanId",
            labels = setOf(MemoryLabels.L0_EVIDENCE),
            properties = nodeProps
        )
        graph.putNode(node)
        return cleanId
    }

    /**
     * Retrieves and decompresses an L0 evidence item by ID.
     */
    suspend fun getDecompressedEvidence(evidenceId: String): DecompressedEvidence? {
        val cleanId = evidenceId.removePrefix("l0:")
        val ev = evidenceCollection.getById(cleanId) ?: return null
        val decompressedBytes = lz4Codec.decompress(ev.compressedPayload)
        return DecompressedEvidence(
            evidenceId = ev.id,
            rawContent = String(decompressedBytes, Charsets.UTF_8),
            rawBytes = decompressedBytes,
            mimeType = ev.mimeType,
            timestampMs = ev.timestampMs,
            originalSize = ev.originalSize,
            compressedSize = ev.compressedPayload.size,
            sourceUri = ev.sourceUri,
            metadata = ev.metadata
        )
    }

    // --- L1 ATOMIC FACT INGESTION ---

    /**
     * Adds an atomic fact reified in the knowledge graph and dual-indexed in BM25 and HNSW.
     * Optionally links to supporting L0 evidence items via [MemoryEdgeTypes.EVIDENCE_FOR].
     *
     * @return The unique fact identifier.
     */
    suspend fun addFact(
        text: String,
        evidenceIds: List<String> = emptyList(),
        embedding: FloatArray? = null,
        confidence: Float = 1.0f,
        subject: String? = null,
        predicate: String? = null,
        `object`: String? = null,
        properties: Map<String, String> = emptyMap(),
        factId: String = "fact_" + UUID.randomUUID().toString().replace("-", ""),
        detectContradictions: Boolean = false,
        nowMs: Long = System.currentTimeMillis()
    ): String {
        val cleanFactId = factId.removePrefix("l1:")
        val l1NodeId = "l1:$cleanFactId"

        val nodeProps = mutableMapOf(
            "id" to cleanFactId,
            "factId" to cleanFactId,
            "text" to text,
            "confidence" to confidence.toString(),
            "certainty" to confidence.toString(),
            "status" to "ACTIVE",
            "isActive" to "true",
            "ingestedAtMs" to nowMs.toString(),
            "validFromMs" to nowMs.toString(),
            "validUntilMs" to Long.MAX_VALUE.toString(),
            "lastAccessedAtMs" to nowMs.toString(),
            "accessCount" to "1",
            "stabilityHours" to "24.0",
            "importance" to "0.5"
        )
        if (subject != null) nodeProps["subject"] = subject
        if (predicate != null) nodeProps["predicate"] = predicate
        if (`object` != null) nodeProps["object"] = `object`
        if (subject != null && predicate != null) {
            nodeProps["contradictionKey"] = "$subject:$predicate"
        }
        nodeProps.putAll(properties)

        if (subject != null && predicate != null && `object` != null && detectContradictions) {
            val atomicFact = AtomicFact(
                id = cleanFactId,
                subject = subject,
                predicate = predicate,
                `object` = `object`,
                text = text,
                certainty = confidence,
                ingestedAtMs = nowMs,
                validFromMs = nowMs,
                validUntilMs = Long.MAX_VALUE,
                lastAccessedAtMs = nowMs,
                accessCount = nodeProps["accessCount"]?.toIntOrNull() ?: 1,
                stabilityHours = nodeProps["stabilityHours"]?.toDoubleOrNull() ?: 24.0,
                importance = nodeProps["importance"]?.toFloatOrNull() ?: 0.5f,
                contradictionKey = nodeProps["contradictionKey"] ?: "$subject:$predicate",
                properties = nodeProps
            )
            graph.putFact(atomicFact, detectContradictions = true, nowMs = nowMs)
        } else {
            val node = Node(
                id = l1NodeId,
                labels = setOf(MemoryLabels.L1_FACT),
                properties = nodeProps
            )
            graph.putNode(node)
        }

        // BM25 Full-Text Indexing
        ftsIndex.indexDocument(l1NodeId, text)
        if (l1NodeId != cleanFactId) {
            ftsIndex.indexDocument(cleanFactId, text)
        }

        // HNSW Dense Vector Indexing
        if (embedding != null) {
            val vecMeta = mutableMapOf<String, Any>(
                "level" to "L1",
                "factId" to cleanFactId,
                "text" to text,
                "status" to "ACTIVE"
            )
            vectorCollection.insert(l1NodeId, embedding, vecMeta)
        }

        // Link L0 Evidence -> L1 Fact via EVIDENCE_FOR
        for (evId in evidenceIds) {
            val cleanEvId = evId.removePrefix("l0:")
            val edge = Edge(
                sourceId = "l0:$cleanEvId",
                targetId = l1NodeId,
                type = MemoryEdgeTypes.EVIDENCE_FOR,
                properties = mapOf("confidence" to confidence.toString(), "timestampMs" to nowMs.toString())
            )
            graph.putEdge(edge)
        }

        return cleanFactId
    }

    /**
     * Creates a semantic relationship edge between two L1 atomic facts.
     */
    suspend fun linkFacts(
        sourceFactId: String,
        targetFactId: String,
        relationType: String = MemoryEdgeTypes.RELATES_TO,
        weight: Double = 1.0,
        properties: Map<String, String> = emptyMap()
    ) {
        val sId = if (sourceFactId.startsWith("l1:")) sourceFactId else "l1:$sourceFactId"
        val tId = if (targetFactId.startsWith("l1:")) targetFactId else "l1:$targetFactId"
        val edge = Edge(
            sourceId = sId,
            targetId = tId,
            type = relationType,
            properties = properties + mapOf("weight" to weight.toString())
        )
        graph.putEdge(edge)
    }

    // --- L2 SCENARIO CLUSTERING (ZERO-LLM) ---

    /**
     * Clusters L1 atomic facts into cohesive L2 scenario clusters using zero-LLM Louvain
     * community detection, calculates normalized centroid embeddings, and links clusters.
     */
    suspend fun clusterScenarios(
        iterations: Int = 10,
        edgeTypes: Collection<String>? = null,
        minClusterSize: Int = 1
    ): List<L2ScenarioCluster> {
        val l1Nodes = graph.getNodesByLabel(MemoryLabels.L1_FACT).map { it.id }
        if (l1Nodes.isEmpty()) return emptyList()

        // 1. Zero-LLM community detection
        val communities = GraphAlgorithms.detectCommunities(
            storage = graph,
            nodeIds = l1Nodes,
            edgeTypes = edgeTypes,
            iterations = iterations
        )

        val grouped = l1Nodes.groupBy { communities[it] ?: 0 }
        val results = mutableListOf<L2ScenarioCluster>()

        for ((commId, memberIds) in grouped) {
            if (memberIds.size < minClusterSize) continue
            val clusterNodeId = "l2:cluster_$commId"

            // 2. Fetch member embeddings and compute centroid
            val vecMap = vectorCollection.getBatchVectors(memberIds)
            val centroid: FloatArray? = if (vecMap.isNotEmpty()) {
                val dim = vecMap.values.first().size
                val sum = FloatArray(dim)
                for (v in vecMap.values) {
                    for (i in 0 until dim) sum[i] += v[i]
                }
                val count = vecMap.size.toFloat()
                for (i in 0 until dim) sum[i] /= count

                // L2 normalization for Cosine Metric
                var normSq = 0.0f
                for (i in 0 until dim) normSq += sum[i] * sum[i]
                val norm = sqrt(normSq)
                if (norm > 1e-6f) {
                    for (i in 0 until dim) sum[i] /= norm
                }
                sum
            } else null

            // 3. Persist Centroid Vector in HNSW
            if (centroid != null) {
                vectorCollection.insert(
                    id = clusterNodeId,
                    vector = centroid,
                    metadata = mapOf("level" to "L2", "clusterId" to commId.toString(), "size" to memberIds.size)
                )
            }

            // 4. Derive descriptive scenario title from top member fact
            val memberNodes = graph.getNodes(memberIds)
            val topFactText = memberNodes.values.firstOrNull()?.properties?.get("text") ?: "Scenario $commId"
            val title = if (topFactText.length > 50) topFactText.take(47) + "..." else topFactText

            // 5. Persist L2 Scenario Node in GraphStorage
            val clusterNode = Node(
                id = clusterNodeId,
                labels = setOf(MemoryLabels.L2_SCENARIO),
                properties = mapOf(
                    "clusterId" to commId.toString(),
                    "communityId" to commId.toString(),
                    "title" to title,
                    "memberCount" to memberIds.size.toString(),
                    "hasCentroid" to (centroid != null).toString(),
                    "createdAtMs" to System.currentTimeMillis().toString()
                )
            )
            graph.putNode(clusterNode)

            // 6. Connect L2 Cluster -> L1 Facts via CLUSTERS_FACT
            for (mId in memberIds) {
                graph.putEdge(
                    Edge(
                        sourceId = clusterNodeId,
                        targetId = mId,
                        type = MemoryEdgeTypes.CLUSTERS_FACT
                    )
                )
            }

            val rawFactIds = memberIds.map { it.removePrefix("l1:") }
            results.add(
                L2ScenarioCluster(
                    clusterId = commId.toString(),
                    title = title,
                    memberFactIds = rawFactIds,
                    centroidEmbedding = centroid
                )
            )
        }

        return results
    }

    // --- L3 PERSONA & INVARIANTS ---

    /**
     * Establishes the top-level system or agent persona.
     */
    suspend fun setPersona(
        name: String,
        traits: List<String>,
        bio: String,
        personaId: String = "l3:persona_system"
    ): String {
        val pId = if (personaId.startsWith("l3:")) personaId else "l3:persona:$personaId"
        val node = Node(
            id = pId,
            labels = setOf(MemoryLabels.L3_PERSONA),
            properties = mapOf(
                "name" to name,
                "traits" to traits.joinToString(", "),
                "bio" to bio,
                "type" to "PERSONA",
                "updatedAtMs" to System.currentTimeMillis().toString()
            )
        )
        graph.putNode(node)
        return pId
    }

    /**
     * Registers an inviolable behavioral invariant governing the hierarchy.
     */
    suspend fun addInvariant(
        statement: String,
        priority: InvariantPriority = InvariantPriority.CRITICAL,
        scope: String = "GLOBAL",
        governedNodeIds: List<String> = emptyList(),
        invariantId: String = "l3:inv_" + UUID.randomUUID().toString().replace("-", "")
    ): String {
        val invId = if (invariantId.startsWith("l3:")) invariantId else "l3:inv:$invariantId"
        val node = Node(
            id = invId,
            labels = setOf(MemoryLabels.L3_INVARIANT),
            properties = mapOf(
                "name" to statement.take(40),
                "rule" to statement,
                "statement" to statement,
                "priority" to priority.name,
                "scope" to scope,
                "type" to "INVARIANT",
                "isActive" to "true",
                "createdAtMs" to System.currentTimeMillis().toString()
            )
        )
        graph.putNode(node)

        for (targetId in governedNodeIds) {
            val tId = if (targetId.startsWith("l1:") || targetId.startsWith("l2:") || targetId.startsWith("l3:")) {
                targetId
            } else {
                "l1:$targetId"
            }
            graph.putEdge(
                Edge(
                    sourceId = invId,
                    targetId = tId,
                    type = MemoryEdgeTypes.GOVERNS
                )
            )
        }
        return invId
    }

    // --- RETRIEVAL & HYBRID SEARCH ---

    /**
     * Searches atomic facts combining BM25 keyword rankings and dense vector similarity
     * via Reciprocal Rank Fusion, returning facts enriched with supporting L0 evidence.
     */
    suspend fun searchFacts(
        query: String,
        queryVector: FloatArray? = null,
        limit: Int = 10,
        activeOnly: Boolean = true
    ): List<FactWithEvidence> {
        val overfetch = (limit * 6).coerceAtLeast(24)
        val ftsResults = ftsIndex.search(query, overfetch)
        val vecResults = if (queryVector != null) {
            vectorCollection.search(queryVector, overfetch) {
                where("level", com.pankaj.koredb.hnsw.eq("L1"))
            }
        } else emptyList()

        val normFts = ftsResults.map { (id, score) ->
            (if (id.startsWith("l1:")) id else "l1:$id") to score
        }.distinctBy { it.first }

        val normVec = vecResults.map { (id, score) ->
            (if (id.startsWith("l1:")) id else "l1:$id") to score
        }.distinctBy { it.first }

        val fusedIds = if (normFts.isNotEmpty() && normVec.isNotEmpty()) {
            ReciprocalRankFusion.fuse(normFts, normVec, limit = overfetch, k = 60).map { it.first }
        } else if (normFts.isNotEmpty()) {
            normFts.take(overfetch).map { it.first }
        } else if (normVec.isNotEmpty()) {
            normVec.take(overfetch).map { it.first }
        } else emptyList()

        val facts = mutableListOf<FactWithEvidence>()
        for (nodeId in fusedIds) {
            val factNode = graph.getNode(nodeId) ?: continue
            if (activeOnly) {
                val isNodeActive = factNode.properties["isActive"] != "false" &&
                    factNode.properties[com.pankaj.koredb.graph.BiTemporalEdge.PROP_IS_ACTIVE] != "false"
                if (!isNodeActive) continue
            }
            val factId = factNode.properties["factId"] ?: factNode.properties["id"] ?: nodeId.removePrefix("l1:")
            val text = factNode.properties["text"] ?: ""
            val conf = factNode.properties["confidence"]?.toFloatOrNull()
                ?: factNode.properties["certainty"]?.toFloatOrNull()
                ?: 1.0f

            val evSourceIds = graph.getInboundSourceIds(nodeId, MemoryEdgeTypes.EVIDENCE_FOR)
            val decompressedEv = evSourceIds.mapNotNull { evNodeId ->
                val rawEvId = evNodeId.removePrefix("l0:")
                getDecompressedEvidence(rawEvId)
            }
            facts.add(FactWithEvidence(factId, text, conf, factNode.properties, decompressedEv))
            if (facts.size >= limit) break
        }
        return facts
    }

    /**
     * Navigates down from any hierarchy node (L3 persona/invariant, L2 scenario, L1 fact)
     * to its underlying atomic facts and decompressed L0 evidence.
     */
    suspend fun drillDown(rootNodeId: String, maxEvidencePerFact: Int = 5): DrillDownResult {
        return db.graphVectorBridge(vectorCollection).drillDown(
            rootNodeId = rootNodeId,
            maxEvidencePerFact = maxEvidencePerFact,
            pyramid = this
        )
    }
}
