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
import java.util.Random
import java.util.UUID
import kotlin.math.abs
import kotlin.math.sqrt

/**
 * Adversarial Challenger Test Suite for Milestone 3 (Memory Pyramid Core, Zero-LLM Clustering & Hybrid Retrieval):
 *
 * 1. Multi-Community Zero-LLM Louvain Partitioning:
 *    Dense multi-cluster graph of L1 facts with cross-community bridges. Verifies that `clusterScenarios`
 *    separates distinct communities cleanly, reifies cluster nodes, and establishes `CLUSTERS_FACT` edges
 *    without cycles or loss of facts.
 *
 * 2. Centroid Embedding Exact Normalization:
 *    Asserts that ||c||_2 == 1.0 +- 10^-5 for each computed scenario cluster centroid, and that cosine similarity
 *    search against member vectors yields monotonically high similarity (> 0.70).
 *
 * 3. Partial & Unembedded Facts Resilience:
 *    Tests scenario clustering when 0 facts have embeddings or only a minority of member facts have embeddings.
 *    Verifies centroid calculation does not divide by zero or crash, and preserves cluster graph topology.
 *
 * 4. Invariant Boundary & Priority Enforcement:
 *    Tests multiple invariants with conflicting priorities (CRITICAL vs LOW). Verifies that `GOVERNS`
 *    relationships correctly point to target scenarios or facts and that priority order is preserved.
 *
 * 5. Hybrid RRF Rank Fusion Consistency:
 *    Stresses `searchFacts` with orthogonal queries where BM25 finds a set of facts and HNSW finds another set.
 *    Verifies that Reciprocal Rank Fusion fuses both ranking streams deterministically, returning facts with
 *    fully hydrated, decompressed L0 evidence.
 */
class M3PyramidChallengerTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase
    private lateinit var pyramid: KoreMemoryPyramid

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_challenger_m3_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
        pyramid = db.memoryPyramid()
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Helper to generate a normalized unit vector of dimension [dim].
     */
    private fun generateUnitVector(dim: Int, seed: Long): FloatArray {
        val rand = Random(seed)
        val vec = FloatArray(dim) { rand.nextGaussian().toFloat() }
        var normSq = 0.0f
        for (v in vec) normSq += v * v
        val norm = sqrt(normSq)
        for (i in 0 until dim) vec[i] /= norm
        return vec
    }

    /**
     * Helper to generate a perturbed vector around a pivot vector with small noise [sigma].
     */
    private fun generatePerturbedVector(pivot: FloatArray, sigma: Float, seed: Long): FloatArray {
        val rand = Random(seed)
        val dim = pivot.size
        val vec = FloatArray(dim) { i -> pivot[i] + (rand.nextGaussian().toFloat() * sigma) }
        var normSq = 0.0f
        for (v in vec) normSq += v * v
        val norm = sqrt(normSq)
        for (i in 0 until dim) vec[i] /= norm
        return vec
    }

    /**
     * Helper to compute cosine similarity (dot product of two unit vectors).
     */
    private fun cosineSimilarity(v1: FloatArray, v2: FloatArray): Float {
        assertEquals("Vector dimensions must match", v1.size, v2.size)
        var dot = 0.0f
        for (i in v1.indices) {
            dot += v1[i] * v2[i]
        }
        return dot
    }

    // ========================================================================
    // CHALLENGE 1: MULTI-COMMUNITY ZERO-LLM LOUVAIN PARTITIONING
    // ========================================================================

    /**
     * Construct a dense multi-cluster graph of L1 facts with cross-community bridges.
     * Verifies that `clusterScenarios` separates distinct communities cleanly, reifies cluster nodes,
     * and establishes `CLUSTERS_FACT` edges without cycles or loss of facts.
     */
    @Test
    fun testMultiCommunityLouvainPartitioningWithCrossBridges() = runBlocking {
        // Community A: Network facts (4 facts: a0, a1, a2, a3)
        val aFacts = (0 until 4).map { i ->
            pyramid.addFact(
                text = "Network service tier node A$i latency probe ok",
                embedding = generateUnitVector(64, 1000L + i),
                properties = mapOf("community" to "network", "index" to i.toString())
            )
        }
        // Dense internal edges for Community A (clique with weight = 8.0)
        for (i in aFacts.indices) {
            for (j in (i + 1) until aFacts.size) {
                pyramid.linkFacts(aFacts[i], aFacts[j], MemoryEdgeTypes.RELATES_TO, weight = 8.0)
            }
        }

        // Community B: Storage facts (4 facts: b0, b1, b2, b3)
        val bFacts = (0 until 4).map { i ->
            pyramid.addFact(
                text = "Storage block tier node B$i NVMe disk capacity ok",
                embedding = generateUnitVector(64, 2000L + i),
                properties = mapOf("community" to "storage", "index" to i.toString())
            )
        }
        // Dense internal edges for Community B (clique with weight = 8.0)
        for (i in bFacts.indices) {
            for (j in (i + 1) until bFacts.size) {
                pyramid.linkFacts(bFacts[i], bFacts[j], MemoryEdgeTypes.RELATES_TO, weight = 8.0)
            }
        }

        // Community C: Auth facts (4 facts: c0, c1, c2, c3)
        val cFacts = (0 until 4).map { i ->
            pyramid.addFact(
                text = "Authentication service node C$i OAuth2 token verification ok",
                embedding = generateUnitVector(64, 3000L + i),
                properties = mapOf("community" to "auth", "index" to i.toString())
            )
        }
        // Dense internal edges for Community C (clique with weight = 8.0)
        for (i in cFacts.indices) {
            for (j in (i + 1) until cFacts.size) {
                pyramid.linkFacts(cFacts[i], cFacts[j], MemoryEdgeTypes.RELATES_TO, weight = 8.0)
            }
        }

        // Cross-community bridges with very low weight (0.05) to test separation
        pyramid.linkFacts(aFacts[3], bFacts[0], MemoryEdgeTypes.RELATES_TO, weight = 0.05)
        pyramid.linkFacts(bFacts[3], cFacts[0], MemoryEdgeTypes.RELATES_TO, weight = 0.05)
        pyramid.linkFacts(cFacts[3], aFacts[0], MemoryEdgeTypes.RELATES_TO, weight = 0.05)

        // Execute Zero-LLM community detection
        val clusters = pyramid.clusterScenarios(iterations = 15, minClusterSize = 2)

        // 1. Clean separation: exactly 3 distinct scenario clusters
        assertEquals("Louvain must discover exactly 3 communities despite cross-bridges", 3, clusters.size)

        val allIngestedFactIds = (aFacts + bFacts + cFacts).toSet()
        val observedFactIds = mutableSetOf<String>()

        val aSet = aFacts.toSet()
        val bSet = bFacts.toSet()
        val cSet = cFacts.toSet()

        for (cluster in clusters) {
            val members = cluster.memberFactIds.toSet()

            // 2. No fact mixing: each cluster must correspond cleanly to one community
            val matchesA = members == aSet
            val matchesB = members == bSet
            val matchesC = members == cSet
            assertTrue(
                "Cluster ${cluster.clusterId} with members $members must match A, B, or C cleanly",
                matchesA || matchesB || matchesC
            )

            // Accumulate to check completeness
            for (m in members) {
                assertFalse("Fact $m must not appear in multiple clusters", observedFactIds.contains(m))
                observedFactIds.add(m)
            }

            // 3. Cluster node reified in GraphStorage
            val clusterNodeId = "l2:cluster_${cluster.clusterId}"
            val clusterNode = pyramid.graph.getNode(clusterNodeId)
            assertNotNull("Cluster node must exist in GraphStorage: $clusterNodeId", clusterNode)
            assertEquals(setOf(MemoryLabels.L2_SCENARIO), clusterNode?.labels)
            assertEquals(cluster.memberFactIds.size.toString(), clusterNode?.properties?.get("memberCount"))
            assertEquals("true", clusterNode?.properties?.get("hasCentroid"))

            // 4. CLUSTERS_FACT outbound edges
            val clusterEdges = pyramid.graph.getOutboundEdges(clusterNodeId, MemoryEdgeTypes.CLUSTERS_FACT)
            assertEquals("Outbound CLUSTERS_FACT edge count must equal member count", members.size, clusterEdges.size)

            val targets = clusterEdges.map { it.targetId.removePrefix("l1:") }.toSet()
            assertEquals("CLUSTERS_FACT targets must exactly match cluster members", members, targets)

            // 5. Acyclic structure verification: No inbound edges from L1 to L2 with CLUSTERS_FACT
            for (m in members) {
                val l1NodeId = "l1:$m"
                val reverseEdges = pyramid.graph.getOutboundEdges(l1NodeId, MemoryEdgeTypes.CLUSTERS_FACT)
                assertTrue("L1 fact must have no outbound CLUSTERS_FACT edges (must be acyclic)", reverseEdges.isEmpty())
            }
        }

        // 6. Partition Completeness (no loss of facts)
        assertEquals("All 12 ingested facts must be accounted for across clusters", allIngestedFactIds, observedFactIds)
    }

    // ========================================================================
    // CHALLENGE 2: CENTROID EMBEDDING EXACT NORMALIZATION & SIMILARITY
    // ========================================================================

    /**
     * For each computed scenario cluster centroid, assert that ||c||_2 == 1.0 +- 10^-5,
     * and cosine similarity search against member vectors yields monotonically high similarity (> 0.70).
     */
    @Test
    fun testCentroidEmbeddingExactNormalizationAndMonotonicHighSimilarity() = runBlocking {
        val dim = 64
        val pivot1 = generateUnitVector(dim, seed = 1111L)
        val pivot2 = generateUnitVector(dim, seed = 2222L)

        // Cluster 1 members: generated tightly around pivot1 (sigma = 0.05)
        val c1Facts = mutableListOf<Pair<String, FloatArray>>()
        for (i in 0 until 5) {
            val vec = generatePerturbedVector(pivot1, sigma = 0.05f, seed = 100L + i)
            val factId = pyramid.addFact("Cluster 1 Fact $i semantic proposition", embedding = vec)
            c1Facts.add(factId to vec)
        }
        for (i in c1Facts.indices) {
            for (j in (i + 1) until c1Facts.size) {
                pyramid.linkFacts(c1Facts[i].first, c1Facts[j].first, MemoryEdgeTypes.RELATES_TO, weight = 5.0)
            }
        }

        // Cluster 2 members: generated tightly around pivot2 (sigma = 0.05)
        val c2Facts = mutableListOf<Pair<String, FloatArray>>()
        for (i in 0 until 5) {
            val vec = generatePerturbedVector(pivot2, sigma = 0.05f, seed = 200L + i)
            val factId = pyramid.addFact("Cluster 2 Fact $i semantic proposition", embedding = vec)
            c2Facts.add(factId to vec)
        }
        for (i in c2Facts.indices) {
            for (j in (i + 1) until c2Facts.size) {
                pyramid.linkFacts(c2Facts[i].first, c2Facts[j].first, MemoryEdgeTypes.RELATES_TO, weight = 5.0)
            }
        }

        val clusters = pyramid.clusterScenarios(iterations = 10, minClusterSize = 2)
        assertEquals(2, clusters.size)

        for (cluster in clusters) {
            val centroid = cluster.centroidEmbedding
            assertNotNull("Centroid embedding must not be null", centroid)
            assertEquals("Centroid dimension must equal vector dimension", dim, centroid!!.size)

            // 1. Strict L2 Normalization Assertion: ||c||_2 == 1.0 +- 10^-5
            var normSq = 0.0
            for (v in centroid) normSq += (v.toDouble() * v.toDouble())
            val l2Norm = sqrt(normSq)
            assertEquals(
                "Centroid ||c||_2 must equal 1.0 within 1e-5 tolerance",
                1.0,
                l2Norm,
                1e-5
            )

            // 2. Monotonically High Cosine Similarity (> 0.70) against all member vectors
            val clusterMemberIds = cluster.memberFactIds.toSet()
            val memberVectors = (c1Facts + c2Facts).filter { it.first in clusterMemberIds }.map { it.second }
            assertEquals(5, memberVectors.size)

            for ((idx, mVec) in memberVectors.withIndex()) {
                val sim = cosineSimilarity(centroid, mVec)
                assertTrue(
                    "Cosine similarity between centroid and member $idx ($sim) must exceed 0.70",
                    sim > 0.70f
                )
                // In practice with sigma = 0.05, similarity is > 0.95
                assertTrue(
                    "High-precision similarity ($sim) expected for tight cluster",
                    sim > 0.90f
                )
            }

            // 3. Vector Collection Search Consistency
            val searchHits = pyramid.vectorCollection.search(centroid, limit = 5)
            assertTrue("HNSW search on centroid must return hits", searchHits.isNotEmpty())
            val topHitId = searchHits[0].first.removePrefix("l1:").removePrefix("l2:cluster_")
            // Either the top hit is one of the members or the centroid itself
            val hitScore = searchHits[0].second
            assertTrue("Top hit score must be monotonically high (> 0.70)", hitScore > 0.70f)
        }

        // Cross-cluster centroid orthogonality: pivot1 and pivot2 are random Gaussian, so dot product is low
        val c1Centroid = clusters[0].centroidEmbedding!!
        val c2Centroid = clusters[1].centroidEmbedding!!
        val crossSim = abs(cosineSimilarity(c1Centroid, c2Centroid))
        assertTrue("Centroids of distinct clusters must be well-separated (crossSim=$crossSim < 0.40)", crossSim < 0.40f)
    }

    // ========================================================================
    // CHALLENGE 3: PARTIAL & UNEMBEDDED FACTS RESILIENCE
    // ========================================================================

    /**
     * Test 3A: When 0 member facts have embeddings, verify centroid calculation does not
     * divide by zero or crash, and preserves the cluster graph topology.
     */
    @Test
    fun testScenarioClusteringWithZeroEmbeddedFactsGracefullyPreservesTopology() = runBlocking {
        // Ingest 5 facts with NO embeddings
        val unembeddedFacts = (0 until 5).map { i ->
            pyramid.addFact(
                text = "Unembedded textual observation $i with no dense vector",
                embedding = null
            )
        }
        // Connect them into a cohesive community
        for (i in unembeddedFacts.indices) {
            for (j in (i + 1) until unembeddedFacts.size) {
                pyramid.linkFacts(unembeddedFacts[i], unembeddedFacts[j], MemoryEdgeTypes.RELATES_TO, weight = 2.0)
            }
        }

        // Must execute cleanly without ArithmeticException or NPE
        val clusters = pyramid.clusterScenarios(iterations = 10, minClusterSize = 2)

        assertEquals("Should form 1 cluster from the 5 interconnected facts", 1, clusters.size)
        val cluster = clusters[0]

        // Centroid must be null (no vectors to average)
        assertNull("Centroid must be null when 0 facts have embeddings", cluster.centroidEmbedding)
        assertEquals("All 5 facts must be members", 5, cluster.memberFactIds.size)
        assertEquals(unembeddedFacts.toSet(), cluster.memberFactIds.toSet())

        // Graph node exists with hasCentroid = false
        val clusterNode = pyramid.graph.getNode("l2:cluster_${cluster.clusterId}")
        assertNotNull(clusterNode)
        assertEquals("false", clusterNode?.properties?.get("hasCentroid"))
        assertEquals("5", clusterNode?.properties?.get("memberCount"))

        // CLUSTERS_FACT edges must still link to all 5 facts
        val edges = pyramid.graph.getOutboundEdges("l2:cluster_${cluster.clusterId}", MemoryEdgeTypes.CLUSTERS_FACT)
        assertEquals(5, edges.size)
        val linkedFacts = edges.map { it.targetId.removePrefix("l1:") }.toSet()
        assertEquals(unembeddedFacts.toSet(), linkedFacts)
    }

    /**
     * Test 3B: When only a minority of member facts have embeddings (e.g. 2 out of 6),
     * verify centroid calculation averages only the available vectors, does not divide
     * by total member count (which would cause under-normalization if not re-normalized),
     * produces ||c||_2 == 1.0, and preserves the cluster graph topology.
     */
    @Test
    fun testScenarioClusteringWithMinorityEmbeddedFactsComputesValidCentroid() = runBlocking {
        val dim = 64
        val vec1 = generateUnitVector(dim, seed = 501L)
        val vec2 = generateUnitVector(dim, seed = 502L)

        // 2 facts with embeddings
        val fEmbed1 = pyramid.addFact("Embedded fact 1", embedding = vec1)
        val fEmbed2 = pyramid.addFact("Embedded fact 2", embedding = vec2)

        // 4 facts WITHOUT embeddings
        val fUnembed = (1..4).map { i ->
            pyramid.addFact("Unembedded companion fact $i", embedding = null)
        }

        val allFacts = listOf(fEmbed1, fEmbed2) + fUnembed

        // Link all 6 facts into a single community
        for (i in allFacts.indices) {
            for (j in (i + 1) until allFacts.size) {
                pyramid.linkFacts(allFacts[i], allFacts[j], MemoryEdgeTypes.RELATES_TO, weight = 3.0)
            }
        }

        val clusters = pyramid.clusterScenarios(iterations = 10, minClusterSize = 2)
        assertEquals(1, clusters.size)
        val cluster = clusters[0]

        assertEquals(6, cluster.memberFactIds.size)
        assertEquals(allFacts.toSet(), cluster.memberFactIds.toSet())

        // Centroid must be computed from the 2 available embeddings
        val centroid = cluster.centroidEmbedding
        assertNotNull("Centroid must be computed from the 2 available vectors", centroid)
        assertEquals(dim, centroid!!.size)

        // L2 norm must be exactly 1.0 +- 1e-5
        var normSq = 0.0
        for (v in centroid) normSq += (v.toDouble() * v.toDouble())
        assertEquals(1.0, sqrt(normSq), 1e-5)

        // Similarity against the two embedded facts must be positive and consistent
        val sim1 = cosineSimilarity(centroid, vec1)
        val sim2 = cosineSimilarity(centroid, vec2)
        assertTrue("Similarity with constituent vec1 must be positive ($sim1)", sim1 > 0.50f)
        assertTrue("Similarity with constituent vec2 must be positive ($sim2)", sim2 > 0.50f)

        // Graph node hasCentroid = true
        val clusterNode = pyramid.graph.getNode("l2:cluster_${cluster.clusterId}")
        assertNotNull(clusterNode)
        assertEquals("true", clusterNode?.properties?.get("hasCentroid"))
        assertEquals("6", clusterNode?.properties?.get("memberCount"))

        // All 6 member facts linked via CLUSTERS_FACT
        val edges = pyramid.graph.getOutboundEdges("l2:cluster_${cluster.clusterId}", MemoryEdgeTypes.CLUSTERS_FACT)
        assertEquals(6, edges.size)
    }

    // ========================================================================
    // CHALLENGE 4: INVARIANT BOUNDARY & PRIORITY ENFORCEMENT
    // ========================================================================

    /**
     * Test multiple invariants with conflicting priorities (CRITICAL vs HIGH vs MEDIUM vs LOW).
     * Verifies that `GOVERNS` relationships correctly point to target scenarios or facts and that
     * priority order is preserved and can be resolved unambiguously.
     */
    @Test
    fun testInvariantBoundaryAndPriorityEnforcementConflictingRules() = runBlocking {
        // Ingest target L1 facts
        val factMedHistory = pyramid.addFact("Patient clinical history: severe penicillin anaphylaxis allergy")
        val factBilling = pyramid.addFact("Patient insurance billing account balance")
        val factTelemetry = pyramid.addFact("Client mobile device display refresh rate 120Hz")

        // Ingest a scenario cluster target
        val dummyFact = pyramid.addFact("Scenario context fact")
        val clusters = pyramid.clusterScenarios(minClusterSize = 1)
        val scenarioNodeId = "l2:cluster_${clusters[0].clusterId}"

        // Register 4 invariants spanning all priority levels with overlapping govern targets:
        val invCrit = pyramid.addInvariant(
            statement = "CRITICAL: Patient medical history must never be exported in cleartext",
            priority = InvariantPriority.CRITICAL,
            governedNodeIds = listOf(factMedHistory, scenarioNodeId)
        )

        val invHigh = pyramid.addInvariant(
            statement = "HIGH: Patient financial accounts require multi-factor authorization",
            priority = InvariantPriority.HIGH,
            governedNodeIds = listOf(factMedHistory, factBilling)
        )

        val invMed = pyramid.addInvariant(
            statement = "MEDIUM: Diagnostic telemetry must be rotated every 7 days",
            priority = InvariantPriority.MEDIUM,
            governedNodeIds = listOf(factBilling, factTelemetry)
        )

        val invLow = pyramid.addInvariant(
            statement = "LOW: Non-critical logs can be sampled at 10%",
            priority = InvariantPriority.LOW,
            governedNodeIds = listOf(factMedHistory, factBilling, factTelemetry)
        )

        // 1. Verify GOVERNS outbound edges from invariants
        val critOutbound = pyramid.graph.getOutboundEdges(invCrit, MemoryEdgeTypes.GOVERNS).map { it.targetId }.toSet()
        assertEquals(setOf("l1:$factMedHistory", scenarioNodeId), critOutbound)

        val highOutbound = pyramid.graph.getOutboundEdges(invHigh, MemoryEdgeTypes.GOVERNS).map { it.targetId }.toSet()
        assertEquals(setOf("l1:$factMedHistory", "l1:$factBilling"), highOutbound)

        val medOutbound = pyramid.graph.getOutboundEdges(invMed, MemoryEdgeTypes.GOVERNS).map { it.targetId }.toSet()
        assertEquals(setOf("l1:$factBilling", "l1:$factTelemetry"), medOutbound)

        val lowOutbound = pyramid.graph.getOutboundEdges(invLow, MemoryEdgeTypes.GOVERNS).map { it.targetId }.toSet()
        assertEquals(setOf("l1:$factMedHistory", "l1:$factBilling", "l1:$factTelemetry"), lowOutbound)

        // 2. Inbound Query & Priority Resolution for factMedHistory (governed by CRITICAL, HIGH, LOW)
        val medHistoryInboundSources = pyramid.graph.getInboundSourceIds("l1:$factMedHistory", MemoryEdgeTypes.GOVERNS)
        assertEquals(3, medHistoryInboundSources.size)
        assertTrue(medHistoryInboundSources.containsAll(listOf(invCrit, invHigh, invLow)))

        // Fetch governing invariant nodes and resolve highest priority
        val medHistoryInvariants = medHistoryInboundSources.mapNotNull { pyramid.graph.getNode(it) }
        val priorities = medHistoryInvariants.map { InvariantPriority.valueOf(it.properties["priority"]!!) }
        val highestPriority = priorities.minByOrNull { it.ordinal } // CRITICAL has ordinal 0
        assertEquals("Highest priority governing medical history must be CRITICAL", InvariantPriority.CRITICAL, highestPriority)

        // 3. Priority Resolution for factBilling (governed by HIGH, MEDIUM, LOW)
        val billingInboundSources = pyramid.graph.getInboundSourceIds("l1:$factBilling", MemoryEdgeTypes.GOVERNS)
        assertEquals(3, billingInboundSources.size)
        assertTrue(billingInboundSources.containsAll(listOf(invHigh, invMed, invLow)))

        val billingInvariants = billingInboundSources.mapNotNull { pyramid.graph.getNode(it) }
        val billingPriorities = billingInvariants.map { InvariantPriority.valueOf(it.properties["priority"]!!) }
        val highestBillingPriority = billingPriorities.minByOrNull { it.ordinal }
        assertEquals("Highest priority governing billing must be HIGH", InvariantPriority.HIGH, highestBillingPriority)

        // 4. Priority Resolution for factTelemetry (governed by MEDIUM, LOW)
        val telemetryInboundSources = pyramid.graph.getInboundSourceIds("l1:$factTelemetry", MemoryEdgeTypes.GOVERNS)
        assertEquals(2, telemetryInboundSources.size)
        assertTrue(telemetryInboundSources.containsAll(listOf(invMed, invLow)))

        val telemetryInvariants = telemetryInboundSources.mapNotNull { pyramid.graph.getNode(it) }
        val telemetryPriorities = telemetryInvariants.map { InvariantPriority.valueOf(it.properties["priority"]!!) }
        val highestTelemetryPriority = telemetryPriorities.minByOrNull { it.ordinal }
        assertEquals("Highest priority governing telemetry must be MEDIUM", InvariantPriority.MEDIUM, highestTelemetryPriority)

        // 5. Scenario Node Governance
        val scenarioInbound = pyramid.graph.getInboundSourceIds(scenarioNodeId, MemoryEdgeTypes.GOVERNS)
        assertEquals(listOf(invCrit), scenarioInbound)
    }

    // ========================================================================
    // CHALLENGE 5: HYBRID RRF RANK FUSION CONSISTENCY & L0 DECOMPRESSION
    // ========================================================================

    /**
     * Stresses `searchFacts` with orthogonal queries where BM25 finds a set of facts and HNSW
     * finds another set. Verifies that Reciprocal Rank Fusion fuses both ranking streams deterministically,
     * returning facts with fully hydrated, decompressed L0 evidence.
     */
    @Test
    fun testHybridRRFRankFusionConsistencyWithOrthogonalStreamsAndL0Decompression() = runBlocking {
        val dim = 64
        val queryVec = generateUnitVector(dim, seed = 777L)

        // Negative vector ensuring f1 has lowest possible vector rank (-1.0 cosine similarity)
        val oppositeVec = FloatArray(dim) { -queryVec[it] }

        // Ingest compressible L0 raw evidence with structured text
        val ev1Raw = buildString {
            appendLine("=== INCIDENT LOG TICKET #8492 ===")
            appendLine("Service: PaymentGatewayService")
            appendLine("Condition: High transaction processing timeout observed during peak load.")
            appendLine("Detail: Latency threshold exceeded 5000ms. Database connection pool saturation.")
            for (i in 1..10) {
                appendLine("Stack trace line $i: com.payment.gateway.TimeoutException at connection pool acquire")
            }
        }
        val ev1Id = pyramid.recordEvidence(
            content = ev1Raw,
            mimeType = "text/plain",
            sourceUri = "audit://gateway/log/8492"
        )

        val ev2Raw = buildString {
            appendLine("=== RECIPE DATABASE ENTRY #102 ===")
            appendLine("Title: Classic Neapolitan Pizza Dough")
            appendLine("Flour: Caputo Tipo 00 500g, Water: 325g (65% hydration), Salt: 15g, Yeast: 1g")
            appendLine("Fermentation: 24h cold ferment followed by 6h room temperature proofing.")
            for (i in 1..8) {
                appendLine("Baking instruction $i: Preheat wood-fired oven to 450 degrees Celsius for optimal crust.")
            }
        }
        val ev2Id = pyramid.recordEvidence(
            content = ev2Raw,
            mimeType = "text/plain",
            sourceUri = "wiki://culinary/pizza"
        )

        val ev3Raw = buildString {
            appendLine("=== TELEMETRY SENSOR REPORT #554 ===")
            appendLine("Metric: PaymentGatewayService transaction latency and database connection pool saturation.")
            appendLine("Severity: High transaction processing timeout anomaly detected.")
            for (i in 1..10) {
                appendLine("Sensor telemetry repeat $i: Connection pool acquire timeout at 5020ms")
            }
        }
        val ev3Id = pyramid.recordEvidence(
            content = ev3Raw,
            mimeType = "text/plain",
            sourceUri = "metrics://payment/sensor/554"
        )

        val ev4Raw = "Random unrelated astrophysics observation about exoplanet spectroscopic atmospheric signatures."
        val ev4Id = pyramid.recordEvidence(
            content = ev4Raw,
            mimeType = "text/plain",
            sourceUri = "science://jwst/exoplanet"
        )

        // Fact 1: BM25 Match ONLY
        // Text contains exact query keywords: "PaymentGatewayService transaction processing timeout"
        // Fact 1: BM25 Match ONLY
        // Text contains query keywords: "PaymentGatewayService transaction processing timeout alert triggered secondary"
        // Vector is opposite to queryVec (cosine sim == -1.0)
        val f1Id = pyramid.addFact(
            text = "PaymentGatewayService transaction processing timeout alert triggered secondary",
            evidenceIds = listOf(ev1Id),
            embedding = oppositeVec
        )

        // Fact 2: HNSW Match ONLY
        // Text is completely disjoint: "Classic Neapolitan Pizza Dough fermentation"
        // Vector is identical to queryVec (cosine sim == 1.0)
        val f2Id = pyramid.addFact(
            text = "Classic Neapolitan Pizza Dough fermentation time and hydration",
            evidenceIds = listOf(ev2Id),
            embedding = queryVec
        )

        // Fact 3: DUAL MATCH (Both BM25 AND HNSW)
        // Text is concise exact match: "PaymentGatewayService transaction processing timeout"
        // Vector is identical to queryVec (cosine sim == 1.0)
        val f3Id = pyramid.addFact(
            text = "PaymentGatewayService transaction processing timeout",
            evidenceIds = listOf(ev3Id),
            embedding = queryVec
        )

        // Fact 4: DISTRACTOR (Neither BM25 nor HNSW)
        val f4Id = pyramid.addFact(
            text = "Astrophysics spectroscopic atmospheric observation",
            evidenceIds = listOf(ev4Id),
            embedding = generateUnitVector(dim, seed = 333L)
        )

        // Execute hybrid search: BM25 on text + HNSW on queryVector
        val results = pyramid.searchFacts(
            query = "PaymentGatewayService transaction processing timeout",
            queryVector = queryVec,
            limit = 4
        )
        // 1. Assert Results Non-Empty
        assertTrue("Hybrid search must return hits", results.isNotEmpty())

        // 2. Deterministic RRF Fusion: Fact 3 (Dual Match) MUST be ranked #1
        // Because Fact 3 receives rank points from BOTH BM25 (rank ~1) and HNSW (rank ~1):
        // RRF(f3) = 1/(60+1) + 1/(60+1) ~ 0.0328
        // Whereas Fact 1 only has BM25 (~0.0164) and Fact 2 only has HNSW (~0.0164)
        assertEquals("Dual matching Fact 3 must be ranked #1 by RRF fusion", f3Id, results[0].factId)

        // 3. Inclusiveness of heterogeneous retrieval: Both Fact 1 (BM25 only) and Fact 2 (HNSW only)
        // must be present in the top-3 results, outranking the distractor Fact 4
        val resultFactIds = results.map { it.factId }
        assertTrue("Fact 1 (BM25 only) must be retrieved in hybrid results", resultFactIds.contains(f1Id))
        assertTrue("Fact 2 (HNSW only) must be retrieved in hybrid results", resultFactIds.contains(f2Id))

        // 4. L0 Raw Evidence Decompression & Hydration Verification
        for (hit in results) {
            assertTrue("Every retrieved fact must have supporting evidence", hit.evidenceList.isNotEmpty())
            val evidence = hit.evidenceList[0]

            when (hit.factId) {
                f3Id -> {
                    assertEquals(ev3Id, evidence.evidenceId)
                    assertEquals("text/plain", evidence.mimeType)
                    assertEquals("metrics://payment/sensor/554", evidence.sourceUri)
                    assertEquals(ev3Raw, evidence.rawContent)
                    assertTrue("Compression ratio must be > 1.0 for repetitive telemetry", evidence.compressionRatio > 1.0f)
                    assertTrue(evidence.compressedSize < evidence.originalSize)
                    assertArrayEquals(ev3Raw.toByteArray(Charsets.UTF_8), evidence.rawBytes)
                }
                f1Id -> {
                    assertEquals(ev1Id, evidence.evidenceId)
                    assertEquals(ev1Raw, evidence.rawContent)
                    assertTrue(evidence.compressionRatio > 1.0f)
                }
                f2Id -> {
                    assertEquals(ev2Id, evidence.evidenceId)
                    assertEquals(ev2Raw, evidence.rawContent)
                    assertTrue(evidence.compressionRatio > 1.0f)
                }
            }
        }
    }

    // ========================================================================
    // CHALLENGE 6: HIGH-DENSITY CLUSTERING SCALE & BOUNDARY STRESS
    // ========================================================================

    /**
     * Stresses zero-LLM Louvain clustering with a larger graph (30 facts across 3 dense cliques)
     * and ensures stable termination without memory or stack overflow.
     */
    @Test
    fun testHighDensityLouvainClusteringScalability() = runBlocking {
        val factsPerCluster = 10
        val clusterCount = 3
        val totalFacts = factsPerCluster * clusterCount

        val allFacts = mutableListOf<String>()

        for (c in 0 until clusterCount) {
            val clusterMemberIds = mutableListOf<String>()
            for (i in 0 until factsPerCluster) {
                val fId = pyramid.addFact(
                    text = "High-density stress cluster $c fact $i proposition",
                    embedding = generateUnitVector(64, seed = (c * 1000L) + i)
                )
                clusterMemberIds.add(fId)
                allFacts.add(fId)
            }
            // Form dense clique
            for (i in clusterMemberIds.indices) {
                for (j in (i + 1) until clusterMemberIds.size) {
                    pyramid.linkFacts(clusterMemberIds[i], clusterMemberIds[j], MemoryEdgeTypes.RELATES_TO, weight = 10.0)
                }
            }
        }

        // Add 5 random sparse bridge edges
        val rand = Random(42L)
        for (step in 0 until 5) {
            val src = allFacts[rand.nextInt(allFacts.size)]
            val tgt = allFacts[rand.nextInt(allFacts.size)]
            if (src != tgt) {
                pyramid.linkFacts(src, tgt, MemoryEdgeTypes.RELATES_TO, weight = 0.01)
            }
        }

        val clusters = pyramid.clusterScenarios(iterations = 20, minClusterSize = 2)

        assertTrue("Should detect major communities (at least 3)", clusters.size >= 3)
        var totalClusteredFacts = 0
        for (cl in clusters) {
            totalClusteredFacts += cl.memberFactIds.size
            assertNotNull(cl.centroidEmbedding)
            var normSq = 0.0
            for (v in cl.centroidEmbedding!!) normSq += (v.toDouble() * v.toDouble())
            assertEquals(1.0, sqrt(normSq), 1e-5)
        }
        assertEquals("All 30 facts should be assigned to clusters", totalFacts, totalClusteredFacts)
    }

    /**
     * Boundary condition: empty graph and single-isolated-fact handling.
     */
    @Test
    fun testClusterScenariosBoundaryConditions() = runBlocking {
        // Empty graph
        val emptyClusters = pyramid.clusterScenarios()
        assertTrue(emptyClusters.isEmpty())

        // Single isolated fact with minClusterSize = 2
        val singleFactId = pyramid.addFact("Single lonely fact")
        val clustersMin2 = pyramid.clusterScenarios(minClusterSize = 2)
        assertTrue("Single fact should be omitted when minClusterSize = 2", clustersMin2.isEmpty())

        // Single isolated fact with minClusterSize = 1
        val clustersMin1 = pyramid.clusterScenarios(minClusterSize = 1)
        assertEquals(1, clustersMin1.size)
        assertEquals(listOf(singleFactId), clustersMin1[0].memberFactIds)
    }
}
