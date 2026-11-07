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

import com.pankaj.koredb.bridge.GraphVectorBridge
import com.pankaj.koredb.db.KoreDatabase
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
import kotlin.math.abs

class HubDampedPersonalizedPageRankTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_hub_damped_ppr_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun testHubDampingSuppressesGenericHubs() = runBlocking {
        val graph = db.graph()

        // Construct graph:
        // Chain: Seed -> Step1 -> Step2 -> Target
        // MegaHub: connected to 30 generic nodes, and Seed also connects to MegaHub
        graph.transaction {
            putNode(Node("Seed"))
            putNode(Node("Step1"))
            putNode(Node("Step2"))
            putNode(Node("Target"))
            putNode(Node("MegaHub"))

            putEdge(Edge("Seed", "Step1", "ASSOCIATED_WITH"))
            putEdge(Edge("Step1", "Step2", "ASSOCIATED_WITH"))
            putEdge(Edge("Step2", "Target", "ASSOCIATED_WITH"))

            putEdge(Edge("Seed", "MegaHub", "ASSOCIATED_WITH"))

            for (i in 1..30) {
                val genericId = "Generic_$i"
                putNode(Node(genericId))
                putEdge(Edge(genericId, "MegaHub", "ASSOCIATED_WITH"))
            }
        }

        val seeds = mapOf("Seed" to 1.0)

        // Standard PageRank without hub damping (beta = 0.0)
        val undampedRanks = GraphAlgorithms.personalizedPageRank(
            storage = graph,
            seedScores = seeds,
            hubDampingBeta = 0.0,
            iterations = 25,
            maxHops = 3
        )

        // Hub-Damped PPR with hub damping (beta = 0.75)
        val dampedRanks = GraphAlgorithms.personalizedPageRank(
            storage = graph,
            seedScores = seeds,
            hubDampingBeta = 0.75,
            iterations = 25,
            maxHops = 3
        )

        val undampedHub = undampedRanks["MegaHub"] ?: 0.0
        val undampedTarget = undampedRanks["Target"] ?: 0.0

        val dampedHub = dampedRanks["MegaHub"] ?: 0.0
        val dampedTarget = dampedRanks["Target"] ?: 0.0

        // In damped mode, the relative weight of Target compared to Hub increases significantly
        val undampedRatio = if (undampedHub > 0.0) undampedTarget / undampedHub else 1.0
        val dampedRatio = if (dampedHub > 0.0) dampedTarget / dampedHub else 1.0

        assertTrue(
            "Hub damping beta=0.75 must increase Target-to-Hub ratio ($dampedRatio > $undampedRatio)",
            dampedRatio > undampedRatio
        )
    }

    @Test
    fun testSeedTeleportationAndConservation() = runBlocking {
        val graph = db.graph()

        // Triangle graph: A -> B -> C -> A
        graph.transaction {
            putNode(Node("A"))
            putNode(Node("B"))
            putNode(Node("C"))
            putEdge(Edge("A", "B", "LINK"))
            putEdge(Edge("B", "C", "LINK"))
            putEdge(Edge("C", "A", "LINK"))
        }

        // Concentrated seed at node A
        val seeds = mapOf("A" to 10.0)
        val ranks = GraphAlgorithms.personalizedPageRank(
            storage = graph,
            seedScores = seeds,
            dampingFactor = 0.85,
            iterations = 20
        )

        assertFalse("Ranks must not be empty", ranks.isEmpty())
        val totalRank = ranks.values.sum()
        assertEquals("Total rank mass must be conserved at 1.0", 1.0, totalRank, 1e-4)

        // Node A must have the highest rank because it is the sole teleportation target
        val highestNode = ranks.entries.first().key
        assertEquals("A", highestNode)
    }

    @Test
    fun testPowerIterationConvergenceWithDanglingNodes() = runBlocking {
        val graph = db.graph()

        // Linear sink graph: S -> D1 -> D2 (D2 has no outgoing edges, dangling sink)
        graph.transaction {
            putNode(Node("S"))
            putNode(Node("D1"))
            putNode(Node("D2"))
            putEdge(Edge("S", "D1", "REL"))
            putEdge(Edge("D1", "D2", "REL"))
        }

        val seeds = mapOf("S" to 1.0)
        val ranks = GraphAlgorithms.personalizedPageRank(
            storage = graph,
            seedScores = seeds,
            dampingFactor = 0.85,
            iterations = 20,
            tolerance = 1e-5
        )

        assertEquals(3, ranks.size)
        val sum = ranks.values.sum()
        assertEquals(1.0, sum, 1e-4)
        assertTrue(ranks["S"]!! > 0.0)
        assertTrue(ranks["D1"]!! > 0.0)
        assertTrue(ranks["D2"]!! > 0.0)
    }

    @Test
    fun testEmptyAndSingleNodeEdgeCases() = runBlocking {
        val graph = db.graph()

        // Empty seed scores
        val emptyRanks = GraphAlgorithms.personalizedPageRank(graph, emptyMap())
        assertTrue(emptyRanks.isEmpty())

        // Single isolated node
        graph.transaction {
            putNode(Node("Isolated"))
        }
        val singleRanks = GraphAlgorithms.personalizedPageRank(graph, mapOf("Isolated" to 5.0))
        assertEquals(1, singleRanks.size)
        assertEquals(1.0, singleRanks["Isolated"] ?: 0.0, 1e-5)
    }

    @Test
    fun testOverloadedDetectCommunitiesWithWeightsAndMultipleEdgeTypes() = runBlocking {
        val graph = db.graph()

        // Community 1: C1_A, C1_B strongly linked via TYPE_1
        // Community 2: C2_A, C2_B strongly linked via TYPE_2
        // Weak cross link between C1_B and C2_A with weight 0.1
        graph.transaction {
            putNode(Node("C1_A"))
            putNode(Node("C1_B"))
            putNode(Node("C2_A"))
            putNode(Node("C2_B"))

            putEdge(Edge("C1_A", "C1_B", "TYPE_1", properties = mapOf("weight" to "5.0")))
            putEdge(Edge("C2_A", "C2_B", "TYPE_2", properties = mapOf("weight" to "5.0")))
            putEdge(Edge("C1_B", "C2_A", "CROSS", properties = mapOf("weight" to "0.1")))
        }

        val allNodes = listOf("C1_A", "C1_B", "C2_A", "C2_B")
        val communities = GraphAlgorithms.detectCommunities(
            storage = graph,
            nodeIds = allNodes,
            edgeTypes = null, // Traverse all edge types
            weightProperty = "weight",
            iterations = 15
        )

        assertEquals(4, communities.size)
        // Nodes in community 1 should share the same community ID
        assertEquals(communities["C1_A"], communities["C1_B"])
        // Nodes in community 2 should share the same community ID
        assertEquals(communities["C2_A"], communities["C2_B"])
        // Distinct communities
        assertNotEquals(communities["C1_A"], communities["C2_A"])
    }

    @Test
    fun testAssociativePageRankQueryEndToEndInBridge() = runBlocking {
        val graph = db.graph()
        val vectorCol = db.vectorCollection("bridge_embeddings", dimensions = 64, metric = DistanceMetric.COSINE)
        val bridge = GraphVectorBridge(graph, vectorCol)

        val rng = Random(42L)
        val docs = listOf("doc_1", "doc_2", "doc_3")
        for (id in docs) {
            graph.putNode(Node(id, labels = setOf("Chunk"), properties = mapOf("title" to "Title for $id")))
            val vec = FloatArray(64) { rng.nextGaussian().toFloat() }
            // normalize
            var norm = 0f
            for (v in vec) norm += v * v
            norm = kotlin.math.sqrt(norm)
            for (i in 0 until 64) vec[i] /= norm
            vectorCol.insert(id, vec)
        }
        vectorCol.waitForIndexing()

        graph.putEdge(Edge("doc_1", "doc_2", "CONNECTED_TO"))
        graph.putEdge(Edge("doc_2", "doc_3", "CONNECTED_TO"))

        val queryVec = FloatArray(64) { rng.nextGaussian().toFloat() }
        var qNorm = 0f
        for (v in queryVec) qNorm += v * v
        qNorm = kotlin.math.sqrt(qNorm)
        for (i in 0 until 64) queryVec[i] /= qNorm

        val results = bridge.associativePageRankQuery(
            queryText = "test query",
            queryVector = queryVec,
            seedLimit = 5,
            maxHops = 2,
            finalLimit = 3,
            targetLabels = setOf("Chunk")
        )

        assertNotNull(results)
        assertFalse("Bridge results should not be empty", results.isEmpty())
        assertTrue("Results should be at most finalLimit", results.size <= 3)

        // Verify BridgeResult fields
        val firstResult = results.first()
        assertTrue(firstResult.id.startsWith("doc_"))
        assertTrue(firstResult.similarity > 0f)
        assertNotNull(firstResult.node)
        assertEquals("Chunk", firstResult.node?.labels?.first())
    }

    @Test
    fun testAdversarialHubNoiseSuppressionMassiveHubDegree100() = runBlocking {
        val graph = db.graph()

        // 3-hop associative chain: Seed -> Step1 -> Step2 -> Target
        // MegaHub: degree 100+ (100 generic nodes + 1 from Seed)
        // Seed also connects to MegaHub: Seed -> MegaHub
        graph.transaction {
            putNode(Node("Seed"))
            putNode(Node("Step1"))
            putNode(Node("Step2"))
            putNode(Node("Target"))
            putNode(Node("MegaHub"))

            putEdge(Edge("Seed", "Step1", "ASSOCIATED_WITH"))
            putEdge(Edge("Step1", "Step2", "ASSOCIATED_WITH"))
            putEdge(Edge("Step2", "Target", "ASSOCIATED_WITH"))

            putEdge(Edge("Seed", "MegaHub", "ASSOCIATED_WITH"))

            for (i in 1..100) {
                val genericId = "Generic_$i"
                putNode(Node(genericId))
                putEdge(Edge(genericId, "MegaHub", "ASSOCIATED_WITH"))
            }
        }

        val seeds = mapOf("Seed" to 1.0)

        // Baseline PPR without hub damping (beta = 0.0)
        val undampedRanks = GraphAlgorithms.personalizedPageRank(
            storage = graph,
            seedScores = seeds,
            hubDampingBeta = 0.0,
            iterations = 30,
            maxHops = 3
        )

        // Hub-Damped PPR with hub damping (beta = 0.5)
        val dampedRanks = GraphAlgorithms.personalizedPageRank(
            storage = graph,
            seedScores = seeds,
            hubDampingBeta = 0.5,
            iterations = 30,
            maxHops = 3
        )

        val undampedHub = undampedRanks["MegaHub"] ?: 0.0
        val undampedTarget = undampedRanks["Target"] ?: 0.0

        val dampedHub = dampedRanks["MegaHub"] ?: 0.0
        val dampedTarget = dampedRanks["Target"] ?: 0.0

        // Assertion 1: Under beta = 0.0, proximity bias causes MegaHub to rank HIGHER than the 3-hop target
        assertTrue(
            "Under beta = 0.0, generic hub ($undampedHub) must rank higher than 3-hop target ($undampedTarget)",
            undampedHub > undampedTarget
        )

        // Assertion 2: Under beta = 0.5, inverse degree damping enables 3-hop target to rank strictly HIGHER than generic hub
        assertTrue(
            "Under beta = 0.5, 3-hop target ($dampedTarget) must rank strictly higher than generic hub ($dampedHub)",
            dampedTarget > dampedHub
        )
    }

    @Test
    fun testAdversarialProbabilityConservationDanglingAndDisconnected() = runBlocking {
        val graph = db.graph()

        // 3 Disconnected components:
        // Component 1: Seed1 -> A1 -> Dangling1 (no outgoing edges)
        // Component 2: Seed2 -> B1 -> Dangling2 (no outgoing edges), and B1 -> B2 (dangling)
        // Component 3: Seed3 (isolated single node, degree 0)
        // Component 4: Disconnected cycle C1 -> C2 -> C1 (no seeds)
        graph.transaction {
            putNode(Node("Seed1"))
            putNode(Node("A1"))
            putNode(Node("Dangling1"))
            putEdge(Edge("Seed1", "A1", "REL"))
            putEdge(Edge("A1", "Dangling1", "REL"))

            putNode(Node("Seed2"))
            putNode(Node("B1"))
            putNode(Node("Dangling2"))
            putNode(Node("B2"))
            putEdge(Edge("Seed2", "B1", "REL"))
            putEdge(Edge("B1", "Dangling2", "REL"))
            putEdge(Edge("B1", "B2", "REL"))

            putNode(Node("Seed3"))

            putNode(Node("C1"))
            putNode(Node("C2"))
            putEdge(Edge("C1", "C2", "CYCLE"))
            putEdge(Edge("C2", "C1", "CYCLE"))
        }

        val seeds = mapOf("Seed1" to 2.0, "Seed2" to 3.0, "Seed3" to 5.0)

        // Test across varying iteration counts and damping parameters
        for (iter in listOf(1, 2, 5, 10, 20, 50)) {
            for (damping in listOf(0.1, 0.5, 0.85, 0.99)) {
                for (beta in listOf(0.0, 0.5, 1.0)) {
                    val ranks = GraphAlgorithms.personalizedPageRank(
                        storage = graph,
                        seedScores = seeds,
                        iterations = iter,
                        dampingFactor = damping,
                        hubDampingBeta = beta,
                        maxHops = 3
                    )
                    val totalProbability = ranks.values.sum()
                    assertEquals(
                        "Probability sum must equal 1.0 +- 1e-4 at iter=$iter, damping=$damping, beta=$beta",
                        1.0,
                        totalProbability,
                        1e-4
                    )
                }
            }
        }
    }

    @Test
    fun testAdversarialDegenerateTopologiesResilience() = runBlocking {
        val graph = db.graph()

        // 1. Empty graph with empty seeds
        val r1 = GraphAlgorithms.personalizedPageRank(graph, emptyMap())
        assertTrue(r1.isEmpty())

        // 2. Zero-weight seeds
        val r2 = GraphAlgorithms.personalizedPageRank(graph, mapOf("A" to 0.0, "B" to 0.0))
        assertTrue(r2.isEmpty())

        // 3. Negative total seed scores
        val r3 = GraphAlgorithms.personalizedPageRank(graph, mapOf("A" to -5.0, "B" to 2.0))
        assertTrue(r3.isEmpty())

        // 4. Non-existent seed nodes in empty graph
        val r4 = GraphAlgorithms.personalizedPageRank(graph, mapOf("Ghost" to 10.0))
        assertEquals(1, r4.size)
        assertEquals(1.0, r4["Ghost"] ?: 0.0, 1e-5)

        // 5. Self-loop node
        graph.transaction {
            putNode(Node("LoopNode"))
            putEdge(Edge("LoopNode", "LoopNode", "SELF"))
        }
        val r5 = GraphAlgorithms.personalizedPageRank(graph, mapOf("LoopNode" to 1.0))
        assertEquals(1, r5.size)
        assertEquals(1.0, r5["LoopNode"] ?: 0.0, 1e-5)

        // 6. Max hops = 0
        graph.transaction {
            putNode(Node("X"))
            putNode(Node("Y"))
            putEdge(Edge("X", "Y", "CONN"))
        }
        val r6 = GraphAlgorithms.personalizedPageRank(graph, mapOf("X" to 1.0), maxHops = 0)
        assertEquals(1, r6.size)
        assertEquals(1.0, r6["X"] ?: 0.0, 1e-5)

        // 7. Iterations = 0
        val r7 = GraphAlgorithms.personalizedPageRank(graph, mapOf("X" to 2.0, "Y" to 8.0), iterations = 0)
        assertEquals(2, r7.size)
        assertEquals(0.2, r7["X"] ?: 0.0, 1e-5)
        assertEquals(0.8, r7["Y"] ?: 0.0, 1e-5)
    }

    @Test
    fun testAdversarialAssociativePageRankQueryResilience() = runBlocking {
        val graph = db.graph()
        val vectorCol = db.vectorCollection("adversarial_bridge_vecs", dimensions = 32, metric = DistanceMetric.COSINE)
        val bridge = GraphVectorBridge(graph, vectorCol)

        // Empty vector collection and empty graph
        val emptyQueryVec = FloatArray(32) { 0.1f }
        val rEmpty = bridge.associativePageRankQuery(
            queryText = "unmatched text",
            queryVector = emptyQueryVec
        )
        assertTrue(rEmpty.isEmpty())

        // Seed nodes exist in vector collection but not in graph
        vectorCol.insert("isolated_chunk_1", FloatArray(32) { 0.5f })
        vectorCol.waitForIndexing()

        val rIsolated = bridge.associativePageRankQuery(
            queryText = "text",
            queryVector = FloatArray(32) { 0.5f },
            targetLabels = emptySet()
        )
        assertEquals(1, rIsolated.size)
        assertEquals("isolated_chunk_1", rIsolated[0].id)
        assertNull(rIsolated[0].node)
    }
}

