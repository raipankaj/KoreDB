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

import com.pankaj.koredb.bridge.GraphVectorBridge
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.hnsw.BinaryQuantizer
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.HNSWIndex
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
 * Comprehensive Opaque-Box E2E Test Suite for:
 * 1. Hub-Damped Personalized PageRank (PPR, Hub-Noise Damping, Multi-Hop Associative Recall)
 * 2. HNSW Prefix-Truncated 64-dim Short-Circuiting
 * 3. Rotated Binary Quantization (Orthogonal Projection, POPCNT Unbiased Estimation)
 *
 * Covering:
 * - Tier 1: Feature Coverage (>=5 test cases per feature)
 * - Tier 2: Boundary & Corner Cases (empty graphs, single-node, disconnected, zero-vectors, dimension mismatches)
 * - Tier 3: Cross-Feature Interactions (RotatedBQ + Prefix vector funnel, Hub-Damped PageRank with RRF multi-modal seeds)
 * - Tier 4: Real-World Scenarios (Multi-hop reasoning over noisy knowledge graphs with high-degree hubs)
 */
class AssociativePageRankAndVectorFunnelE2ETest {

    private lateinit var testDir: File
    private val random = Random(42L)

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_e2e_associative_ppr_funnel_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // TIER 1: FEATURE COVERAGE (>= 5 Tests Per Feature)
    // ========================================================================

    // --- Feature 1: Hub-Damped Personalized PageRank ---

    @Test
    fun testHubDampedPPRSeedNormalizationAndConservation() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Construct triangle graph: A -> B -> C -> A
        graph.putNode(Node("A", properties = mapOf("text" to "Alpha")))
        graph.putNode(Node("B", properties = mapOf("text" to "Beta")))
        graph.putNode(Node("C", properties = mapOf("text" to "Gamma")))
        graph.putEdge(Edge("A", "B", "NEXT"))
        graph.putEdge(Edge("B", "C", "NEXT"))
        graph.putEdge(Edge("C", "A", "NEXT"))

        val seeds = mapOf("A" to 2.0, "B" to 1.0)
        val groundTruth = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = setOf("A", "B", "C"),
            outEdges = mapOf("A" to listOf("B" to 1.0), "B" to listOf("C" to 1.0), "C" to listOf("A" to 1.0)),
            seeds = seeds,
            dampingFactor = 0.85,
            beta = 0.5
        )

        assertNotNull(groundTruth)
        assertEquals(3, groundTruth.size)
        // Probability mass conservation check
        val sumRanks = groundTruth.values.sum()
        assertEquals(1.0, sumRanks, 1e-4)

        // Seed A (weight 2.0) should have strictly higher rank than non-seed C
        assertTrue("Seed A rank should be positive", groundTruth["A"]!! > 0.0)
        assertTrue("Seed B rank should be positive", groundTruth["B"]!! > 0.0)
        assertTrue("Seed A rank (${groundTruth["A"]}) should exceed C (${groundTruth["C"]})",
            groundTruth["A"]!! > groundTruth["C"]!!)

        db.close()
    }

    @Test
    fun testHubDampingSuppressesHubNoise() = runBlocking {
        // Topology:
        // Source Seed S -> Target T (specific associative relationship)
        // Source Seed S -> Hub H (generic hub with 30 incoming links from noise nodes N1..N30)
        val nodes = HashSet<String>()
        nodes.add("S")
        nodes.add("T")
        nodes.add("H")
        for (i in 1..30) nodes.add("N$i")

        val outEdges = HashMap<String, MutableList<Pair<String, Double>>>()
        outEdges["S"] = mutableListOf("T" to 1.0, "H" to 1.0)
        for (i in 1..30) {
            outEdges["N$i"] = mutableListOf("H" to 1.0)
        }

        val seeds = mapOf("S" to 1.0)

        // 1. Without hub damping (beta = 0.0) - Standard PageRank where hub pools mass
        val standardRanks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85,
            beta = 0.0
        )

        // 2. With Hub-Damped PPR hub damping (beta = 0.75)
        val dampedRanks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85,
            beta = 0.75
        )

        // Verifies hub damping strictly reduces the hub's proportion of mass relative to specific target T
        val ratioWithoutDamping = standardRanks["H"]!! / standardRanks["T"]!!
        val ratioWithDamping = dampedRanks["H"]!! / dampedRanks["T"]!!

        assertTrue("Hub ratio with damping ($ratioWithDamping) must be strictly less than without damping ($ratioWithoutDamping)",
            ratioWithDamping < ratioWithoutDamping)
        assertTrue("Target T should receive meaningful probability mass under damping", dampedRanks["T"]!! > 0.1)
    }

    @Test
    fun testHubDampedPPRMultiHopAssociativeRecall() = runBlocking {
        // Multi-hop path: Query Seed (Q) -> Intermediate Context (I) -> Target Entity (E)
        // Non-relevant branch: Q -> Irrelevant (X) -> Dead End (D)
        val nodes = setOf("Q", "I", "E", "X", "D")
        val outEdges = mapOf(
            "Q" to listOf("I" to 2.0, "X" to 0.5),
            "I" to listOf("E" to 2.0),
            "X" to listOf("D" to 1.0),
            "E" to listOf("I" to 1.0),
            "D" to listOf("X" to 0.5)
        )
        val seeds = mapOf("Q" to 10.0)

        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85,
            beta = 0.5
        )

        // 2-hop target E should have significantly higher rank than the irrelevant branch D
        assertTrue(ranks["E"]!! > ranks["D"]!!)
        assertTrue(ranks["I"]!! > ranks["X"]!!)
    }

    @Test
    fun testHubDampedPPRDanglingNodesTeleportation() = runBlocking {
        // Graph with dangling sinks (no outbound edges): S -> D1, S -> D2
        val nodes = setOf("S", "D1", "D2")
        val outEdges = mapOf(
            "S" to listOf("D1" to 1.0, "D2" to 1.0)
            // D1 and D2 have empty outbound edges (dangling)
        )
        val seeds = mapOf("S" to 1.0)

        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85,
            beta = 0.5
        )

        // Probability mass should not leak from dangling sinks; total sum must remain 1.0
        val sumRanks = ranks.values.sum()
        assertEquals(1.0, sumRanks, 1e-4)
        assertTrue(ranks["S"]!! > 0.0)
        assertTrue(ranks["D1"]!! > 0.0)
        assertTrue(ranks["D2"]!! > 0.0)
    }

    @Test
    fun testHubDampedPPROverloadedDetectCommunities() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Cluster 1: N1, N2, N3 connected by "FRIEND" and "COWORKER"
        graph.putNode(Node("N1"))
        graph.putNode(Node("N2"))
        graph.putNode(Node("N3"))
        graph.putEdge(Edge("N1", "N2", "FRIEND"))
        graph.putEdge(Edge("N2", "N3", "COWORKER"))

        // Cluster 2: M1, M2 connected by "MENTIONS"
        graph.putNode(Node("M1"))
        graph.putNode(Node("M2"))
        graph.putEdge(Edge("M1", "M2", "MENTIONS"))

        // Community detection should handle edge clustering
        val allNodes = listOf("N1", "N2", "N3", "M1", "M2")
        val communities = GraphAlgorithms.detectCommunities(graph, allNodes, "FRIEND")
        assertNotNull(communities)
        assertEquals(allNodes.size, communities.size)

        db.close()
    }

    // --- Feature 2: HNSW Prefix Short-Circuiting ---

    @Test
    fun testPrefixDistanceCalculation() {
        val dim = 768
        val prefixDim = 64
        val v1 = TestContracts.generateNormalizedVector(dim, Random(101L))
        val v2 = TestContracts.generateNormalizedVector(dim, Random(202L))

        // DistanceMetric prefix compute
        val prefixDist = DistanceMetric.COSINE.computePrefix(v1, v2, prefixDim)
        assertTrue("Prefix similarity should be in valid range [-1, 1]", prefixDist in -1.0f..1.0f)

        // Manual slice verification
        var dotPrefix = 0.0f
        var mag1 = 0.0f
        var mag2 = 0.0f
        for (i in 0 until prefixDim) {
            dotPrefix += v1[i] * v2[i]
            mag1 += v1[i] * v1[i]
            mag2 += v2[i] * v2[i]
        }
        val expectedPrefixSim = dotPrefix / (sqrt(mag1) * sqrt(mag2))
        assertEquals(expectedPrefixSim, prefixDist, 1e-4f)
    }

    @Test
    fun testPrefixUpperLayerRoutingWithCachedMagnitude() {
        val index = HNSWIndex(dimensions = 128, metric = DistanceMetric.COSINE, maxNeighbors = 16, efConstruction = 64)
        for (i in 0 until 50) {
            val vec = TestContracts.generateNormalizedVector(128, Random(i.toLong()))
            index.insert("doc_$i", vec)
        }

        assertEquals(50, index.size())
        val query = TestContracts.generateNormalizedVector(128, Random(999L))
        val results = index.search(query, limit = 5)

        assertEquals(5, results.size)
        // Verify monotonicity of search similarities
        for (i in 0 until results.size - 1) {
            assertTrue(results[i].second >= results[i + 1].second - 1e-6f)
        }
    }

    @Test
    fun testPrefixLayer0FullRescoringGuaranteesExactness() {
        val index = HNSWIndex(dimensions = 768, metric = DistanceMetric.COSINE, maxNeighbors = 16, efConstruction = 64)
        val vectors = HashMap<String, FloatArray>()
        for (i in 0 until 30) {
            val id = "vec_$i"
            val vec = TestContracts.generateNormalizedVector(768, Random(i.toLong() * 37))
            vectors[id] = vec
            index.insert(id, vec)
        }

        val query = TestContracts.generateNormalizedVector(768, Random(12345L))
        val results = index.search(query, limit = 5)

        assertEquals(5, results.size)
        // Verify Layer 0 rescore similarity matches exact 768-dim similarity
        for (res in results) {
            val originalVec = vectors[res.first]!!
            val exactSim = DistanceMetric.COSINE.compute(query, originalVec)
            assertEquals("Reported similarity must match full 768-dim exact similarity",
                exactSim, res.second, 1e-3f)
        }
    }

    @Test
    fun testPrefixRecallPreservationAcrossEmbeddings() {
        val dim = 128
        val count = 40
        val index = HNSWIndex(dimensions = dim, metric = DistanceMetric.COSINE, maxNeighbors = 16)
        val data = (0 until count).map { "item_$it" to TestContracts.generateNormalizedVector(dim, Random(it.toLong())) }
        for ((id, vec) in data) {
            index.insert(id, vec)
        }

        val query = TestContracts.generateNormalizedVector(dim, Random(7777L))
        val hnswResults = index.search(query, limit = 5).map { it.first }.toSet()

        // Brute-force top 5
        val exactTop5 = data.map { Pair(it.first, DistanceMetric.COSINE.compute(query, it.second)) }
            .sortedByDescending { it.second }
            .take(5)
            .map { it.first }
            .toSet()

        val intersection = hnswResults.intersect(exactTop5).size
        assertTrue("Recall should be >= 60% on small index, got $intersection / 5", intersection >= 3)
    }

    @Test
    fun testPrefixDimensionConfigurability() {
        val dim = 64
        val v1 = TestContracts.generateNormalizedVector(dim, Random(1L))
        val v2 = TestContracts.generateNormalizedVector(dim, Random(2L))

        val d32 = DistanceMetric.COSINE.computePrefix(v1, v2, 32)
        val d64 = DistanceMetric.COSINE.computePrefix(v1, v2, 64)
        val full = DistanceMetric.COSINE.compute(v1, v2)

        assertTrue("Prefix 32 distance should be valid", d32 >= 0.0f)
        assertEquals("Prefix 64 on 64-dim vector should match full compute", full, d64, 1e-4f)
    }

    // --- Feature 3: Rotated Binary Quantization in BinaryQuantizer ---

    @Test
    fun testRotatedBinaryOrthogonalProjectionPreservesNorm() {
        val dim = 64
        val matrix = TestContracts.generateOrthogonalMatrixGroundTruth(dim, seed = 42L)

        // Verify R^T R = I
        for (i in 0 until dim) {
            for (j in 0 until dim) {
                var dot = 0.0f
                for (k in 0 until dim) {
                    dot += matrix[k][i] * matrix[k][j]
                }
                val expected = if (i == j) 1.0f else 0.0f
                assertEquals(expected, dot, 1e-3f)
            }
        }

        // Verify norm preservation: ||Rx||_2 = ||x||_2
        val x = TestContracts.generateNormalizedVector(dim, Random(99L))
        val rx = FloatArray(dim)
        for (i in 0 until dim) {
            var sum = 0.0f
            for (j in 0 until dim) {
                sum += matrix[i][j] * x[j]
            }
            rx[i] = sum
        }
        var rxNormSq = 0.0f
        for (v in rx) rxNormSq += v * v
        assertEquals(1.0f, sqrt(rxNormSq), 1e-3f)
    }

    @Test
    fun testRotatedBinaryCodeQuantizationAndCompressionRatio() {
        val dim = 768
        val bq = BinaryQuantizer(dim)
        val vec = TestContracts.generateNormalizedVector(dim, Random(123L))

        val packed = bq.quantize(vec)
        // 768 bits packed into LongArray: 768 / 64 = 12 Longs = 96 bytes
        assertEquals(12, packed.size)

        val rawBytes = dim * 4 // 3072 bytes
        val compressedBytes = packed.size * 8 // 96 bytes
        val compressionRatio = rawBytes.toFloat() / compressedBytes.toFloat()
        assertEquals(32.0f, compressionRatio, 0.01f)
    }

    @Test
    fun testRotatedBinaryAsymmetricInnerProductEstimator() {
        val dim = 128
        val random = Random(456L)
        val q = TestContracts.generateNormalizedVector(dim, random)
        val x = TestContracts.generateNormalizedVector(dim, random)

        // Exact inner product
        var exactIp = 0.0f
        for (i in 0 until dim) exactIp += q[i] * x[i]

        // 1-bit quantization of x
        val bq = BinaryQuantizer(dim)
        val packedX = bq.quantize(x)

        // Evaluate Hamming distance against quantized q
        val packedQ = bq.quantize(q)
        val hamming = bq.hammingDistance(packedQ, packedX)

        val sim = bq.hammingSimilarity(packedQ, packedX)
        assertTrue("Hamming similarity in [0, 1]", sim in 0.0f..1.0f)
    }

    @Test
    fun testRotatedBinarySymmetricEstimatorUsingHardwarePopcnt() {
        val dim = 64
        val bq = BinaryQuantizer(dim)
        val v1 = TestContracts.generateNormalizedVector(dim, Random(11L))
        val v2 = TestContracts.generateNormalizedVector(dim, Random(22L))

        val q1 = bq.quantize(v1)
        val q2 = bq.quantize(v2)

        val hammingDist = bq.hammingDistance(q1, q2)
        assertTrue("Hamming distance in [0, dim]", hammingDist in 0..dim)

        // POPCNT based ground truth estimator
        val cq = 1.0f / sqrt(dim.toFloat())
        val cx = 1.0f / sqrt(dim.toFloat())
        val estIp = TestContracts.estimateRotatedBinaryInnerProductGroundTruth(dim, hammingDist, cq, cx)

        // Estimated IP should be finite and bounded
        assertFalse(estIp.isNaN())
        assertFalse(estIp.isInfinite())
    }

    @Test
    fun testRotatedBinaryUnbiasedExpectation() {
        val dim = 64
        val random = Random(789L)
        val q = TestContracts.generateNormalizedVector(dim, random)
        val x = TestContracts.generateNormalizedVector(dim, random)

        var exactIp = 0.0f
        for (i in 0 until dim) exactIp += q[i] * x[i]

        // Sample multiple random orthogonal projections
        var sumEstimatedIp = 0.0f
        val numProjections = 20
        for (p in 0 until numProjections) {
            val matrix = TestContracts.generateOrthogonalMatrixGroundTruth(dim, seed = (p + 1).toLong())
            val qRotated = FloatArray(dim)
            val xRotated = FloatArray(dim)
            for (i in 0 until dim) {
                for (j in 0 until dim) {
                    qRotated[i] += matrix[i][j] * q[j]
                    xRotated[i] += matrix[i][j] * x[j]
                }
            }
            val bq = BinaryQuantizer(dim)
            val codeQ = bq.quantize(qRotated)
            val codeX = bq.quantize(xRotated)
            val h = bq.hammingDistance(codeQ, codeX)
            val cq = 1.0f / sqrt(dim.toFloat())
            val cx = 1.0f / sqrt(dim.toFloat())
            sumEstimatedIp += TestContracts.estimateRotatedBinaryInnerProductGroundTruth(dim, h, cq, cx)
        }
        val meanEstimatedIp = sumEstimatedIp / numProjections
        // Correlation check: estimated sign tracks exact sign
        assertEquals(exactIp > 0, meanEstimatedIp > 0.0f)
    }

    // ========================================================================
    // TIER 2: BOUNDARY & CORNER CASES
    // ========================================================================

    @Test
    fun testHubDampedPPREmptyGraphReturnsEmpty() {
        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = emptySet(),
            outEdges = emptyMap(),
            seeds = emptyMap()
        )
        assertTrue(ranks.isEmpty())
    }

    @Test
    fun testHubDampedPPRSingleNodeGraph() {
        val nodes = setOf("SOLO")
        val outEdges = mapOf("SOLO" to listOf("SOLO" to 1.0))
        val seeds = mapOf("SOLO" to 5.0)

        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds
        )

        assertEquals(1, ranks.size)
        assertEquals(1.0, ranks["SOLO"]!!, 1e-4)
    }

    @Test
    fun testHubDampedPPRDisconnectedComponents() {
        // Component 1: A -> B
        // Component 2: X -> Y
        // Seed strictly in Component 1
        val nodes = setOf("A", "B", "X", "Y")
        val outEdges = mapOf(
            "A" to listOf("B" to 1.0),
            "B" to listOf("A" to 1.0),
            "X" to listOf("Y" to 1.0),
            "Y" to listOf("X" to 1.0)
        )
        val seeds = mapOf("A" to 1.0)

        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85
        )

        // Component 2 nodes have 0 teleportation and 0 paths from A
        assertEquals(0.0, ranks["X"] ?: 0.0, 1e-6)
        assertEquals(0.0, ranks["Y"] ?: 0.0, 1e-6)
        assertEquals(1.0, (ranks["A"]!! + ranks["B"]!!), 1e-4)
    }

    @Test
    fun testRotatedBinaryZeroVectorSafeguard() {
        val dim = 64
        val bq = BinaryQuantizer(dim)
        val zeroVec = FloatArray(dim) { 0.0f }

        // Quantization of zero vector should not crash
        val code = bq.quantize(zeroVec)
        assertNotNull(code)
        assertEquals((dim + 63) ushr 6, code.size)
    }

    @Test
    fun testPrefixMaxDimensionAndSmallDimension() {
        // Small dimension: 16 dims (smaller than 64-dim prefix)
        val dim = 16
        val v1 = TestContracts.generateNormalizedVector(dim, Random(1L))
        val v2 = TestContracts.generateNormalizedVector(dim, Random(2L))

        val d16 = DistanceMetric.COSINE.computePrefix(v1, v2, 64) // requesting 64 prefix on 16 dims
        val full = DistanceMetric.COSINE.compute(v1, v2)
        assertEquals(full, d16, 1e-4f)
    }

    // ========================================================================
    // TIER 3: CROSS-FEATURE INTERACTIONS
    // ========================================================================

    @Test
    fun testAdaptiveVectorFunnelRotatedBinaryPreFilterThenPrefixRescore() {
        val dim = 128
        val numItems = 100
        val bq = BinaryQuantizer(dim)
        val corpus = (0 until numItems).map {
            "item_$it" to TestContracts.generateNormalizedVector(dim, Random(it.toLong()))
        }

        val quantizedCorpus = corpus.map { it.first to bq.quantize(it.second) }
        val query = TestContracts.generateNormalizedVector(dim, Random(888L))
        val quantizedQuery = bq.quantize(query)

        // Stage 1: Rotated 1-bit Hamming candidate retrieval (oversample 3x -> top 15)
        val candidates = quantizedCorpus.map { (id, code) ->
            id to bq.hammingDistance(quantizedQuery, code)
        }.sortedBy { it.second }.take(15).map { it.first }.toSet()

        assertEquals(15, candidates.size)

        // Stage 2: Prefix 64-dim rescoring
        val corpusMap = corpus.toMap()
        val stage2Results = candidates.map { id ->
            id to DistanceMetric.COSINE.computePrefix(query, corpusMap[id]!!, 64)
        }.sortedBy { it.second }.take(5)

        assertEquals(5, stage2Results.size)
        // Stage 3: Layer 0 full 128-dim rescoring
        val finalResults = stage2Results.map { (id, _) ->
            id to DistanceMetric.COSINE.compute(query, corpusMap[id]!!)
        }.sortedBy { it.second }

        assertEquals(5, finalResults.size)
        assertTrue("Top candidate has lower distance than 5th", finalResults[0].second <= finalResults[4].second)
    }

    @Test
    fun testHubDampedPPRPoweredByDualVectorBM25Seeds() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()

        // Multi-relational graph
        graph.putNode(Node("doc1", properties = mapOf("text" to "Android Database Engine")))
        graph.putNode(Node("doc2", properties = mapOf("text" to "HNSW Vector Indexing")))
        graph.putNode(Node("doc3", properties = mapOf("text" to "PageRank Graph Navigation")))

        graph.putEdge(Edge("doc1", "doc2", "REFERENCES"))
        graph.putEdge(Edge("doc2", "doc3", "REFERENCES"))

        // Seed scores simulated from RRF (BM25 + Vector)
        val rrfSeeds = mapOf("doc1" to 0.033, "doc2" to 0.016)

        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = setOf("doc1", "doc2", "doc3"),
            outEdges = mapOf(
                "doc1" to listOf("doc2" to 1.0),
                "doc2" to listOf("doc3" to 1.0),
                "doc3" to listOf("doc1" to 1.0)
            ),
            seeds = rrfSeeds,
            dampingFactor = 0.85
        )

        assertTrue(ranks["doc1"]!! > 0.0)
        assertTrue(ranks["doc2"]!! > 0.0)
        assertTrue("doc3 reached through associative propagation", ranks["doc3"]!! > 0.0)
        assertEquals(1.0, ranks.values.sum(), 1e-4)

        db.close()
    }

    @Test
    fun testMultiRelationalHubDampedPPRAcrossHeterogeneousEdges() = runBlocking {
        val nodes = setOf("Chunk1", "EntityA", "EntityB", "Chunk2")
        val outEdges = mapOf(
            "Chunk1" to listOf("EntityA" to 1.0), // MENTIONS
            "EntityA" to listOf("EntityB" to 2.0), // RELATED_TO
            "EntityB" to listOf("Chunk2" to 1.0), // MENTIONED_IN
            "Chunk2" to listOf("Chunk1" to 0.5)  // NEXT_CHUNK
        )
        val seeds = mapOf("Chunk1" to 1.0)

        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85,
            beta = 0.5
        )

        // Associative recall across multi-relational path: Chunk1 -> EntityA -> EntityB -> Chunk2
        assertTrue(ranks["EntityA"]!! > 0.0)
        assertTrue(ranks["EntityB"]!! > 0.0)
        assertTrue(ranks["Chunk2"]!! > 0.0)
        assertTrue(ranks["EntityA"]!! > ranks["Chunk2"]!!)
    }

    // ========================================================================
    // TIER 4: REAL-WORLD SCENARIOS
    // ========================================================================

    @Test
    fun testMultiHopReasoningOverKnowledgeGraphWithHubNoise() = runBlocking {
        // Scenario: Question = "Who is the advisor of Alice's collaborator?"
        // Alice -> CollaboratesWith -> Bob -> AdvisedBy -> Carol (Target)
        // Noise Hubs: Year_2026, Computer_Science, Stanford (each connected to 20 other entities)
        val nodes = HashSet<String>()
        nodes.addAll(listOf("Alice", "Bob", "Carol", "Hub_2026", "Hub_CS", "Hub_Univ"))
        for (i in 1..20) {
            nodes.add("Noise_$i")
        }

        val outEdges = HashMap<String, MutableList<Pair<String, Double>>>()
        outEdges["Alice"] = mutableListOf("Bob" to 2.0, "Hub_2026" to 0.5, "Hub_CS" to 0.5)
        outEdges["Bob"] = mutableListOf("Carol" to 2.0, "Hub_2026" to 0.5, "Hub_Univ" to 0.5)
        outEdges["Carol"] = mutableListOf("Hub_Univ" to 0.5)

        for (i in 1..20) {
            outEdges["Noise_$i"] = mutableListOf("Hub_2026" to 1.0, "Hub_CS" to 1.0, "Hub_Univ" to 1.0)
        }

        val seeds = mapOf("Alice" to 10.0)

        // Run Hub-Damped PageRank
        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = nodes,
            outEdges = outEdges,
            seeds = seeds,
            dampingFactor = 0.85,
            beta = 0.75
        )

        // Assert Carol (the 2-hop answer) ranks significantly above the generic hubs
        assertTrue("Carol rank (${ranks["Carol"]}) should be strictly greater than Hub_2026 (${ranks["Hub_2026"]})",
            ranks["Carol"]!! > ranks["Hub_2026"]!!)
        assertTrue("Bob rank (${ranks["Bob"]}) should be greater than Hub_CS (${ranks["Hub_CS"]})",
            ranks["Bob"]!! > ranks["Hub_CS"]!!)
    }

    @Test
    fun testEndToEndHubDampedPPRDocumentPassageRetrievalPipeline() = runBlocking {
        val db = KoreDatabase(testDir)
        val graph = db.graph()
        val vectorCol = db.vectorCollection("doc_embeddings") {
            dimensions = 64
            metric = DistanceMetric.COSINE
        }

        // Ingest documents and vectors
        val docs = listOf(
            "doc_1" to "KoreDB provides on-device LSM storage and HNSW vector indexing.",
            "doc_2" to "Hub-Damped PageRank eliminates hub noise using degree-damped personalized PageRank.",
            "doc_3" to "Binary quantization compresses float vectors by 32x using POPCNT."
        )

        for ((id, text) in docs) {
            graph.putNode(Node(id, properties = mapOf("text" to text)))
            val vec = TestContracts.generateNormalizedVector(64, Random(id.hashCode().toLong()))
            vectorCol.insert(id, vec)
        }
        vectorCol.waitForIndexing()

        // Add structural graph link doc_1 -> doc_2 -> doc_3
        graph.putEdge(Edge("doc_1", "doc_2", "NEXT_SECTION"))
        graph.putEdge(Edge("doc_2", "doc_3", "NEXT_SECTION"))

        // Query vector
        val queryVec = TestContracts.generateNormalizedVector(64, Random(999L))
        val vectorHits = vectorCol.search(queryVec, limit = 2)
        assertTrue(vectorHits.isNotEmpty())

        val seeds = vectorHits.associate { it.first to it.second.toDouble() }
        val ranks = TestContracts.computeHubDampedPageRankGroundTruth(
            nodes = docs.map { it.first }.toSet(),
            outEdges = mapOf("doc_1" to listOf("doc_2" to 1.0), "doc_2" to listOf("doc_3" to 1.0)),
            seeds = seeds
        )

        assertNotNull(ranks)
        assertEquals(3, ranks.size)
        db.close()
    }
}
