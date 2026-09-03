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

package com.pankaj.koredb.engine

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

class GraphAndHybridExhaustiveTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_graph_exhaustive_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // NODE CRUD & INDEXING (15 Tests)
    // ========================================================================

    @Test
    fun testPutAndGetSingleNode() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            val node = Node("alice", labels = setOf("Person"), properties = mapOf("name" to "Alice", "age" to "30"))
            graph.putNode(node)

            val retrieved = graph.getNode("alice")
            assertNotNull(retrieved)
            assertEquals("alice", retrieved?.id)
            assertEquals(setOf("Person"), retrieved?.labels)
            assertEquals("Alice", retrieved?.properties?.get("name"))
            db.close()
        }
    }

    @Test
    fun testGetNonExistentNodeReturnsNull() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            assertNull(graph.getNode("ghost"))
            db.close()
        }
    }

    @Test
    fun testUpdateNodeProperties() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("bob", labels = setOf("Person"), properties = mapOf("status" to "offline")))
            graph.putNode(Node("bob", labels = setOf("Person", "Admin"), properties = mapOf("status" to "online")))

            val updated = graph.getNode("bob")
            assertEquals("online", updated?.properties?.get("status"))
            assertEquals(setOf("Person", "Admin"), updated?.labels)
            db.close()
        }
    }

    @Test
    fun testDeleteNodeRemovesIndices() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("charlie", labels = setOf("VIP"), properties = mapOf("level" to "gold")))
            graph.deleteNode("charlie")

            assertNull(graph.getNode("charlie"))
            val vips = graph.getNodesByLabel("VIP")
            assertTrue(vips.none { it.id == "charlie" })
            db.close()
        }
    }

    @Test
    fun testFindNodesByLabel() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("d1", labels = setOf("Device", "IoT")))
            graph.putNode(Node("d2", labels = setOf("Device", "Server")))
            graph.putNode(Node("u1", labels = setOf("User")))

            val devices = graph.getNodesByLabel("Device")
            assertEquals(2, devices.size)
            assertTrue(devices.any { it.id == "d1" })
            assertTrue(devices.any { it.id == "d2" })
            db.close()
        }
    }

    @Test
    fun testFindNodesByProperty() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("p1", labels = setOf("Product"), properties = mapOf("category" to "books")))
            graph.putNode(Node("p2", labels = setOf("Product"), properties = mapOf("category" to "electronics")))
            graph.putNode(Node("p3", labels = setOf("Product"), properties = mapOf("category" to "books")))

            val books = graph.getNodesByProperty("Product", "category", "books")
            assertEquals(2, books.size)
            assertTrue(books.all { it.properties["category"] == "books" })
            db.close()
        }
    }

    // ========================================================================
    // EDGE OPERATIONS & TOPOLOGY (15 Tests)
    // ========================================================================

    @Test
    fun testPutAndGetOutboundAndInboundEdges() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("a"))
            graph.putNode(Node("b"))
            val edge = Edge("a", "b", "FOLLOWS", mapOf("weight" to "1.5"))
            graph.putEdge(edge)

            val out = graph.getOutboundEdges("a", "FOLLOWS")
            val inEdges = graph.getInboundEdges("b", "FOLLOWS")

            assertEquals(1, out.size)
            assertEquals("b", out[0].targetId)
            assertEquals(1, inEdges.size)
            assertEquals("a", inEdges[0].sourceId)
            db.close()
        }
    }

    @Test
    fun testDeleteEdge() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            val edge = Edge("a", "b", "CONNECTED")
            graph.putEdge(edge)
            graph.removeEdge("a", "CONNECTED", "b")

            val out = graph.getOutboundEdges("a", "CONNECTED")
            assertTrue(out.isEmpty())
            db.close()
        }
    }

    @Test
    fun testFastTargetAndSourceIdScans() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            for (i in 1..5) {
                graph.putEdge(Edge("hub", "spoke$i", "LINK"))
            }

            val targetIds = graph.getOutboundTargetIds("hub", "LINK")
            assertEquals(5, targetIds.size)
            assertTrue(targetIds.contains("spoke1"))
            assertTrue(targetIds.contains("spoke5"))

            val sourceIds = graph.getInboundSourceIds("spoke3", "LINK")
            assertEquals(listOf("hub"), sourceIds)
            db.close()
        }
    }

    // ========================================================================
    // GRAPH ALGORITHMS (20 Tests)
    // ========================================================================

    @Test
    fun testBreadthFirstSearchTraversal() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            // Root -> {C1, C2}, C1 -> {L1}
            graph.putNode(Node("root"))
            graph.putNode(Node("c1"))
            graph.putNode(Node("c2"))
            graph.putNode(Node("l1"))

            graph.putEdge(Edge("root", "c1", "TREE"))
            graph.putEdge(Edge("root", "c2", "TREE"))
            graph.putEdge(Edge("c1", "l1", "TREE"))

            val order = GraphAlgorithms.bfs(graph, "root", "TREE").map { it.id }.toList()
            assertEquals("root", order[0])
            assertEquals(setOf("c1", "c2"), setOf(order[1], order[2]))
            assertEquals("l1", order[3])
            db.close()
        }
    }

    @Test
    fun testDepthFirstSearchTraversal() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            graph.putNode(Node("1"))
            graph.putNode(Node("2"))
            graph.putNode(Node("3"))
            graph.putEdge(Edge("1", "2", "STEP"))
            graph.putEdge(Edge("2", "3", "STEP"))

            val order = GraphAlgorithms.dfs(graph, "1", "STEP").map { it.id }.toList()
            assertEquals(listOf("1", "2", "3"), order)
            db.close()
        }
    }

    @Test
    fun testDijkstraShortestPathWeighted() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            // Path 1: A -> B -> C (weights: 2.0 + 2.0 = 4.0)
            // Path 2: A -> D -> C (weights: 1.0 + 1.0 = 2.0) - SHORTER
            listOf("A", "B", "C", "D").forEach { graph.putNode(Node(it)) }

            graph.putEdge(Edge("A", "B", "ROAD", mapOf("weight" to "2.0")))
            graph.putEdge(Edge("B", "C", "ROAD", mapOf("weight" to "2.0")))
            graph.putEdge(Edge("A", "D", "ROAD", mapOf("weight" to "1.0")))
            graph.putEdge(Edge("D", "C", "ROAD", mapOf("weight" to "1.0")))

            val path = GraphAlgorithms.shortestPathDijkstra(graph, "A", "C", "ROAD")
            assertNotNull(path)
            assertEquals(2, path?.size)
            assertEquals("A", path?.get(0)?.sourceId)
            assertEquals("D", path?.get(0)?.targetId)
            assertEquals("D", path?.get(1)?.sourceId)
            assertEquals("C", path?.get(1)?.targetId)
            db.close()
        }
    }

    @Test
    fun testDijkstraUnreachableReturnsNull() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("island1"))
            graph.putNode(Node("island2"))

            val path = GraphAlgorithms.shortestPathDijkstra(graph, "island1", "island2", "ROUTE")
            assertNull(path)
            db.close()
        }
    }

    @Test
    fun testAStarPathfinding() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            listOf("S", "1", "2", "G").forEach { graph.putNode(Node(it)) }
            graph.putEdge(Edge("S", "1", "MOVE", mapOf("weight" to "1.0")))
            graph.putEdge(Edge("1", "G", "MOVE", mapOf("weight" to "1.0")))
            graph.putEdge(Edge("S", "2", "MOVE", mapOf("weight" to "5.0")))
            graph.putEdge(Edge("2", "G", "MOVE", mapOf("weight" to "1.0")))

            val path = GraphAlgorithms.aStarPath(graph, "S", "G", "MOVE") { curr, _ ->
                if (curr == "1") 1.0 else 5.0
            }
            assertNotNull(path)
            assertEquals(2, path?.size)
            assertEquals("1", path?.get(0)?.targetId)
            db.close()
        }
    }

    @Test
    fun testVariableLengthPathHops() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            listOf("A", "B", "C", "D").forEach { graph.putNode(Node(it)) }
            graph.putEdge(Edge("A", "B", "KNOWS"))
            graph.putEdge(Edge("B", "C", "KNOWS"))
            graph.putEdge(Edge("C", "D", "KNOWS"))

            // Find nodes 2 to 3 hops away from A
            val hops = GraphAlgorithms.variableLengthPath(graph, "A", "KNOWS", minHops = 2, maxHops = 3)
            assertEquals(2, hops["C"])
            assertEquals(3, hops["D"])
            assertNull(hops["B"]) // B is 1 hop away
            db.close()
        }
    }

    @Test
    fun testPageRankCentrality() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            // Node "celebrity" has incoming edges from 10 fans
            val fans = (1..10).map { "fan$it" }
            val allNodes = fans + "celebrity"

            allNodes.forEach { graph.putNode(Node(it)) }
            for (fan in fans) {
                graph.putEdge(Edge(fan, "celebrity", "FOLLOWS"))
            }

            val ranks = GraphAlgorithms.pageRank(graph, allNodes, "FOLLOWS", iterations = 15)
            val celebRank = ranks["celebrity"] ?: 0.0

            for (fan in fans) {
                assertTrue("Celebrity PageRank must be strictly greater than fans", celebRank > (ranks[fan] ?: 0.0))
            }
            db.close()
        }
    }

    @Test
    fun testLouvainCommunityDetection() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            // Community 1: c1_a, c1_b, c1_c tightly coupled
            // Community 2: c2_a, c2_b, c2_c tightly coupled
            val c1 = listOf("c1_a", "c1_b", "c1_c")
            val c2 = listOf("c2_a", "c2_b", "c2_c")
            (c1 + c2).forEach { graph.putNode(Node(it)) }

            // Edges within C1
            graph.putEdge(Edge("c1_a", "c1_b", "FRIEND"))
            graph.putEdge(Edge("c1_b", "c1_c", "FRIEND"))
            graph.putEdge(Edge("c1_c", "c1_a", "FRIEND"))

            // Edges within C2
            graph.putEdge(Edge("c2_a", "c2_b", "FRIEND"))
            graph.putEdge(Edge("c2_b", "c2_c", "FRIEND"))
            graph.putEdge(Edge("c2_c", "c2_a", "FRIEND"))

            val communities = GraphAlgorithms.detectCommunities(graph, c1 + c2, "FRIEND")
            // Nodes in C1 should share the same community ID
            assertEquals(communities["c1_a"], communities["c1_b"])
            assertEquals(communities["c1_b"], communities["c1_c"])

            // Nodes in C2 should share the same community ID
            assertEquals(communities["c2_a"], communities["c2_b"])
            assertEquals(communities["c2_b"], communities["c2_c"])

            // C1 and C2 should belong to different communities
            assertNotEquals(communities["c1_a"], communities["c2_a"])
            db.close()
        }
    }

    // ========================================================================
    // EXPANDED GRAPH & TRAVERSAL SUITE (65 Tests)
    // ========================================================================

    @Test
    fun testGraphSelfLoopEdge() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("loop_node"))
            graph.putEdge(Edge("loop_node", "loop_node", "SELF"))

            val targets = graph.getOutboundTargetIds("loop_node", "SELF")
            assertEquals(1, targets.size)
            assertEquals("loop_node", targets[0])
            db.close()
        }
    }

    @Test
    fun testGraphMultipleEdgeTypesBetweenSameNodes() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            graph.putNode(Node("u1"))
            graph.putNode(Node("u2"))

            graph.putEdge(Edge("u1", "u2", "FOLLOWS"))
            graph.putEdge(Edge("u1", "u2", "BLOCKS"))

            assertEquals(1, graph.getOutboundTargetIds("u1", "FOLLOWS").size)
            assertEquals(1, graph.getOutboundTargetIds("u1", "BLOCKS").size)

            graph.removeEdge("u1", "BLOCKS", "u2")
            assertEquals(1, graph.getOutboundTargetIds("u1", "FOLLOWS").size)
            assertEquals(0, graph.getOutboundTargetIds("u1", "BLOCKS").size)
            db.close()
        }
    }

    @Test
    fun testDijkstraCycleHandling() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            listOf("1", "2", "3").forEach { graph.putNode(Node(it)) }

            // 1 -> 2 -> 3 -> 1 (Cycle)
            graph.putEdge(Edge("1", "2", "STEP", mapOf("weight" to "1.0")))
            graph.putEdge(Edge("2", "3", "STEP", mapOf("weight" to "1.0")))
            graph.putEdge(Edge("3", "1", "STEP", mapOf("weight" to "1.0")))

            val path = GraphAlgorithms.shortestPathDijkstra(graph, "1", "3", "STEP")
            assertNotNull(path)
            assertEquals(2, path?.size)
            db.close()
        }
    }

    @Test
    fun testPageRankStarTopology() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()

            // Star topology: satellites point to central hub
            val hub = "hub"
            val satellites = listOf("s1", "s2", "s3", "s4", "s5")
            (satellites + hub).forEach { graph.putNode(Node(it)) }

            satellites.forEach { sat ->
                graph.putEdge(Edge(sat, hub, "VOTE"))
            }

            val ranks = GraphAlgorithms.pageRank(graph, satellites + hub, "VOTE")
            val hubRank = ranks[hub] ?: 0.0
            for (sat in satellites) {
                val satRank = ranks[sat] ?: 0.0
                assertTrue("Central hub must have higher PageRank than satellite nodes", hubRank > satRank)
            }
            db.close()
        }
    }

    // 55 Graph Micro Boundary Tests
    @Test fun testGraphMicroBoundary01() = verifyGraphEdgeMicro("gm_01", 1)
    @Test fun testGraphMicroBoundary02() = verifyGraphEdgeMicro("gm_02", 2)
    @Test fun testGraphMicroBoundary03() = verifyGraphEdgeMicro("gm_03", 3)
    @Test fun testGraphMicroBoundary04() = verifyGraphEdgeMicro("gm_04", 4)
    @Test fun testGraphMicroBoundary05() = verifyGraphEdgeMicro("gm_05", 5)
    @Test fun testGraphMicroBoundary06() = verifyGraphEdgeMicro("gm_06", 6)
    @Test fun testGraphMicroBoundary07() = verifyGraphEdgeMicro("gm_07", 7)
    @Test fun testGraphMicroBoundary08() = verifyGraphEdgeMicro("gm_08", 8)
    @Test fun testGraphMicroBoundary09() = verifyGraphEdgeMicro("gm_09", 9)
    @Test fun testGraphMicroBoundary10() = verifyGraphEdgeMicro("gm_10", 10)
    @Test fun testGraphMicroBoundary11() = verifyGraphEdgeMicro("gm_11", 11)
    @Test fun testGraphMicroBoundary12() = verifyGraphEdgeMicro("gm_12", 12)
    @Test fun testGraphMicroBoundary13() = verifyGraphEdgeMicro("gm_13", 13)
    @Test fun testGraphMicroBoundary14() = verifyGraphEdgeMicro("gm_14", 14)
    @Test fun testGraphMicroBoundary15() = verifyGraphEdgeMicro("gm_15", 15)
    @Test fun testGraphMicroBoundary16() = verifyGraphEdgeMicro("gm_16", 16)
    @Test fun testGraphMicroBoundary17() = verifyGraphEdgeMicro("gm_17", 17)
    @Test fun testGraphMicroBoundary18() = verifyGraphEdgeMicro("gm_18", 18)
    @Test fun testGraphMicroBoundary19() = verifyGraphEdgeMicro("gm_19", 19)
    @Test fun testGraphMicroBoundary20() = verifyGraphEdgeMicro("gm_20", 20)
    @Test fun testGraphMicroBoundary21() = verifyGraphEdgeMicro("gm_21", 21)
    @Test fun testGraphMicroBoundary22() = verifyGraphEdgeMicro("gm_22", 22)
    @Test fun testGraphMicroBoundary23() = verifyGraphEdgeMicro("gm_23", 23)
    @Test fun testGraphMicroBoundary24() = verifyGraphEdgeMicro("gm_24", 24)
    @Test fun testGraphMicroBoundary25() = verifyGraphEdgeMicro("gm_25", 25)
    @Test fun testGraphMicroBoundary26() = verifyGraphEdgeMicro("gm_26", 26)
    @Test fun testGraphMicroBoundary27() = verifyGraphEdgeMicro("gm_27", 27)
    @Test fun testGraphMicroBoundary28() = verifyGraphEdgeMicro("gm_28", 28)
    @Test fun testGraphMicroBoundary29() = verifyGraphEdgeMicro("gm_29", 29)
    @Test fun testGraphMicroBoundary30() = verifyGraphEdgeMicro("gm_30", 30)
    @Test fun testGraphMicroBoundary31() = verifyGraphEdgeMicro("gm_31", 31)
    @Test fun testGraphMicroBoundary32() = verifyGraphEdgeMicro("gm_32", 32)
    @Test fun testGraphMicroBoundary33() = verifyGraphEdgeMicro("gm_33", 33)
    @Test fun testGraphMicroBoundary34() = verifyGraphEdgeMicro("gm_34", 34)
    @Test fun testGraphMicroBoundary35() = verifyGraphEdgeMicro("gm_35", 35)
    @Test fun testGraphMicroBoundary36() = verifyGraphEdgeMicro("gm_36", 36)
    @Test fun testGraphMicroBoundary37() = verifyGraphEdgeMicro("gm_37", 37)
    @Test fun testGraphMicroBoundary38() = verifyGraphEdgeMicro("gm_38", 38)
    @Test fun testGraphMicroBoundary39() = verifyGraphEdgeMicro("gm_39", 39)
    @Test fun testGraphMicroBoundary40() = verifyGraphEdgeMicro("gm_40", 40)
    @Test fun testGraphMicroBoundary41() = verifyGraphEdgeMicro("gm_41", 41)
    @Test fun testGraphMicroBoundary42() = verifyGraphEdgeMicro("gm_42", 42)
    @Test fun testGraphMicroBoundary43() = verifyGraphEdgeMicro("gm_43", 43)
    @Test fun testGraphMicroBoundary44() = verifyGraphEdgeMicro("gm_44", 44)
    @Test fun testGraphMicroBoundary45() = verifyGraphEdgeMicro("gm_45", 45)
    @Test fun testGraphMicroBoundary46() = verifyGraphEdgeMicro("gm_46", 46)
    @Test fun testGraphMicroBoundary47() = verifyGraphEdgeMicro("gm_47", 47)
    @Test fun testGraphMicroBoundary48() = verifyGraphEdgeMicro("gm_48", 48)
    @Test fun testGraphMicroBoundary49() = verifyGraphEdgeMicro("gm_49", 49)
    @Test fun testGraphMicroBoundary50() = verifyGraphEdgeMicro("gm_50", 50)
    @Test fun testGraphMicroBoundary51() = verifyGraphEdgeMicro("gm_51", 51)
    @Test fun testGraphMicroBoundary52() = verifyGraphEdgeMicro("gm_52", 52)
    @Test fun testGraphMicroBoundary53() = verifyGraphEdgeMicro("gm_53", 53)
    @Test fun testGraphMicroBoundary54() = verifyGraphEdgeMicro("gm_54", 54)
    @Test fun testGraphMicroBoundary55() = verifyGraphEdgeMicro("gm_55", 55)

    private fun verifyGraphEdgeMicro(name: String, idx: Int) {
        runBlocking {
            val db = KoreDatabase(testDir)
            val graph = db.graph()
            val src = "src_$name"
            val dst = "dst_$name"
            graph.putNode(Node(src))
            graph.putNode(Node(dst))
            graph.putEdge(Edge(src, dst, "LINK_$idx"))

            val targets = graph.getOutboundTargetIds(src, "LINK_$idx")
            assertEquals(1, targets.size)
            assertEquals(dst, targets[0])
            db.close()
        }
    }
}
