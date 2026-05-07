package com.pankaj.koredb

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.pankaj.koredb.graph.*
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.graph.query.query
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.system.measureTimeMillis

@RunWith(AndroidJUnit4::class)
class GraphPillarTest {
    private lateinit var app: MyApplication
    private lateinit var graph: GraphStorage

    @Before
    fun setup() = runBlocking {
        app = ApplicationProvider.getApplicationContext()
        app.roomDatabase.noteDao().clearEdges()
        graph = app.database.graph()
    }

    private fun buildSocialGraph(nodeCount: Int, edgesPerNode: Int = 5) = runBlocking {
        val nodes = (1..nodeCount).map { i ->
            Node("u$i", setOf("User"), mapOf("name" to "User$i", "city" to if (i % 3 == 0) "Tokyo" else "NYC"))
        }
        graph.putNodes(nodes)
        val edges = mutableListOf<Edge>()
        for (i in 1..nodeCount) {
            for (j in 1..edgesPerNode) {
                val target = ((i + j * 7) % nodeCount) + 1
                if (target != i) edges.add(Edge("u$i", "u$target", "FOLLOWS", mapOf("weight" to "${j * 0.5}")))
            }
        }
        graph.putEdges(edges)
    }

    @Test
    fun test_01_BatchNodeInsert() = runBlocking {
        println("\n🕸️ GRAPH: BATCH NODE INSERT (1000 nodes)")
        val nodes = (1..1000).map { Node("bn$it", setOf("User"), mapOf("name" to "User$it")) }
        val dao = app.roomDatabase.noteDao()

        val kore = measureTimeMillis { graph.putNodes(nodes) }
        val room = measureTimeMillis {
            dao.insertAll(nodes.map { Note(it.id, it.properties["name"]!!, "content") })
        }
        println("  INSERT (1000 nodes) → KoreDB: ${kore}ms | Room: ${room}ms")
    }

    @Test
    fun test_02_BatchEdgeInsert() = runBlocking {
        println("\n🕸️ GRAPH: BATCH EDGE INSERT (5000 edges)")
        val nodes = (1..500).map { Node("be$it", setOf("User"), mapOf("name" to "U$it")) }
        graph.putNodes(nodes)
        val edges = (1..5000).map { Edge("be${(it % 500) + 1}", "be${((it * 3) % 500) + 1}", "FOLLOWS") }

        val kore = measureTimeMillis { graph.putEdges(edges) }
        val room = measureTimeMillis {
            for (e in edges) app.roomDatabase.noteDao().insertEdge(EdgeEntity(e.sourceId, e.targetId, e.type))
        }
        println("  INSERT (5000 edges) → KoreDB: ${kore}ms | Room: ${room}ms")
    }

    @Test
    fun test_03_DeleteNodeCascade() = runBlocking {
        println("\n🕸️ GRAPH: CASCADING DELETE")
        buildSocialGraph(200, 5)

        val kore = measureTimeMillis {
            repeat(50) { graph.deleteNode("u${it + 1}") }
        }
        // Room: manual cascade
        val dao = app.roomDatabase.noteDao()
        dao.insertAll((1..200).map { Note("u$it", "U$it", "C") })
        val room = measureTimeMillis {
            repeat(50) { dao.deleteById("u${it + 1}") }
        }
        println("  CASCADE DELETE (x50) → KoreDB: ${kore}ms | Room: ${room}ms (no edge cascade)")

        // Verify: deleted node should not be retrievable
        val deleted = graph.getNode("u1")
        println("  Verification: u1 exists = ${deleted != null} (expected: false) ${if (deleted == null) "✅" else "❌"}")
    }

    @Test
    fun test_04_QueryDSL() = runBlocking {
        println("\n🕸️ GRAPH: QUERY DSL")
        buildSocialGraph(500, 5)

        val kore = measureTimeMillis {
            repeat(50) {
                graph.query {
                    startingWith("User", "city", "Tokyo")
                    outbound("FOLLOWS")
                }.toIdList()
            }
        }
        println("  QUERY DSL (x50) → KoreDB: ${kore}ms (Room: N/A - no graph DSL)")
    }

    @Test
    fun test_05_VariableLengthPath() = runBlocking {
        println("\n🕸️ GRAPH: VARIABLE-LENGTH PATH (2-4 hops)")
        buildSocialGraph(500, 5)

        val kore = measureTimeMillis {
            repeat(20) {
                graph.query {
                    startingWith("u1")
                    outboundRange("FOLLOWS", minHops = 2, maxHops = 4)
                }.toIdList()
            }
        }
        // Room equivalent: multi-join
        val room = measureTimeMillis {
            repeat(20) { app.roomDatabase.noteDao().getTwoHopNodes("u1", "FOLLOWS") }
        }
        println("  VAR-LENGTH PATH (x20) → KoreDB: ${kore}ms | Room 2-hop only: ${room}ms")
    }

    @Test
    fun test_06_Dijkstra() = runBlocking {
        println("\n🕸️ GRAPH: DIJKSTRA SHORTEST PATH")
        buildSocialGraph(500, 5)

        val kore = measureTimeMillis {
            repeat(20) { GraphAlgorithms.shortestPathDijkstra(graph, "u1", "u250", "FOLLOWS") }
        }
        println("  DIJKSTRA (x20) → KoreDB: ${kore}ms (Room: N/A)")
    }

    @Test
    fun test_07_AStar() = runBlocking {
        println("\n🕸️ GRAPH: A* PATHFINDING")
        buildSocialGraph(500, 5)

        val kore = measureTimeMillis {
            repeat(20) {
                GraphAlgorithms.aStarPath(graph, "u1", "u250", "FOLLOWS") { _, _ -> 0.5 }
            }
        }
        println("  A* PATH (x20) → KoreDB: ${kore}ms (Room: N/A)")
    }

    @Test
    fun test_08_CommunityDetection() = runBlocking {
        println("\n🕸️ GRAPH: COMMUNITY DETECTION (Louvain)")
        buildSocialGraph(200, 5)
        val ids = (1..200).map { "u$it" }

        val kore = measureTimeMillis {
            val communities = GraphAlgorithms.detectCommunities(graph, ids, "FOLLOWS")
            val numCommunities = communities.values.distinct().size
            println("  Found $numCommunities communities")
        }
        println("  LOUVAIN (200 nodes) → KoreDB: ${kore}ms (Room: N/A)")
    }

    @Test
    fun test_09_ConnectedComponents() = runBlocking {
        println("\n🕸️ GRAPH: CONNECTED COMPONENTS")
        buildSocialGraph(200, 5)
        val ids = (1..200).map { "u$it" }

        val kore = measureTimeMillis {
            val comps = GraphAlgorithms.connectedComponents(graph, ids, "FOLLOWS")
            println("  Found ${comps.size} components")
        }
        println("  COMPONENTS (200 nodes) → KoreDB: ${kore}ms (Room: N/A)")
    }

    @Test
    fun test_10_PageRank() = runBlocking {
        println("\n🕸️ GRAPH: PAGERANK")
        buildSocialGraph(200, 5)
        val ids = (1..200).map { "u$it" }

        val kore = measureTimeMillis {
            val ranks = GraphAlgorithms.pageRank(graph, ids, "FOLLOWS")
            val top = ranks.entries.sortedByDescending { it.value }.take(3)
            println("  Top 3: ${top.map { "${it.key}=${String.format("%.4f", it.value)}" }}")
        }
        println("  PAGERANK (200 nodes) → KoreDB: ${kore}ms (Room: N/A)")
    }

    @Test
    fun test_11_GraphExport() = runBlocking {
        println("\n🕸️ GRAPH: EXPORT (DOT + GraphML)")
        buildSocialGraph(50, 3)
        val ids = (1..50).map { "u$it" }

        val dotTime = measureTimeMillis {
            val dot = GraphExport.toDot(graph, ids, "FOLLOWS")
            assert(dot.contains("digraph"))
            println("  DOT size: ${dot.length} chars")
        }
        val gmlTime = measureTimeMillis {
            val gml = GraphExport.toGraphML(graph, ids, "FOLLOWS")
            assert(gml.contains("<graphml"))
            println("  GraphML size: ${gml.length} chars")
        }
        println("  DOT: ${dotTime}ms | GraphML: ${gmlTime}ms")
    }

    @Test
    fun test_12_DegreeCentrality() = runBlocking {
        println("\n🕸️ GRAPH: DEGREE CENTRALITY")
        buildSocialGraph(200, 5)
        val ids = (1..200).map { "u$it" }

        val kore = measureTimeMillis {
            val centrality = GraphAlgorithms.degreeCentrality(graph, ids, "FOLLOWS")
            val top = centrality.entries.sortedByDescending { it.value }.take(3)
            println("  Top 3: ${top.map { "${it.key}=${String.format("%.4f", it.value)}" }}")
        }
        println("  DEGREE CENTRALITY → KoreDB: ${kore}ms (Room: N/A)")
    }

    @Test
    fun test_13_FullReport() = runBlocking {
        println("\n" + "═".repeat(55))
        println("  🕸️ GRAPH PILLAR — FULL REPORT (500 nodes, 2500 edges)")
        println("═".repeat(55))
        buildSocialGraph(500, 5)
        val ids = (1..500).map { "u$it" }

        val t1 = measureTimeMillis { repeat(50) { graph.getNode("u${(it % 500) + 1}") } }
        val t2 = measureTimeMillis { repeat(20) { graph.getOutboundEdges("u${it+1}", "FOLLOWS") } }
        val t3 = measureTimeMillis {
            repeat(20) { graph.query { startingWith("u1"); outbound("FOLLOWS") }.toIdList() }
        }
        val t4 = measureTimeMillis {
            repeat(10) { graph.query { startingWith("u1"); outboundRange("FOLLOWS", 2, 4) }.toIdList() }
        }
        val t5 = measureTimeMillis {
            repeat(10) { GraphAlgorithms.shortestPathDijkstra(graph, "u1", "u250", "FOLLOWS") }
        }
        val t6 = measureTimeMillis {
            repeat(10) { GraphAlgorithms.aStarPath(graph, "u1", "u250", "FOLLOWS") { _, _ -> 0.5 } }
        }
        val t7 = measureTimeMillis { GraphAlgorithms.pageRank(graph, ids, "FOLLOWS") }
        val t8 = measureTimeMillis { GraphAlgorithms.detectCommunities(graph, ids, "FOLLOWS") }

        println("  ┌──────────────────────────┬──────────┐")
        println("  │ Operation                │ KoreDB   │")
        println("  ├──────────────────────────┼──────────┤")
        println("  │ Node Lookup (x50)        │ ${t1.toString().padStart(5)}ms  │")
        println("  │ Edge Query (x20)         │ ${t2.toString().padStart(5)}ms  │")
        println("  │ 1-Hop Traversal (x20)    │ ${t3.toString().padStart(5)}ms  │")
        println("  │ 2-4 Hop Path (x10)       │ ${t4.toString().padStart(5)}ms  │")
        println("  │ Dijkstra (x10)           │ ${t5.toString().padStart(5)}ms  │")
        println("  │ A* Path (x10)            │ ${t6.toString().padStart(5)}ms  │")
        println("  │ PageRank (500 nodes)     │ ${t7.toString().padStart(5)}ms  │")
        println("  │ Communities (500 nodes)  │ ${t8.toString().padStart(5)}ms  │")
        println("  └──────────────────────────┴──────────┘")
    }
}
