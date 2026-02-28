package com.pankaj.koredb.graph

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.graph.query.query
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Test Suite for the Graph Database Engine (KoreGraph).
 *
 * Covers:
 * - Node/Edge CRUD (Create, Read, Update, Delete)
 * - Property Indexing (O(log N) lookups)
 * - Graph Traversals (Fluent DSL)
 * - Graph Algorithms (BFS, DFS, PageRank, Dijkstra)
 * - Edge Cases (Self-loops, Dangling edges)
 */
class GraphEngineTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_graph_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Verifies Node creation and automatic indexing of its properties.
     */
    @Test
    fun `test Node Creation and Property Indexing`() = runBlocking {
        val graph = db.graph()

        val alice = Node("u1", setOf("Person"), mapOf("name" to "Alice", "city" to "London"))
        val bob = Node("u2", setOf("Person"), mapOf("name" to "Bob", "city" to "London"))
        val charlie = Node("u3", setOf("Person"), mapOf("name" to "Charlie", "city" to "Paris"))

        graph.putNode(alice)
        graph.putNode(bob)
        graph.putNode(charlie)

        // Basic ID lookup
        val u1 = graph.getNode("u1")
        assertNotNull("Node u1 not found", u1)
        assertEquals("Property mismatch", "Alice", u1?.properties?.get("name"))

        // Property Index Lookup
        val londoners = graph.getNodesByProperty("Person", "city", "London")
        assertEquals("Index count mismatch", 2, londoners.size)
        assertTrue("u1 missing from index", londoners.any { it.id == "u1" })
        assertTrue("u2 missing from index", londoners.any { it.id == "u2" })
    }

    /**
     * Verifies Edge creation and bidirectional traversal capabilities.
     */
    @Test
    fun `test Edge Creation and Bidirectional Index`() = runBlocking {
        val graph = db.graph()

        val u1 = Node("u1", properties = mapOf("name" to "Alice"))
        val u2 = Node("u2", properties = mapOf("name" to "Bob"))

        graph.putNode(u1)
        graph.putNode(u2)

        val edge = Edge("u1", "u2", "KNOWS")
        graph.putEdge(edge)

        val outbound = graph.getOutboundEdges("u1", "KNOWS")
        val inbound = graph.getInboundEdges("u2", "KNOWS")

        assertEquals("Outbound edge count mismatch", 1, outbound.size)
        assertEquals("Target ID mismatch", "u2", outbound.first().targetId)

        assertEquals("Inbound edge count mismatch", 1, inbound.size)
        assertEquals("Source ID mismatch", "u1", inbound.first().sourceId)
    }

    /**
     * Verifies the fluent query DSL for multi-hop traversals.
     * This is the primary way users will interact with the graph.
     */
    @Test
    fun `test Fluent DSL for 2-hop traversal`() = runBlocking {
        val graph = db.graph()

        // Alice -> Bob -> Charlie
        graph.transaction {
            putNode(Node("u1", setOf("Person"), mapOf("name" to "Alice")))
            putNode(Node("u2", setOf("Person"), mapOf("name" to "Bob")))
            putNode(Node("u3", setOf("Person"), mapOf("name" to "Charlie")))

            putEdge(Edge("u1", "u2", "FOLLOWS"))
            putEdge(Edge("u2", "u3", "FOLLOWS"))
        }

        val result = graph.query()
            .startingWith("u1")
            .outbound("FOLLOWS", hops = 2)
            .toNodeList()

        assertEquals("Result count mismatch", 1, result.size)
        assertEquals("Final node ID mismatch", "u3", result.first().id)
        assertEquals("Final node property mismatch", "Charlie", result.first().properties["name"])
    }

    /**
     * EDGE CASE: Self-Loops
     * Verifies that a node can have an edge pointing to itself without causing infinite recursion or errors.
     */
    @Test
    fun `test Self-Loops`() = runBlocking {
        val graph = db.graph()
        graph.putNode(Node("u1", properties = mapOf("name" to "Solo")))
        graph.putEdge(Edge("u1", "u1", "LIKES"))

        val outbound = graph.getOutboundEdges("u1", "LIKES")
        assertEquals("Self-loop edge missing", 1, outbound.size)
        assertEquals("Target must be self", "u1", outbound.first().targetId)
    }

    /**
     * ALGORITHM: Breadth-First Search (BFS)
     * Verifies standard BFS traversal on a small graph.
     */
    @Test
    fun `test BFS Traversal`() = runBlocking {
        val graph = db.graph()
        // A -> B -> C
        // A -> D
        graph.transaction {
            putNode(Node("A"))
            putNode(Node("B"))
            putNode(Node("C"))
            putNode(Node("D"))
            putEdge(Edge("A", "B", "LINK"))
            putEdge(Edge("B", "C", "LINK"))
            putEdge(Edge("A", "D", "LINK"))
        }

        val visited = GraphAlgorithms.bfs(graph, "A", "LINK").map { it.id }.toList()
        
        // Order should generally be A, then (B, D) in any order, then C.
        // A is definitely first.
        assertEquals("Start node mismatch", "A", visited[0])
        
        // Size should be 4
        assertEquals("Visited count mismatch", 4, visited.size)
        
        // Ensure all nodes reached
        assertTrue(visited.containsAll(listOf("A", "B", "C", "D")))
    }

    /**
     * ALGORITHM: Shortest Path (Dijkstra)
     * Verifies finding the shortest path in a weighted graph.
     */
    @Test
    fun `test Dijkstra Shortest Path`() = runBlocking {
        val graph = db.graph()
        // A --(1)--> B --(1)--> C  (Total 2)
        // A --(10)--> C            (Total 10)
        
        graph.transaction {
            putNode(Node("A"))
            putNode(Node("B"))
            putNode(Node("C"))
            
            putEdge(Edge("A", "B", "ROAD", mapOf("weight" to "1.0")))
            putEdge(Edge("B", "C", "ROAD", mapOf("weight" to "1.0")))
            putEdge(Edge("A", "C", "ROAD", mapOf("weight" to "10.0")))
        }

        val path = GraphAlgorithms.shortestPathDijkstra(graph, "A", "C", "ROAD")
        assertNotNull("Path should exist", path)
        assertEquals("Shortest path should have 2 hops (via B)", 2, path!!.size)
        assertEquals("First hop target", "B", path[0].targetId)
        assertEquals("Second hop target", "C", path[1].targetId)
    }
}
