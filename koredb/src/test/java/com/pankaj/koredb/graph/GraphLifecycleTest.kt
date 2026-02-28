package com.pankaj.koredb.graph

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Validates the full lifecycle of graph elements.
 *
 * Covers:
 * - Edge Removal (Bi-directional cleanup)
 * - Edge Property Updates
 */
class GraphLifecycleTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_graph_life_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Edge Removal`() = runBlocking {
        val graph = db.graph()
        
        graph.putNode(Node("u1"))
        graph.putNode(Node("u2"))
        graph.putEdge(Edge("u1", "u2", "KNOWS"))

        // Verify Exists
        assertEquals(1, graph.getOutboundEdges("u1", "KNOWS").size)
        assertEquals(1, graph.getInboundEdges("u2", "KNOWS").size)

        // Remove
        graph.removeEdge("u1", "KNOWS", "u2")

        // Verify Gone
        assertEquals("Outbound edge should be gone", 0, graph.getOutboundEdges("u1", "KNOWS").size)
        assertEquals("Inbound edge should be gone", 0, graph.getInboundEdges("u2", "KNOWS").size)
    }

    @Test
    fun `test Edge Property Update`() = runBlocking {
        val graph = db.graph()
        
        graph.putNode(Node("A"))
        graph.putNode(Node("B"))
        
        // 1. Create with weight 1.0
        graph.putEdge(Edge("A", "B", "ROAD", mapOf("weight" to "1.0")))
        
        var edge = graph.getOutboundEdges("A", "ROAD").first()
        assertEquals("1.0", edge.properties["weight"])

        // 2. Update to weight 5.0 (Overwrite)
        graph.putEdge(Edge("A", "B", "ROAD", mapOf("weight" to "5.0")))
        
        edge = graph.getOutboundEdges("A", "ROAD").first()
        assertEquals("Weight should be updated", "5.0", edge.properties["weight"])
        
        // Also verify inbound update
        val inEdge = graph.getInboundEdges("B", "ROAD").first()
        assertEquals("Inbound weight should be updated", "5.0", inEdge.properties["weight"])
    }
}
