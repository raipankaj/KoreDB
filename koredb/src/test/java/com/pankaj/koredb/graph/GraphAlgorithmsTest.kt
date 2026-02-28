package com.pankaj.koredb.graph

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

class GraphAlgorithmsTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_algo_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test PageRank with Cycles`() = runBlocking {
        val graph = db.graph()
        // A <-> B (Cycle)
        graph.transaction {
            putNode(Node("A"))
            putNode(Node("B"))
            putEdge(Edge("A", "B", "LINK"))
            putEdge(Edge("B", "A", "LINK"))
        }

        val scores = GraphAlgorithms.pageRank(graph, listOf("A", "B"), "LINK", iterations = 20)
        
        // In a symmetric 2-node cycle, PageRank should be equal (approx 0.5 each)
        val scoreA = scores["A"] ?: 0.0
        val scoreB = scores["B"] ?: 0.0

        assertEquals(scoreA, scoreB, 0.01)
        assertTrue(scoreA > 0.4)
    }

    @Test
    fun `test Disconnected Components`() = runBlocking {
        val graph = db.graph()
        // A->B, C->D (Isolated)
        graph.transaction {
            putNode(Node("A")); putNode(Node("B"))
            putNode(Node("C")); putNode(Node("D"))
            putEdge(Edge("A", "B", "LINK"))
            putEdge(Edge("C", "D", "LINK"))
        }

        val bfsFromA = GraphAlgorithms.bfs(graph, "A", "LINK").map { it.id }.toList()
        assertTrue(bfsFromA.contains("B"))
        assertFalse(bfsFromA.contains("C"))
        assertFalse(bfsFromA.contains("D"))
    }
}
