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

class GraphTransactionsTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_transactions_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test transaction startingWith label reachable`() = runBlocking {
        val graph = db.graph()

        // A -> B
        graph.transaction {
            putNode(Node("A", labels = setOf("Label"), properties = mapOf("a" to "a")))
            putNode(Node("B", labels = setOf("Label"), properties = mapOf("b" to "b")))
            putEdge(Edge("A", "B", "LINK"))
        }
        val destNodes = graph.query()
            .startingWith("Label", "a", "a")
            .outbound("LINK")
            .toNodeList()

        assertEquals(destNodes.size, 1)
        assertEquals(destNodes.first().id, "B")
    }
}
