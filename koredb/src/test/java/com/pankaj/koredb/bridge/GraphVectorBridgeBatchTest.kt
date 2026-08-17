package com.pankaj.koredb.bridge

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

class GraphVectorBridgeBatchTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_bridge_batch_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Batch Vector and Node Lookups in Bridge`() = runBlocking {
        val graph = db.graph()
        val vectorCol = db.vectorCollection("products")

        // 1. Insert Nodes and Vectors
        val nodeA = Node("prod_1", setOf("Product"), mapOf("brand" to "Nike", "price" to "100"))
        val nodeB = Node("prod_2", setOf("Product"), mapOf("brand" to "Adidas", "price" to "120"))
        val nodeC = Node("prod_3", setOf("Product"), mapOf("brand" to "Nike", "price" to "90"))
        val user = Node("user_1", setOf("User"), emptyMap())

        graph.putNodes(listOf(nodeA, nodeB, nodeC, user))
        graph.putEdge(Edge("user_1", "prod_1", "PURCHASED"))
        graph.putEdge(Edge("user_1", "prod_2", "PURCHASED"))
        graph.putEdge(Edge("user_1", "prod_3", "PURCHASED"))

        val vecA = floatArrayOf(1.0f, 0.0f, 0.0f)
        val vecB = floatArrayOf(0.0f, 1.0f, 0.0f)
        val vecC = floatArrayOf(0.9f, 0.1f, 0.0f)

        vectorCol.insert("prod_1", vecA)
        vectorCol.insert("prod_2", vecB)
        vectorCol.insert("prod_3", vecC)
        vectorCol.waitForIndexing()

        // 2. Test Batch Vector Retrieval on KoreVectorCollection
        val batchVectors = vectorCol.getBatchVectors(listOf("prod_1", "prod_2", "prod_3", "nonexistent"))
        assertEquals(3, batchVectors.size)
        assertArrayEquals(vecA, batchVectors["prod_1"], 0.001f)
        assertArrayEquals(vecB, batchVectors["prod_2"], 0.001f)
        assertArrayEquals(vecC, batchVectors["prod_3"], 0.001f)
        assertNull(batchVectors["nonexistent"])

        // 3. Test Batch Node Retrieval on GraphStorage
        val batchNodes = graph.getNodes(listOf("prod_1", "prod_2", "prod_3", "nonexistent"))
        assertEquals(3, batchNodes.size)
        assertEquals("Nike", batchNodes["prod_1"]?.properties?.get("brand"))
        assertEquals("Adidas", batchNodes["prod_2"]?.properties?.get("brand"))

        // 4. Test Graph-First Traversal and Batch Reranking
        val bridge = db.graphVectorBridge(vectorCol)
        val query = floatArrayOf(1.0f, 0.0f, 0.0f)
        val reranked = bridge.graphTraversal("user_1", "PURCHASED", hops = 1)
            .rerankByVector(query)

        assertEquals(3, reranked.size)
        assertEquals("prod_1", reranked[0].id)
        assertEquals("prod_3", reranked[1].id)
        assertEquals("prod_2", reranked[2].id)

        // Verify Node metadata is attached properly in batch
        assertEquals("Nike", reranked[0].node?.properties?.get("brand"))
        assertEquals("Nike", reranked[1].node?.properties?.get("brand"))
        assertEquals("Adidas", reranked[2].node?.properties?.get("brand"))

        // 5. Test Vector-First Search with Graph Enrichment in batch
        val vectorFirst = bridge.vectorSearch(query, limit = 10)
            .enrichWithGraph()

        assertTrue(vectorFirst.isNotEmpty())
        for (item in vectorFirst) {
            assertNotNull(item.node)
        }
    }
}
