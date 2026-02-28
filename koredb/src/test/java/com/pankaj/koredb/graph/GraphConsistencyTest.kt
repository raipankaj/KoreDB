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
 * Test Suite for Graph Database Consistency and Transactional Integrity.
 *
 * Covers:
 * - Transaction Rollback (Atomicity)
 * - Ghost Index Entries (Node deletion vs Index deletion)
 * - Dangling Edge references
 */
class GraphConsistencyTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_consistency_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Verifies that a failed transaction rolls back all changes.
     * Partial writes (e.g., node created but edge failed) must not persist.
     */
    @Test
    fun `test Transaction Rollback on Failure`() = runBlocking {
        val graph = db.graph()

        try {
            graph.transaction {
                putNode(Node("A"))
                putNode(Node("B"))
                throw RuntimeException("Simulated Failure")
            }
        } catch (e: Exception) {
            // Expected exception
        }

        assertNull("Node A should not exist after rollback", graph.getNode("A"))
        assertNull("Node B should not exist after rollback", graph.getNode("B"))
    }

    /**
     * CRITICAL BUG TEST: Ghost Index Entries.
     * If we delete a node using raw deletion (since GraphStorage lacks removeNode),
     * the property indices (e.g., "city=London") might still point to it.
     *
     * This test exposes whether the system is robust against index inconsistencies.
     */
    @Test
    fun `test Ghost Index Consistency`() = runBlocking {
        val graph = db.graph()
        val node = Node("ghost", setOf("Person"), mapOf("city" to "London"))
        
        graph.putNode(node)

        // Verify index works initially
        var results = graph.getNodesByProperty("Person", "city", "London")
        assertEquals(1, results.size)

        // Simulate raw deletion of the node (bypassing any hypothetical index cleanup logic)
        // Currently GraphStorage has no removeNode(), so users might do this manually.
        db.deleteRaw("g:v:ghost".toByteArray())

        // Verify the primary lookup returns null
        assertNull(graph.getNode("ghost"))

        // Query the index again. Ideally, it should handle the missing node gracefully 
        // (e.g., return empty list) rather than returning a null/invalid entry.
        results = graph.getNodesByProperty("Person", "city", "London")
        
        // If results is NOT empty, we have a "Ghost Index" pointing to a deleted node.
        // The implementation of getNodesByProperty uses `mapNotNull { getNode(id) }`.
        // So it *should* filter out the missing node automatically.
        assertEquals("Ghost index entry should be filtered out", 0, results.size)
    }

    /**
     * Verifies handling of edges pointing to non-existent nodes (Dangling Edges).
     * The system should not crash during traversal.
     */
    @Test
    fun `test Dangling Edge Traversal`() = runBlocking {
        val graph = db.graph()
        
        // Create edge A -> B, but never create Node B
        graph.putNode(Node("A"))
        graph.putEdge(Edge("A", "B", "LINK"))

        // Traverse A -> B
        val neighbors = graph.getOutboundTargetIds("A", "LINK")
        assertEquals(1, neighbors.size)
        assertEquals("B", neighbors[0])

        // Attempt to load Node B
        val nodeB = graph.getNode("B")
        assertNull("Node B should not exist", nodeB)
    }
}
