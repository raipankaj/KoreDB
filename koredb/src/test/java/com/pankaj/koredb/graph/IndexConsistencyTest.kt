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
 * Test Suite for Graph Index Consistency.
 *
 * Focuses on the "Update" scenario:
 * When a node's property changes, the old index entry becomes stale.
 * The system must ensuring that queries don't return false positives based on stale indices.
 */
class IndexConsistencyTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_idx_consistency_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * BUG PROOF: Verifies if changing a property leaves a stale index entry that causes false positive lookups.
     *
     * Scenario:
     * 1. Create User in "London".
     * 2. Index created for "city:London".
     * 3. Update User to "Paris".
     * 4. Index created for "city:Paris".
     * 5. BUT "city:London" index remains!
     * 6. Searching for "London" returns the user (False Positive), even though they live in Paris now.
     */
    @Test
    fun `test Index Stale Entry Handling`() = runBlocking {
        val graph = db.graph()
        
        // 1. Initial State: Alice in London
        val v1 = Node("alice", setOf("Person"), mapOf("city" to "London"))
        graph.putNode(v1)

        val londoners1 = graph.getNodesByProperty("Person", "city", "London")
        assertEquals(1, londoners1.size)
        assertEquals("alice", londoners1[0].id)

        // 2. Update: Alice moves to Paris
        val v2 = Node("alice", setOf("Person"), mapOf("city" to "Paris"))
        graph.putNode(v2)

        // 3. Verify Paris index works
        val parisians = graph.getNodesByProperty("Person", "city", "Paris")
        assertEquals("Should be found in Paris", 1, parisians.size)
        assertEquals("alice", parisians[0].id)

        // 4. Verify London index (Should be empty or filtered)
        // If the system doesn't clean up old indices on write, it must filter them on read.
        val londoners2 = graph.getNodesByProperty("Person", "city", "London")
        
        // If this fails, it means we have a Ghost Index bug.
        // We expect the graph engine to check the node's ACTUAL property before returning.
        val falsePositives = londoners2.filter { it.properties["city"] != "London" }
        
        assertTrue("Found ${falsePositives.size} false positives in index result! Stale index returned nodes that no longer match.", falsePositives.isEmpty())
        
        // Optionally, strict check:
        // assertEquals("London search should be empty", 0, londoners2.size)
    }
}
