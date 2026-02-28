package com.pankaj.koredb.graph

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.system.measureTimeMillis

/**
 * Validates Graph performance with Supernodes (high-degree vertices).
 */
class GraphSupernodeTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_supernode_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Supernode with 10k Edges`() = runBlocking {
        val graph = db.graph()
        val CELEBRITY = "celeb_1"
        val FOLLOWERS_COUNT = 10_000

        // 1. Bulk create 10k followers
        val time = measureTimeMillis {
            graph.transaction {
                putNode(Node(CELEBRITY, setOf("Person")))
                for (i in 1..FOLLOWERS_COUNT) {
                    val followerId = "u$i"
                    // Optimization: We don't necessarily need to create the Node object 
                    // if we only care about edge traversal integrity for this test,
                    // but let's be rigorous.
                    // putNode(Node(followerId)) 
                    
                    // Create Edge: u$i -> FOLLOWS -> celeb_1
                    putEdge(Edge(followerId, CELEBRITY, "FOLLOWS"))
                }
            }
        }
        println("Created Supernode with $FOLLOWERS_COUNT edges in ${time}ms")

        // 2. Query Inbound (Who follows celeb_1?)
        val followers = graph.getInboundSourceIds(CELEBRITY, "FOLLOWS")
        assertEquals(FOLLOWERS_COUNT, followers.size)
        
        // 3. Verify specific follower
        assertTrue(followers.contains("u1"))
        assertTrue(followers.contains("u$FOLLOWERS_COUNT"))
    }
}
