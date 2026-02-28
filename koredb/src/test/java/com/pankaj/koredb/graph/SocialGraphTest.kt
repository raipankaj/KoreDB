package com.pankaj.koredb.graph

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.query.query
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Validates Graph Capabilities for a Social Media App Scenario (e.g., Instagram).
 *
 * Concepts:
 * - Users, Posts, Hashtags
 * - Relationships: FOLLOWS, LIKED, TAGGED_WITH, BLOCKED
 */
class SocialGraphTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_social_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Follow Unfollow Lifecycle`() = runBlocking {
        val graph = db.graph()
        
        graph.putNode(Node("u1", properties = mapOf("username" to "alice")))
        graph.putNode(Node("u2", properties = mapOf("username" to "bob")))

        // 1. Follow
        graph.putEdge(Edge("u1", "u2", "FOLLOWS"))
        assertTrue("Alice should follow Bob", graph.getOutboundTargetIds("u1", "FOLLOWS").contains("u2"))
        assertTrue("Bob should have Alice as follower", graph.getInboundSourceIds("u2", "FOLLOWS").contains("u1"))

        // 2. Unfollow
        graph.removeEdge("u1", "FOLLOWS", "u2")
        assertFalse("Alice should NOT follow Bob", graph.getOutboundTargetIds("u1", "FOLLOWS").contains("u2"))
    }

    @Test
    fun `test Feed Generation (1-hop)`() = runBlocking {
        val graph = db.graph()
        
        // Alice follows Bob and Charlie. Bob and Charlie have posts.
        graph.transaction {
            putNode(Node("alice")); putNode(Node("bob")); putNode(Node("charlie"))
            putNode(Node("post_b1")); putNode(Node("post_c1"))
            
            putEdge(Edge("alice", "bob", "FOLLOWS"))
            putEdge(Edge("alice", "charlie", "FOLLOWS"))
            
            putEdge(Edge("bob", "post_b1", "CREATED"))
            putEdge(Edge("charlie", "post_c1", "CREATED"))
        }

        // Query: Get all posts created by people Alice follows
        val feedPostIds = graph.query {
            startingWith("alice")
            outbound("FOLLOWS") // -> [bob, charlie]
            outbound("CREATED") // -> [post_b1, post_c1]
        }.toIdList()

        assertEquals(2, feedPostIds.size)
        assertTrue(feedPostIds.contains("post_b1"))
        assertTrue(feedPostIds.contains("post_c1"))
    }

    @Test
    fun `test Friend Recommendations (2-hop)`() = runBlocking {
        val graph = db.graph()
        
        // Alice -> Bob -> Dave
        // Alice -> Charlie -> Dave
        // Alice -> Bob -> Eve
        graph.transaction {
            putNode(Node("alice")); putNode(Node("bob")); putNode(Node("charlie"))
            putNode(Node("dave")); putNode(Node("eve"))
            
            putEdge(Edge("alice", "bob", "FOLLOWS"))
            putEdge(Edge("alice", "charlie", "FOLLOWS"))
            
            putEdge(Edge("bob", "dave", "FOLLOWS"))
            putEdge(Edge("charlie", "dave", "FOLLOWS"))
            putEdge(Edge("bob", "eve", "FOLLOWS"))
        }

        // Recommend people followed by my friends
        val recommendations = graph.query {
            startingWith("alice")
            outbound("FOLLOWS") // -> [bob, charlie]
            outbound("FOLLOWS") // -> [dave, eve]
        }.toIdList()

        assertEquals(2, recommendations.size) // Dave and Eve (Dave appears twice in path but Set deduplicates)
        assertTrue(recommendations.contains("dave"))
        assertTrue(recommendations.contains("eve"))
    }

    @Test
    fun `test Influence Check (Follower Count)`() = runBlocking {
        val graph = db.graph()
        val influencer = "celeb"
        graph.putNode(Node(influencer))

        // 100 people follow celeb
        for (i in 1..100) {
            graph.putEdge(Edge("fan_$i", influencer, "FOLLOWS"))
        }

        // Count inbound edges
        val followerCount = graph.getInboundSourceIds(influencer, "FOLLOWS").size
        assertEquals(100, followerCount)
    }

    @Test
    fun `test Viral Post (Shared Interests)`() = runBlocking {
        val graph = db.graph()
        
        // Alice likes Post1.
        // Bob likes Post1.
        // Who else likes Post1? (Potential friends for Alice)
        graph.transaction {
            putNode(Node("alice")); putNode(Node("bob")); putNode(Node("post1"))
            putEdge(Edge("alice", "post1", "LIKES"))
            putEdge(Edge("bob", "post1", "LIKES"))
        }

        val sharedInterestUsers = graph.query {
            startingWith("alice")
            outbound("LIKES") // -> post1
            inbound("LIKES")  // -> [alice, bob]
        }.toIdList().filter { it != "alice" } // Exclude self

        assertEquals(1, sharedInterestUsers.size)
        assertEquals("bob", sharedInterestUsers[0])
    }

    @Test
    fun `test Blocked User Filtering`() = runBlocking {
        val graph = db.graph()
        
        // Alice follows Bob.
        // Bob follows Charlie.
        // Alice BLOCKS Charlie.
        graph.transaction {
            putNode(Node("alice")); putNode(Node("bob")); putNode(Node("charlie"))
            putEdge(Edge("alice", "bob", "FOLLOWS"))
            putEdge(Edge("bob", "charlie", "FOLLOWS"))
            putEdge(Edge("alice", "charlie", "BLOCKED"))
        }

        // Get FOF (Friends of Friends), but exclude blocked users
        val blockedIds = graph.getOutboundTargetIds("alice", "BLOCKED").toSet()

        val fof = graph.query {
            startingWith("alice")
            outbound("FOLLOWS")
            outbound("FOLLOWS")
        }.toIdList().filter { !blockedIds.contains(it) }

        assertEquals(0, fof.size) // Charlie should be filtered out
    }

    @Test
    fun `test Hashtag Search`() = runBlocking {
        val graph = db.graph()
        
        // Post1 -> #Travel
        // Post2 -> #Food
        // Post3 -> #Travel
        graph.transaction {
            putNode(Node("post1")); putNode(Node("post2")); putNode(Node("post3"))
            putNode(Node("tag_travel", labels = setOf("Hashtag")))
            
            putEdge(Edge("post1", "tag_travel", "TAGGED"))
            putEdge(Edge("post3", "tag_travel", "TAGGED"))
        }

        val travelPosts = graph.getInboundSourceIds("tag_travel", "TAGGED")
        assertEquals(2, travelPosts.size)
        assertTrue(travelPosts.contains("post1"))
        assertTrue(travelPosts.contains("post3"))
    }

    @Test
    fun `test User Search by Property`() = runBlocking {
        val graph = db.graph()
        
        graph.putNode(Node("u1", setOf("Person"), mapOf("username" to "john_doe")))
        graph.putNode(Node("u2", setOf("Person"), mapOf("username" to "johnny_bravo")))
        
        val exact = graph.getNodesByProperty("Person", "username", "john_doe")
        assertEquals(1, exact.size)
        assertEquals("u1", exact[0].id)
    }
    
    @Test
    fun `test User Search with Labels`() = runBlocking {
        val graph = db.graph()
        
        graph.putNode(Node("u1", setOf("User"), mapOf("username" to "john_doe")))
        graph.putNode(Node("u2", setOf("User"), mapOf("username" to "johnny_bravo")))

        val result = graph.getNodesByProperty("User", "username", "johnny_bravo")
        assertEquals(1, result.size)
        assertEquals("u2", result[0].id)
    }

    @Test
    fun `test Cascading Deletion Simulation`() = runBlocking {
        val graph = db.graph()
        
        // User -> Post -> Comment
        graph.transaction {
            putNode(Node("user"))
            putNode(Node("post"))
            putNode(Node("comment"))
            
            putEdge(Edge("user", "post", "CREATED"))
            putEdge(Edge("post", "comment", "HAS_COMMENT"))
        }

        // Delete Post (User deletes their post)
        // We must manually delete edges because KoreDB is NoSQL (no foreign keys).
        // 1. Find all comments
        val comments = graph.getOutboundTargetIds("post", "HAS_COMMENT")
        // 2. Delete edges to comments
        comments.forEach { graph.removeEdge("post", "HAS_COMMENT", it) }
        // 3. Delete edge from user
        graph.removeEdge("user", "CREATED", "post")
        // 4. Delete post node (via DB deleteRaw since graph.removeNode doesn't exist yet officially in public API, but we use db.deleteRaw)
        db.deleteRaw("g:v:post".toByteArray())

        // Verify
        assertEquals(0, graph.getOutboundTargetIds("user", "CREATED").size)
        assertNull(graph.getNode("post"))
    }

    @Test
    fun `test Path Existence (Does A follow B)`() = runBlocking {
        val graph = db.graph()
        // A -> B -> C
        graph.transaction {
            putNode(Node("A")); putNode(Node("B")); putNode(Node("C"))
            putEdge(Edge("A", "B", "FOLLOWS"))
            putEdge(Edge("B", "C", "FOLLOWS"))
        }

        // Direct check
        val followsB = graph.getOutboundTargetIds("A", "FOLLOWS").contains("B")
        assertTrue(followsB)

        // Indirect check (Transitive)
        val followsIndirectly = graph.query {
            startingWith("A")
            outbound("FOLLOWS", hops = 2)
        }.toIdList().contains("C")
        
        assertTrue(followsIndirectly)
    }
}
