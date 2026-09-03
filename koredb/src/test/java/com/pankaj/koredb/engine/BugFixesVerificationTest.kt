package com.pankaj.koredb.engine

import com.pankaj.koredb.bridge.HybridQueryPlanner
import com.pankaj.koredb.bridge.QueryStrategy
import com.pankaj.koredb.core.KoreCollection
import com.pankaj.koredb.core.KoreVectorCollection
import com.pankaj.koredb.core.VectorCollectionConfig
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.exporter.exportToJson
import com.pankaj.koredb.exporter.importFromJson
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.ScalarQuantizer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Comprehensive verification test suite for all reported critical bugs and performance enhancements.
 */
class BugFixesVerificationTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    data class Product(
        val name: String,
        val category: String,
        val price: Double
    )

    @Before
    fun setup() {
        testDir = File("build/tmp/test_bugs_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test ScalarQuantizer Euclidean and Manhattan distance signs`() {
        val quantizer = ScalarQuantizer(dimensions = 2)
        val vectors = listOf(
            floatArrayOf(0f, 0f),
            floatArrayOf(10f, 10f)
        )
        quantizer.train(vectors)

        val query = floatArrayOf(0f, 0f)
        val closeVec = quantizer.quantize(floatArrayOf(0.1f, 0.1f))
        val farVec = quantizer.quantize(floatArrayOf(9.0f, 9.0f))

        // Euclidean: closer vector must have HIGHER score (e.g. -0.14 > -12.7)
        val euclideanClose = quantizer.computeDistance(query, closeVec, DistanceMetric.EUCLIDEAN)
        val euclideanFar = quantizer.computeDistance(query, farVec, DistanceMetric.EUCLIDEAN)
        assertTrue("Euclidean: closer score ($euclideanClose) must be > far score ($euclideanFar)", euclideanClose > euclideanFar)

        // Manhattan: closer vector must have HIGHER score
        val manhattanClose = quantizer.computeDistance(query, closeVec, DistanceMetric.MANHATTAN)
        val manhattanFar = quantizer.computeDistance(query, farVec, DistanceMetric.MANHATTAN)
        assertTrue("Manhattan: closer score ($manhattanClose) must be > far score ($manhattanFar)", manhattanClose > manhattanFar)

        // Cosine & Inner Product
        val cosineClose = quantizer.computeDistance(query, closeVec, DistanceMetric.COSINE)
        val cosineFar = quantizer.computeDistance(query, farVec, DistanceMetric.COSINE)
        assertNotNull(cosineClose)
        assertNotNull(cosineFar)
    }

    @Test
    fun `test GraphTransaction writes reverse pointers and escapes colons`() = runBlocking {
        val graph = db.graph()

        // Write a node inside a transaction with colons in properties and label
        graph.transaction {
            putNode(
                Node(
                    id = "user:42",
                    labels = setOf("Developer:Kotlin"),
                    properties = mapOf(
                        "city" to "San:Francisco",
                        "role" to "Architect"
                    )
                )
            )
            putEdge(
                Edge(
                    sourceId = "user:42",
                    targetId = "user:99",
                    type = "WORKS:WITH",
                    properties = mapOf("weight" to "1.0")
                )
            )
        }

        // 1. Point lookup
        val node = graph.getNode("user:42")
        assertNotNull("Node written in transaction must exist", node)
        assertEquals("San:Francisco", node!!.properties["city"])

        // 2. Property lookup using reverse pointer oracle
        val foundNodes = graph.getNodesByProperty("Developer:Kotlin", "city", "San:Francisco")
        assertEquals("Property lookup must find node created in transaction via reverse pointer", 1, foundNodes.size)
        assertEquals("user:42", foundNodes[0].id)

        // 3. Edge lookup with colons
        val edges = graph.getOutboundEdges("user:42", "WORKS:WITH")
        assertEquals(1, edges.size)
        assertEquals("user:99", edges[0].targetId)
    }

    @Test
    fun `test KoreCollection insertBatch maintains reverse pointers across updates and deletes`() = runBlocking {
        val coll = db.collection<Product>("products")
        coll.createIndex("category") { it.category }

        // Batch insert
        val batch1 = mapOf(
            "p1" to Product("Sneakers", "Shoes", 99.0),
            "p2" to Product("Loafers", "Shoes", 120.0),
            "p3" to Product("T-Shirt", "Clothing", 25.0)
        )
        coll.insertBatch(batch1)

        // Verify initial index state
        val shoesBefore = coll.getByIndex("category", "Shoes")
        assertEquals(2, shoesBefore.size)

        // Update p2 to "Clothing" in a second batch
        val batch2 = mapOf(
            "p2" to Product("Loafers", "Clothing", 110.0)
        )
        coll.insertBatch(batch2)

        // Stale index for "Shoes" must NOT return p2!
        val shoesAfter = coll.getByIndex("category", "Shoes")
        assertEquals("p2 must no longer be found under Shoes", 1, shoesAfter.size)
        assertEquals("Sneakers", shoesAfter[0].name)

        // "Clothing" must now contain p2 and p3
        val clothingAfter = coll.getByIndex("category", "Clothing")
        assertEquals(2, clothingAfter.size)

        // Delete p1
        coll.delete("p1")
        val shoesFinal = coll.getByIndex("category", "Shoes")
        assertEquals("Deleted document must not remain in secondary index", 0, shoesFinal.size)
    }

    @Test
    fun `test KoreQuery whereEq uses secondary index pushdown`() = runBlocking {
        val coll = db.collection<Product>("catalog")
        coll.createIndex("category") { it.category }

        val items = (1..50).associate { i ->
            val cat = if (i % 2 == 0) "Electronics" else "Books"
            "item_$i" to Product("Item $i", cat, i * 10.0)
        }
        coll.insertBatch(items)

        // Query with whereEq (should leverage index pushdown)
        val electronics = coll.query()
            .whereEq("category", "Electronics")
            .execute()

        assertEquals(25, electronics.size)
        assertTrue(electronics.all { it.category == "Electronics" })

        // Query count with whereEq
        val count = coll.query()
            .whereEq("category", "Books")
            .count()

        assertEquals(25, count)
    }

    @Test
    fun `test KoreDatabase vectorCollection caching`() {
        val v1 = db.vectorCollection("embeddings")
        val v2 = db.vectorCollection("embeddings")
        assertSame("vectorCollection with same name must return cached instance", v1, v2)

        val v3 = db.vectorCollection("embeddings_configured") {
            dimensions = 128
            metric = DistanceMetric.COSINE
        }
        val v4 = db.vectorCollection("embeddings_configured") {
            dimensions = 128
        }
        assertSame("Configured vectorCollection must return cached instance", v3, v4)
    }

    @Test
    fun `test DataExporter streaming export and import`() = runBlocking {
        val coll = db.collection<Product>("export_test")
        val testData = mapOf(
            "doc1" to Product("Laptop", "Electronics", 1200.0),
            "doc2" to Product("Headphones", "Electronics", 150.0),
            "doc3" to Product("Novel", "Books", 15.0)
        )
        coll.insertBatch(testData)

        val jsonFile = File(testDir, "exported_products.json")
        val stats = coll.exportToJson(jsonFile)

        assertEquals(3, stats.totalRecords)
        assertTrue(jsonFile.exists())
        assertTrue(jsonFile.length() > 0)

        // Clear collection and import back
        coll.delete("doc1")
        coll.delete("doc2")
        coll.delete("doc3")
        assertEquals(0, coll.count())

        val importStats = coll.importFromJson(jsonFile)
        assertEquals(3, importStats.totalRecords)
        assertEquals(3, coll.count())
        assertEquals("Laptop", coll.getById("doc1")?.name)
    }
}
