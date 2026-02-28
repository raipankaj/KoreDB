package com.pankaj.koredb.core

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Validates advanced document collection features.
 * 
 * Covers:
 * - Secondary Indexing (createIndex, getByIndex)
 * - Bulk Deletion (deleteAll)
 * - Prefix Queries
 */
class DocumentAdvancedTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    data class Product(val id: String, val category: String, val price: Double)

    @Before
    fun setup() {
        testDir = File("build/tmp/test_doc_adv_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Secondary Indexing`() = runBlocking {
        val products = db.collection<Product>("products")
        
        // Create an index on 'category' field
        products.createIndex("category_idx") { it.category }

        // Insert items
        products.insert("p1", Product("p1", "Electronics", 100.0))
        products.insert("p2", Product("p2", "Electronics", 200.0))
        products.insert("p3", Product("p3", "Books", 15.0))

        // Query by Index
        val electronics = products.getByIndex("category_idx", "Electronics")
        val books = products.getByIndex("category_idx", "Books")
        val food = products.getByIndex("category_idx", "Food")

        assertEquals("Electronics count mismatch", 2, electronics.size)
        assertTrue(electronics.any { it.id == "p1" })
        assertTrue(electronics.any { it.id == "p2" })

        assertEquals("Books count mismatch", 1, books.size)
        assertEquals("p3", books[0].id)

        assertTrue("Food should be empty", food.isEmpty())
    }

    @Test
    fun `test Delete All`() = runBlocking {
        val products = db.collection<Product>("products")
        
        for (i in 1..10) {
            products.insert("p$i", Product("p$i", "Cat", 1.0))
        }
        
        assertEquals(10, products.getAll().size)

        products.deleteAll()

        assertTrue("Collection should be empty after deleteAll", products.getAll().isEmpty())
        assertNull("Individual item lookup should fail", products.getById("p1"))
    }

    @Test
    fun `test ID Prefix Scan`() = runBlocking {
        val products = db.collection<Product>("products")
        
        products.insert("groupA:1", Product("groupA:1", "A", 10.0))
        products.insert("groupA:2", Product("groupA:2", "A", 10.0))
        products.insert("groupB:1", Product("groupB:1", "B", 20.0))

        // Test getIdsByPrefix
        val aIds = products.getIdsByPrefix("groupA")
        assertEquals(2, aIds.size)
        assertTrue(aIds.contains("groupA:1"))
        assertTrue(aIds.contains("groupA:2"))
        assertFalse(aIds.contains("groupB:1"))

        // Test getByIdPrefix (Full objects)
        val aDocs = products.getByIdPrefix("groupA")
        assertEquals(2, aDocs.size)
        assertEquals("A", aDocs[0].category)
    }
}
