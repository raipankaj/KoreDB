package com.pankaj.koredb.core

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Exhaustive Test Suite for Document Object Storage.
 *
 * Designed to prove KoreDB as a robust alternative to Room/SQLite for object storage.
 * Covers 20+ scenarios including complex types, constraints, and edge cases.
 */
class ExhaustiveDocumentTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_doc_full_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    // --- Data Models ---

    @Serializable
    data class UserProfile(
        val id: String,
        val name: String,
        val age: Int,
        val isActive: Boolean,
        val salary: Double
    )

    @Serializable
    enum class Status { PENDING, ACTIVE, BANNED }

    @Serializable
    data class Order(
        val id: String,
        val status: Status,
        val items: List<String>,
        val metadata: Map<String, String>
    )

    @Serializable
    data class Config(
        val id: String,
        val theme: String? = null, // Nullable
        val version: Int = 1 // Default value
    )

    // --- 1. Basic CRUD ---

    @Test
    fun `test 01 Insert and Get`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        val user = UserProfile("u1", "Alice", 30, true, 5000.50)
        collection.insert("u1", user)
        
        val retrieved = collection.getById("u1")
        assertEquals(user, retrieved)
    }

    @Test
    fun `test 02 Update Object`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        val user = UserProfile("u1", "Alice", 30, true, 5000.0)
        collection.insert("u1", user)
        
        val updated = user.copy(age = 31, salary = 6000.0)
        collection.insert("u1", updated)
        
        val result = collection.getById("u1")
        assertEquals(31, result?.age)
        assertEquals(6000.0, result?.salary ?: 0.0, 0.01)
    }

    @Test
    fun `test 03 Delete Object`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        collection.insert("u1", UserProfile("u1", "Bob", 20, false, 0.0))
        
        collection.delete("u1")
        assertNull(collection.getById("u1"))
    }

    @Test
    fun `test 04 Read Non-Existent Object`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        assertNull(collection.getById("ghost"))
    }

    // --- 2. Data Types & Complexity ---

    @Test
    fun `test 05 Enums Serialization`() = runBlocking {
        val collection = db.collection<Order>("orders")
        val order = Order("o1", Status.ACTIVE, listOf("A"), emptyMap())
        
        collection.insert("o1", order)
        val res = collection.getById("o1")
        
        assertEquals(Status.ACTIVE, res?.status)
        
        // Use BANNED to avoid unused warning
        val bannedOrder = Order("o2", Status.BANNED, listOf("B"), emptyMap())
        collection.insert("o2", bannedOrder)
        assertEquals(Status.BANNED, collection.getById("o2")?.status)
    }

    @Test
    fun `test 06 Lists and Maps`() = runBlocking {
        val collection = db.collection<Order>("orders")
        val items = listOf("Apple", "Banana", "Cherry")
        val meta = mapOf("source" to "web", "priority" to "high")
        
        val order = Order("o2", Status.PENDING, items, meta)
        collection.insert("o2", order)
        
        val res = collection.getById("o2")!!
        assertEquals(3, res.items.size)
        assertEquals("Banana", res.items[1])
        assertEquals("high", res.metadata["priority"])
    }

    @Test
    fun `test 07 Nullable Fields`() = runBlocking {
        val collection = db.collection<Config>("config")
        val cfg = Config("c1", theme = null) // Explicit null
        
        collection.insert("c1", cfg)
        val res = collection.getById("c1")
        
        assertNull(res?.theme)
    }

    @Test
    fun `test 08 Default Values`() = runBlocking {
        val collection = db.collection<Config>("config")
        // Note: When inserting, we create object. 
        // Default values apply on object creation, not DB read, unless deserializer handles missing JSON fields.
        // Here we test simply storing an object that relies on default.
        val cfg = Config("c2", theme = "dark") // version defaults to 1
        
        collection.insert("c2", cfg)
        val res = collection.getById("c2")
        
        assertEquals(1, res?.version)
    }

    @Test
    fun `test 09 Large Text Content`() = runBlocking {
        // Room limits are typically 2MB cursor window. KoreDB splits segments.
        val collection = db.collection<UserProfile>("users")
        val largeName = "A".repeat(10_000)
        val user = UserProfile("u99", largeName, 20, true, 0.0)
        
        collection.insert("u99", user)
        val res = collection.getById("u99")
        assertEquals(10_000, res?.name?.length)
    }

    @Test
    fun `test 10 Special Characters in Strings`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        val name = "Alice O'Neil \n \t \uD83D\uDE00 (Emoji)"
        val user = UserProfile("u_special", name, 25, true, 0.0)
        
        collection.insert("u_special", user)
        val res = collection.getById("u_special")
        assertEquals(name, res?.name)
    }

    // --- 3. Batch Operations ---

    @Test
    fun `test 11 Batch Insert`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        val batch = mapOf(
            "u1" to UserProfile("u1", "A", 10, true, 100.0),
            "u2" to UserProfile("u2", "B", 20, true, 200.0),
            "u3" to UserProfile("u3", "C", 30, true, 300.0)
        )
        
        collection.insertBatch(batch)
        assertEquals(3, collection.getAll().size)
    }

    @Test
    fun `test 12 Get All`() = runBlocking {
        val collection = db.collection<Config>("config")
        collection.insert("c1", Config("c1"))
        collection.insert("c2", Config("c2"))
        
        val all = collection.getAll().sortedBy { it.id }
        assertEquals(2, all.size)
        assertEquals("c1", all[0].id)
        assertEquals("c2", all[1].id)
    }

    @Test
    fun `test 13 Delete All`() = runBlocking {
        val collection = db.collection<Config>("config")
        collection.insert("c1", Config("c1"))
        collection.deleteAll()
        assertTrue(collection.getAll().isEmpty())
    }

    // --- 4. Indexing & Queries ---

    @Test
    fun `test 14 Secondary Index Lookup`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        collection.createIndex("age") { it.age.toString() }
        
        collection.insert("u1", UserProfile("u1", "A", 25, true, 0.0))
        collection.insert("u2", UserProfile("u2", "B", 25, true, 0.0)) // Same age
        collection.insert("u3", UserProfile("u3", "C", 40, true, 0.0))
        
        val age25 = collection.getByIndex("age", "25")
        assertEquals(2, age25.size)
        assertTrue(age25.any { it.id == "u1" })
        assertTrue(age25.any { it.id == "u2" })
    }

    @Test
    fun `test 15 Index Update`() = runBlocking {
        val collection = db.collection<UserProfile>("users")
        collection.createIndex("name") { it.name }
        
        // Insert as Alice
        collection.insert("u1", UserProfile("u1", "Alice", 20, true, 0.0))
        assertEquals(1, collection.getByIndex("name", "Alice").size)
        
        // Update to Bob
        collection.insert("u1", UserProfile("u1", "Bob", 20, true, 0.0))
        val bobs = collection.getByIndex("name", "Bob")
        assertEquals(1, bobs.size)
        assertEquals("Bob", bobs[0].name)
    }

    @Test
    fun `test 16 Prefix Scan`() = runBlocking {
        val collection = db.collection<Config>("config")
        collection.insert("sys:1", Config("sys:1"))
        collection.insert("sys:2", Config("sys:2"))
        collection.insert("app:1", Config("app:1"))
        
        val sysConfigs = collection.getByIdPrefix("sys")
        assertEquals(2, sysConfigs.size)
        assertTrue(sysConfigs.all { it.id.startsWith("sys") })
    }

    // --- 5. Edge Cases & Errors ---

    @Test
    fun `test 17 Keys with Colons`() = runBlocking {
        val collection = db.collection<Config>("config")
        // Key with multiple colons
        val key = "user:123:settings:v1"
        collection.insert(key, Config(key))
        
        val res = collection.getById(key)
        assertNotNull(res)
        assertEquals(key, res?.id)
        
        // Verify prefix works
        val prefixRes = collection.getIdsByPrefix("user:123")
        assertTrue(prefixRes.contains(key))
    }

    @Test
    fun `test 18 Empty Collection Operations`() = runBlocking {
        val collection = db.collection<Config>("empty")
        assertTrue(collection.getAll().isEmpty())
        assertNull(collection.getById("any"))
        
        // Should not crash
        collection.deleteAll()
        collection.createIndex("idx") { it.id }
        assertTrue(collection.getByIndex("idx", "val").isEmpty())
    }

    // --- 6. Concurrency & Persistence ---

    @Test
    fun `test 19 Concurrent Access`() = runBlocking {
        val collection = db.collection<Config>("concurrent")
        
        coroutineScope {
            val jobs = (1..50).map { i ->
                async(Dispatchers.IO) {
                    collection.insert("c$i", Config("c$i"))
                    collection.getById("c$i")
                }
            }
            val results = jobs.awaitAll()
            assertTrue(results.all { it != null })
        }
    }

    @Test
    fun `test 20 Persistence across instances`() = runBlocking {
        val collection = db.collection<UserProfile>("persist")
        collection.insert("p1", UserProfile("p1", "SaveMe", 99, true, 0.0))
        
        db.close()
        
        val db2 = KoreDatabase(testDir)
        val col2 = db2.collection<UserProfile>("persist")
        val res = col2.getById("p1")
        
        assertNotNull(res)
        assertEquals("SaveMe", res?.name)
        
        db2.close()
    }
}
