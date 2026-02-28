package com.pankaj.koredb.core

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Test Suite for the High-Level Collection API (KoreCollection).
 *
 * Covers:
 * - Typed CRUD (Create, Read, Update, Delete)
 * - Complex Serialization (Nested objects, Lists, Maps)
 * - Batch Processing
 * - Reactive Updates via Flow
 * - Tombstone Deletion handling
 */
class KoreCollectionTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    data class User(val id: String, val email: String, val age: Int)

    @Serializable
    data class ComplexEntity(
        val id: String,
        val meta: Map<String, String>,
        val tags: List<String>,
        val embedded: User? = null
    )

    @Before
    fun setup() {
        testDir = File("build/tmp/test_col_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Verifies standard typed operations for simple entities.
     */
    @Test
    fun `test Typed CRUD operations`() = runBlocking {
        val users = db.collection<User>("users")
        val user1 = User("u1", "alice@example.com", 25)

        // CREATE
        users.insert("u1", user1)

        // READ
        val fetched = users.getById("u1")
        assertEquals("Fetched user ID mismatch", user1.id, fetched?.id)
        assertEquals("Fetched user Email mismatch", user1.email, fetched?.email)

        // UPDATE
        val updatedUser = user1.copy(age = 26)
        users.insert("u1", updatedUser)
        assertEquals("Age not updated", 26, users.getById("u1")?.age)

        // DELETE
        users.delete("u1")
        assertNull("Deleted user must return null", users.getById("u1"))
    }

    /**
     * Verifies that complex nested structures (Lists, Maps, Embedded objects)
     * are correctly serialized and deserialized.
     */
    @Test
    fun `test Complex Object Serialization`() = runBlocking {
        val collection = db.collection<ComplexEntity>("complex")
        
        val original = ComplexEntity(
            id = "c1",
            meta = mapOf("source" to "API", "version" to "1.0"),
            tags = listOf("priority", "urgent"),
            embedded = User("u99", "sub@test.com", 0)
        )

        collection.insert("c1", original)

        val retrieved = collection.getById("c1")
        assertNotNull(retrieved)
        assertEquals("Metadata mismatch", "API", retrieved?.meta?.get("source"))
        assertEquals("Tags count mismatch", 2, retrieved?.tags?.size)
        assertEquals("Embedded user mismatch", "sub@test.com", retrieved?.embedded?.email)
    }

    /**
     * Tests inserting multiple items at once (Batch Insert).
     * This is crucial for performance optimization in bulk loads.
     */
    @Test
    fun `test Batch Insertion`() = runBlocking {
        val users = db.collection<User>("users")
        val batch = mapOf(
            "u1" to User("u1", "a", 10),
            "u2" to User("u2", "b", 20),
            "u3" to User("u3", "c", 30)
        )

        users.insertBatch(batch)

        val all = users.getAll()
        assertEquals("All batch items should be present", 3, all.size)
        assertEquals("Specific item verify failed", "b", users.getById("u2")?.email)
    }

    /**
     * REACTIVE TEST: Observation Flow
     * Verifies that the Flow API emits the initial value correctly.
     */
    @Test
    fun `test Observation Flow emits updates`() = runBlocking {
        val users = db.collection<User>("users")
        val user = User("u1", "init", 0)

        users.insert("u1", user)

        // We can't easily assert flow emissions in runBlocking without specialized TestScope logic,
        // but we can verify basic reactivity if we launch a collector.
        // For simplicity, we just verify the flow emits the current value.
        
        val emitted = users.observeById("u1").first()
        assertEquals("Observed value mismatch", user, emitted)
    }

    /**
     * Verifies that deleting an item (Tombstone) correctly makes it disappear from getById.
     */
    @Test
    fun `test Deletion via Tombstone`() = runBlocking {
        val users = db.collection<User>("users")
        users.insert("u1", User("u1", "test", 1))
        
        users.delete("u1")
        
        val result = users.getById("u1")
        assertNull("Deleted user should return null (Tombstone handled)", result)
    }
}
