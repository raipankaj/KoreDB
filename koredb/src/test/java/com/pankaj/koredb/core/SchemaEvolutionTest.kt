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
 * Validates backward and forward compatibility of data models.
 * Ensures that modifying Data Classes doesn't break existing data access.
 */
class SchemaEvolutionTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_schema_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        if (this::db.isInitialized) {
            // Check if db is closed? KoreDatabase doesn't have isClosed.
            // Just try to close, ignore errors.
            try { db.close() } catch (e: Exception) {}
        }
        testDir.deleteRecursively()
    }

    // Version 1 of the User class
    @Serializable
    data class UserV1(val id: String, val name: String)

    // Version 2: Added a new field 'email' with default value (Backward Compatible)
    @Serializable
    data class UserV2(val id: String, val name: String, val email: String = "unknown")

    // Version 3: Removed 'name' field (Forward Compatible - ignores unknown keys)
    @Serializable
    data class UserV3(val id: String, val email: String = "unknown")

    @Test
    fun `test Adding Fields (V1 to V2)`() = runBlocking {
        val colV1 = db.collection<UserV1>("users")
        colV1.insert("u1", UserV1("u1", "Alice"))

        // Simulate App Restart (Clear internal cache of KoreDatabase)
        db.close()
        val db2 = KoreDatabase(testDir)

        // Re-open as V2
        val colV2 = db2.collection<UserV2>("users")
        val userV2 = colV2.getById("u1")

        assertNotNull(userV2)
        assertEquals("Alice", userV2?.name)
        assertEquals("unknown", userV2?.email) // Default value used
        
        db2.close()
    }

    @Test
    fun `test Removing Fields (V2 to V3)`() = runBlocking {
        // Insert with V2 (has name and email)
        val colV2 = db.collection<UserV2>("users")
        colV2.insert("u2", UserV2("u2", "Bob", "bob@test.com"))

        // Simulate App Restart
        db.close()
        val db2 = KoreDatabase(testDir)

        // Read with V3 (no name)
        // Note: KoreDB's Json config must have `ignoreUnknownKeys = true` for this to work.
        val colV3 = db2.collection<UserV3>("users")
        val userV3 = colV3.getById("u2")

        assertNotNull(userV3)
        assertEquals("u2", userV3?.id)
        assertEquals("bob@test.com", userV3?.email)
        // 'name' is silently ignored
        
        db2.close()
    }
}
