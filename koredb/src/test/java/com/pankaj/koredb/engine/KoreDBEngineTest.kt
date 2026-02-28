package com.pankaj.koredb.engine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Test Suite for the Core Storage Engine (KoreDB).
 *
 * Covers:
 * - Basic CRUD (Create, Read, Update, Delete)
 * - Data Persistence (WAL Recovery)
 * - Prefix Scans
 * - Concurrency & Thread Safety
 * - Edge Cases (Empty values, Large Payloads)
 */
class KoreDBEngineTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        // Create a unique temporary directory for each test to ensure isolation
        testDir = File("build/tmp/test_db_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Verifies that simple key-value pairs can be written and retrieved accurately.
     */
    @Test
    fun `test basic Put and Get`() = runBlocking {
        val key = "user:1001"
        val value = """{"name": "Alice", "age": 30}"""

        db.putRaw(key.toByteArray(), value.toByteArray())

        val result = db.getRaw(key.toByteArray())
        assertNotNull("Value should exist for key", result)
        assertEquals("Value content should match", value, String(result!!))
    }

    /**
     * Verifies that deleting a key actually removes it and subsequent lookups return null.
     */
    @Test
    fun `test Delete operation`() = runBlocking {
        val key = "user:to_delete".toByteArray()
        val value = "some_data".toByteArray()

        db.putRaw(key, value)
        assertNotNull(db.getRaw(key))

        db.deleteRaw(key)
        assertNull("Deleted key should return null", db.getRaw(key))
    }

    /**
     * Simulates an application restart to verify that data is durable.
     * It writes data, closes the DB, reopens it, and checks if data is recovered from the WAL.
     */
    @Test
    fun `test Data Persistence across restarts (WAL Recovery)`() = runBlocking {
        val key = "persistent_key".toByteArray()
        val value = "persistent_value".toByteArray()

        // 1. Write data
        db.putRaw(key, value)

        // 2. Close the DB to simulate app shutdown
        db.close()

        // 3. Reopen the DB from the same directory
        val reopenedDb = KoreDB(testDir)

        // 4. Verify data exists (recovered from WAL)
        val result = reopenedDb.getRaw(key)
        assertNotNull("Data should be recovered from WAL", result)
        assertArrayEquals("Recovered value should match original", value, result)
        
        reopenedDb.close()
    }

    /**
     * Verifies that writing to an existing key updates its value (Last-Write-Wins).
     */
    @Test
    fun `test Overwriting keys updates value`() = runBlocking {
        val key = "config:theme".toByteArray()
        
        db.putRaw(key, "light".toByteArray())
        assertEquals("light", String(db.getRaw(key)!!))

        db.putRaw(key, "dark".toByteArray())
        assertEquals("dark", String(db.getRaw(key)!!))
    }

    /**
     * Tests the Prefix Scan capability, which is crucial for range queries.
     * It ensures unrelated keys are ignored and results are correct.
     */
    @Test
    fun `test Prefix Scan`() = runBlocking {
        val prefix = "log:2024:".toByteArray()
        
        val entries = mapOf(
            "log:2024:01" to "January Data",
            "log:2024:02" to "February Data",
            "log:2023:12" to "Old Data",
            "user:alice" to "Irrelevant"
        )

        entries.forEach { (k, v) -> 
            db.putRaw(k.toByteArray(), v.toByteArray()) 
        }

        val results = db.getByPrefixRaw(prefix)
            .map { String(it) }
            .sorted()

        assertEquals(2, results.size)
        assertTrue(results.contains("January Data"))
        assertTrue(results.contains("February Data"))
        assertFalse(results.contains("Old Data"))
    }

    /**
     * STRESS TEST: Concurrency
     * Launches multiple coroutines to write to the DB simultaneously.
     * Verifies that the engine handles concurrent writes without corruption or locking issues.
     */
    @Test
    fun `test Concurrent Writes`() = runBlocking {
        val concurrency = 50
        val deferreds = (1..concurrency).map { i ->
            async(Dispatchers.IO) {
                val key = "concurrent_key_$i".toByteArray()
                val value = "value_$i".toByteArray()
                db.putRaw(key, value)
            }
        }
        
        deferreds.awaitAll()

        // Verify all keys were written successfully
        for (i in 1..concurrency) {
            val key = "concurrent_key_$i".toByteArray()
            val result = db.getRaw(key)
            assertNotNull("Concurrent write for key $i failed", result)
            assertEquals("value_$i", String(result!!))
        }
    }

    /**
     * EDGE CASE: Large Values
     * Tests writing a value larger than typical small objects (e.g., 1MB blob).
     */
    @Test
    fun `test Large Payload Handling`() = runBlocking {
        val key = "image:blob".toByteArray()
        val largeSize = 1024 * 1024 // 1MB
        val largeValue = ByteArray(largeSize) { (it % 255).toByte() }

        db.putRaw(key, largeValue)

        val result = db.getRaw(key)
        assertNotNull(result)
        assertEquals("Payload size mismatch", largeSize, result!!.size)
        assertArrayEquals("Payload content mismatch", largeValue, result)
    }

    /**
     * EDGE CASE: Empty Values
     * In KoreDB, an empty byte array IS the tombstone marker.
     * Therefore, writing an empty value effectively deletes the key.
     * This test verifies that behavior.
     */
    @Test
    fun `test Empty Value Treated as Tombstone`() = runBlocking {
        val key = "flag:empty".toByteArray()
        val emptyValue = ByteArray(0)

        // 1. Write non-empty first
        db.putRaw(key, "data".toByteArray())
        assertNotNull(db.getRaw(key))

        // 2. Overwrite with empty array (Tombstone)
        db.putRaw(key, emptyValue)

        // 3. Verify it is now null (deleted)
        val result = db.getRaw(key)
        assertNull("Writing empty array should be treated as tombstone/delete", result)
    }
}
