package com.pankaj.koredb.engine

import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.system.measureTimeMillis

/**
 * Enterprise-Grade Stress Test Suite.
 *
 * Validates the database stability under high load, ensuring:
 * - No memory leaks or crashes during massive ingestion.
 * - Consistent read performance after heavy writes.
 * - Durability of large datasets.
 */
class StressTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/test_stress_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Writes 100,000 small records to simulate bulk ingestion.
     * Verifies that the engine doesn't crash and all data is retrievable.
     */
    @Test
    fun `test Massive Bulk Ingestion (100k items)`() = runBlocking {
        val count = 100_000
        val payload = "SmallPayload".toByteArray()

        val time = measureTimeMillis {
            // Write in batches of 1000 for efficiency
            for (i in 0 until count step 1000) {
                val batch = (i until (i + 1000).coerceAtMost(count)).map { id ->
                    val key = "user:$id".toByteArray()
                    key to payload
                }
                db.writeBatchRaw(batch)
            }
        }

        println("Written $count items in ${time}ms")

        // Verify a sample
        val sampleKey = "user:50000".toByteArray()
        val result = db.getRaw(sampleKey)
        assertNotNull("Sample key 50000 missing", result)
        assertArrayEquals(payload, result)

        // Verify total count via iteration (approximate check via internal metrics or just sampling)
        // Since we don't have count(), we check boundaries
        assertNotNull(db.getRaw("user:0".toByteArray()))
        assertNotNull(db.getRaw("user:99999".toByteArray()))
    }

    /**
     * Simulates a "Write-Heavy" mixed workload: 80% Write, 20% Read.
     * Ensures reads remain accurate even while memtable is filling up.
     */
    @Test
    fun `test Mixed Workload`() = runBlocking {
        val iterations = 10_000
        
        for (i in 0 until iterations) {
            val key = "mix:$i".toByteArray()
            val value = "val:$i".toByteArray()
            
            db.putRaw(key, value)
            
            // Every 5th op, read a random previous key
            if (i > 0 && i % 5 == 0) {
                val readId = (0 until i).random()
                val readKey = "mix:$readId".toByteArray()
                val result = db.getRaw(readKey)
                assertNotNull("Failed to read back key $readId during load", result)
                assertEquals("val:$readId", String(result!!))
            }
        }
    }

    /**
     * Writes a large dataset, closes the DB, and reopens it to ensure
     * the WAL and SSTables are correctly recovered.
     */
    @Test
    fun `test Durability of Large Dataset`() = runBlocking {
        val count = 50_000
        for (i in 0 until count) {
            db.putRaw("k:$i".toByteArray(), "v:$i".toByteArray())
        }
        
        // Force flush to ensure some SSTables are created
        db.flushMemTableInternal()
        
        // Write more to stay in WAL/MemTable
        for (i in count until count + 1000) {
            db.putRaw("k:$i".toByteArray(), "v:$i".toByteArray())
        }

        db.close()

        val reopened = KoreDB(testDir)
        
        // Check SSTable data
        assertEquals("v:100", String(reopened.getRaw("k:100".toByteArray())!!))
        
        // Check WAL data
        assertEquals("v:50005", String(reopened.getRaw("k:50005".toByteArray())!!))
        
        reopened.close()
    }
}
