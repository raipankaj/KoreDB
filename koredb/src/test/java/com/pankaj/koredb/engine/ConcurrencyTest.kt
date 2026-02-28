package com.pankaj.koredb.engine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Enterprise-Grade Concurrency Test Suite.
 *
 * Validates:
 * - Thread Safety of MemTable and WAL.
 * - Prevention of Race Conditions.
 * - Deadlock Freedom under load.
 */
class ConcurrencyTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/test_conc_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Massive Parallel Writes.
     * Launches 100 coroutines, each writing 100 items.
     * Total 10,000 items written concurrently.
     */
    @Test
    fun `test Parallel Writers`() = runBlocking {
        val threads = 100
        val itemsPerThread = 100
        
        coroutineScope {
            val jobs = (0 until threads).map { threadId ->
                async(Dispatchers.IO) {
                    for (i in 0 until itemsPerThread) {
                        val key = "t:$threadId:i:$i".toByteArray()
                        val value = "val-$threadId-$i".toByteArray()
                        db.putRaw(key, value)
                    }
                }
            }
            jobs.awaitAll()
        }

        // Verification
        for (t in 0 until threads) {
            for (i in 0 until itemsPerThread) {
                val key = "t:$t:i:$i".toByteArray()
                val result = db.getRaw(key)
                assertNotNull("Missing key $key from thread $t", result)
                assertEquals("val-$t-$i", String(result!!))
            }
        }
    }

    /**
     * Concurrent Read/Write.
     * While one set of coroutines writes, another set reads.
     * Ensures consistent reads (no dirty reads or crashes).
     */
    @Test
    fun `test Concurrent Readers and Writers`() = runBlocking {
        val writeCount = 5000
        val readCount = 5000
        
        // Pre-populate some data for readers
        for (i in 0 until 1000) {
            db.putRaw("static:$i".toByteArray(), "static-val".toByteArray())
        }

        coroutineScope {
            val writer = async(Dispatchers.IO) {
                for (i in 0 until writeCount) {
                    db.putRaw("dyn:$i".toByteArray(), "dyn-val".toByteArray())
                }
            }

            val reader = async(Dispatchers.IO) {
                for (i in 0 until readCount) {
                    // Randomly read static or dynamic keys
                    if (i % 2 == 0) {
                        val res = db.getRaw("static:${i % 1000}".toByteArray())
                        assertNotNull(res)
                    } else {
                        // Dynamic key might not exist yet, that's fine, just ensure no crash
                        db.getRaw("dyn:${i % writeCount}".toByteArray())
                    }
                }
            }
            
            writer.await()
            reader.await()
        }
    }

    /**
     * Atomic Counter Simulation.
     * Simulates a "Read-Modify-Write" pattern (though KoreDB is key-value, 
     * this tests if high contention on specific keys causes issues).
     * Since KoreDB put is Last-Write-Wins, we can't do atomic increments easily without locking in app code.
     * But we can verify that heavy contention on ONE KEY doesn't deadlock.
     */
    @Test
    fun `test Hot Key Contention`() = runBlocking {
        val hotKey = "hot_counter".toByteArray()
        val iterations = 2000
        
        coroutineScope {
            val jobs = (1..10).map { id ->
                async(Dispatchers.IO) {
                    for (i in 0 until iterations) {
                        db.putRaw(hotKey, "val-$id-$i".toByteArray())
                    }
                }
            }
            jobs.awaitAll()
        }
        
        // Ensure the key is readable and has SOME valid value (LWW)
        val finalVal = db.getRaw(hotKey)
        assertNotNull(finalVal)
        val str = String(finalVal!!)
        assertTrue(str.startsWith("val-"))
    }
}
