package com.pankaj.koredb.engine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Validates system stability when Compaction occurs simultaneous to Reading.
 *
 * Compaction deletes old SSTables. We must ensure that active readers
 * don't crash if the underlying file is unlinked.
 */
class CompactionRaceTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/test_race_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Reading while Compacting`() = runBlocking {
        val entryCount = 5000
        
        // 1. Seed data (Split into multiple segments to allow compaction)
        for (i in 0 until entryCount) {
            db.putRaw("k:$i".toByteArray(), "val:$i".toByteArray())
            if (i % 1000 == 0) db.flushMemTableInternal()
        }

        // 2. Launch a long-running read operation
        val reader = async(Dispatchers.IO) {
            for (i in 0 until entryCount) {
                // Read every key. This iterates over sstReaders.
                val valBytes = db.getRaw("k:$i".toByteArray())
                assertNotNull(valBytes)
                assertEquals("val:$i", String(valBytes!!))
                
                // Slow down slightly to increase overlap chance
                if (i % 100 == 0) delay(1) 
            }
            true
        }

        // 3. Trigger Compaction concurrently
        val compactor = async(Dispatchers.IO) {
            delay(50) // Wait for reader to start
            db.performCompaction()
            true
        }

        // 4. Wait for both
        val readResult = reader.await()
        val compactResult = compactor.await()

        assertTrue(readResult)
        assertTrue(compactResult)
        
        // 5. Verify final state
        val sstFiles = testDir.listFiles { _, name -> name.endsWith(".sst") }
        assertEquals("Should compact to 1 file", 1, sstFiles?.size)
        
        // 6. Verify data is still readable after file swap
        assertEquals("val:0", String(db.getRaw("k:0".toByteArray())!!))
    }
}
