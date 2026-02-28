package com.pankaj.koredb.engine

import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

/**
 * Test Suite for LSM-Tree Compaction Logic.
 */
class CompactionTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/test_compact_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Compaction Merges Segments`() = runBlocking {
        // Segment 1
        db.putRaw("A".toByteArray(), "Val1".toByteArray())
        db.flushMemTableInternal()

        // Segment 2
        db.putRaw("B".toByteArray(), "Val2".toByteArray())
        db.flushMemTableInternal()

        // Segment 3 -> This triggers Auto-Compaction!
        db.putRaw("C".toByteArray(), "Val3".toByteArray())
        db.flushMemTableInternal()

        // Verify compaction happened. We should have 1 segment now.
        val sstFiles = testDir.listFiles { _, name -> name.endsWith(".sst") } ?: emptyArray()
        
        // Assert we have fewer than 3 files (likely 1)
        assertTrue("Compaction should have reduced file count. Found: ${sstFiles.size}", sstFiles.size < 3)

        // Verify data is still intact
        assertEquals("Val1", String(db.getRaw("A".toByteArray())!!))
        assertEquals("Val2", String(db.getRaw("B".toByteArray())!!))
        assertEquals("Val3", String(db.getRaw("C".toByteArray())!!))
    }

    @Test
    fun `test Compaction Respects Tombstones`() = runBlocking {
        val key = "zombie".toByteArray()

        db.putRaw(key, "alive".toByteArray())
        db.flushMemTableInternal()

        db.deleteRaw(key)
        db.flushMemTableInternal() // 2nd file

        // Trigger compaction manually (if not triggered yet)
        db.performCompaction()

        // Verify it is deleted
        assertNull("Key should be deleted", db.getRaw(key))
    }

    @Test
    fun `test Compaction Keeps Newest Version`() = runBlocking {
        val key = "versioned".toByteArray()

        // Write V1 -> Flush (File 0)
        db.putRaw(key, "v1".toByteArray())
        db.flushMemTableInternal()

        // Write V2 -> Flush (File 1)
        db.putRaw(key, "v2".toByteArray())
        db.flushMemTableInternal()

        // Verify V2 is active (before compaction)
        val preCompact = db.getRaw(key)
        assertNotNull("Pre-compact value missing", preCompact)
        assertEquals("v2", String(preCompact!!))

        // Force compaction
        db.performCompaction()

        // Verify V2 is still active (after compaction)
        val postCompact = db.getRaw(key)
        assertNotNull("Post-compact value missing (Compaction lost data!)", postCompact)
        assertEquals("Newest version must survive compaction", "v2", String(postCompact!!))
    }
}
