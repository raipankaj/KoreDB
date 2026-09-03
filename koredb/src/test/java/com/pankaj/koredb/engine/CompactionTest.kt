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

        // Segment 3
        db.putRaw("C".toByteArray(), "Val3".toByteArray())
        db.flushMemTableInternal()

        // Segment 4 -> This triggers Auto-Compaction under Leveled Compaction (L0 threshold is 4)!
        db.putRaw("D".toByteArray(), "Val4".toByteArray())
        db.flushMemTableInternal()

        // Wait for background compaction to complete
        kotlinx.coroutines.delay(100)
        while (db.isCompacting) {
            kotlinx.coroutines.delay(10)
        }

        // Verify compaction happened. We should have 1 segment now.
        val sstFiles = testDir.listFiles { _, name -> name.endsWith(".sst") } ?: emptyArray()
        
        // Assert we have fewer than 3 files (likely 1)
        assertTrue("Compaction should have reduced file count. Found: ${sstFiles.size}", sstFiles.size < 3)

        // Verify data is still intact
        assertEquals("Val1", String(db.getRaw("A".toByteArray())!!))
        assertEquals("Val2", String(db.getRaw("B".toByteArray())!!))
        assertEquals("Val3", String(db.getRaw("C".toByteArray())!!))
        assertEquals("Val4", String(db.getRaw("D".toByteArray())!!))
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

    @Test
    fun `test Compaction Level 0 overrides Level 1`() = runBlocking {
        val key = "l0_vs_l1_key".toByteArray()

        // 1. Write V1 and trigger compaction to push it to Level 1
        db.putRaw(key, "old_l1_value".toByteArray())
        db.flushMemTableInternal()

        // Write 3 more files to force compaction into L1
        for (i in 1..3) {
            db.putRaw("dummy_$i".toByteArray(), "val_$i".toByteArray())
            db.flushMemTableInternal()
        }

        db.performLeveledCompaction()

        // Verify key is in L1 with old value
        assertEquals("old_l1_value", String(db.getRaw(key)!!))

        // 2. Now write V2 into Level 0 (fresh memtable flush)
        db.putRaw(key, "new_l0_value".toByteArray())
        db.flushMemTableInternal()

        // Verify that before L0->L1 compaction, read path returns new L0 value
        assertEquals("new_l0_value", String(db.getRaw(key)!!))

        // 3. Compact L0 + L1 together
        db.performLeveledCompaction()

        // The newer Level 0 value MUST survive compaction!
        val afterCompaction = db.getRaw(key)
        assertNotNull("Record lost during L0->L1 compaction", afterCompaction)
        assertEquals("Level 0 value must override Level 1 value", "new_l0_value", String(afterCompaction!!))
    }

    @Test
    fun `test Compaction Level 0 Tombstone overrides Level 1 value`() = runBlocking {
        val key = "l0_tombstone_key".toByteArray()

        // 1. Write V1 and push to Level 1
        db.putRaw(key, "persisted_value".toByteArray())
        db.flushMemTableInternal()

        for (i in 1..3) {
            db.putRaw("dummy_tomb_$i".toByteArray(), "val_$i".toByteArray())
            db.flushMemTableInternal()
        }
        db.performLeveledCompaction()

        assertEquals("persisted_value", String(db.getRaw(key)!!))

        // 2. Delete key in Level 0
        db.deleteRaw(key)
        db.flushMemTableInternal()

        // 3. Compact L0 + L1 together
        db.performLeveledCompaction()

        // The tombstone in Level 0 MUST delete the Level 1 value!
        val afterCompaction = db.getRaw(key)
        assertNull("Level 0 tombstone must delete Level 1 entry during compaction", afterCompaction)
    }
}
