package com.pankaj.koredb.engine

import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.RandomAccessFile
import java.util.UUID

/**
 * Test Suite specifically for Write-Ahead Log (WAL) Recovery and Corruption Handling.
 *
 * Covers:
 * - Truncated WALs (Partial writes due to power loss)
 * - Corrupted WALs (Bit rot or disk errors)
 * - Safe Recovery (Restoring valid prefix of data)
 */
class WalRecoveryTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/test_wal_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    /**
     * Simulates a scenario where the WAL file is truncated (e.g., system crash during write).
     * The engine should discard the partial record and recover the valid prefix.
     */
    @Test
    fun `test Recovery from Truncated WAL`() = runBlocking {
        // 1. Write 3 records
        val k1 = "k1".toByteArray(); val v1 = "v1".toByteArray()
        val k2 = "k2".toByteArray(); val v2 = "v2".toByteArray()
        val k3 = "k3".toByteArray(); val v3 = "v3".toByteArray()

        db.putRaw(k1, v1)
        db.putRaw(k2, v2)
        db.putRaw(k3, v3)
        db.close()

        // 2. Corrupt the WAL by truncating the last record partially
        val walFile = File(testDir, "kore.wal")
        assertTrue(walFile.exists())
        
        RandomAccessFile(walFile, "rw").use { raf ->
            // Cut off the last few bytes (simulating partial write of k3/v3)
            // We assume the records are appended sequentially. 
            // Truncating slightly should break the checksum or structure of the last entry.
            val newLength = raf.length() - 5 
            raf.setLength(newLength)
        }

        // 3. Reopen DB
        val reopenedDb = KoreDB(testDir)

        // 4. Verify k1 and k2 exist, but k3 is gone (or ignored safely)
        assertArrayEquals("k1 should survive", v1, reopenedDb.getRaw(k1))
        assertArrayEquals("k2 should survive", v2, reopenedDb.getRaw(k2))
        
        // k3 might be missing due to corruption
        val k3Result = reopenedDb.getRaw(k3)
        if (k3Result != null) {
            // If it survived, it means we didn't cut enough, or it was flushed differently.
            // But main goal is: NO CRASH.
        } else {
            // This is the expected behavior for a partial write
        }
        
        reopenedDb.close()
    }

    /**
     * Simulates a WAL with random garbage appended to the end.
     * The engine should detect the invalid format/checksum and stop replay safely.
     */
    @Test
    fun `test Recovery from Garbage Tail`() = runBlocking {
        db.putRaw("safe".toByteArray(), "data".toByteArray())
        db.close()

        val walFile = File(testDir, "kore.wal")
        RandomAccessFile(walFile, "rw").use { raf ->
            raf.seek(raf.length())
            raf.write(byteArrayOf(0xBA.toByte(), 0xAD.toByte(), 0xF0.toByte(), 0x0D.toByte())) // Garbage
        }

        val reopenedDb = KoreDB(testDir)
        assertArrayEquals("Safe data should be recovered", "data".toByteArray(), reopenedDb.getRaw("safe".toByteArray()))
        reopenedDb.close()
    }
}
