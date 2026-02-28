package com.pankaj.koredb.engine

import com.pankaj.koredb.foundation.SSTableReader
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.RandomAccessFile
import java.util.UUID

/**
 * Validates integrity checks for SSTables.
 */
class CorruptSSTableTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/test_corrupt_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test Detect Invalid Magic Number`() = runBlocking {
        // 1. Create a valid SSTable
        db.putRaw("key".toByteArray(), "val".toByteArray())
        db.flushMemTableInternal()
        
        val sstFile = testDir.listFiles { _, name -> name.endsWith(".sst") }!!.first()
        db.close() // Release file lock

        // 2. Corrupt Magic Number (Last 4 bytes)
        RandomAccessFile(sstFile, "rw").use { raf ->
            raf.seek(raf.length() - 4)
            raf.writeInt(0xDEADBEEF.toInt()) // Bad Magic
        }

        // 3. Attempt to read
        try {
            SSTableReader(sstFile)
            fail("Should have thrown IllegalStateException for invalid magic number")
        } catch (e: IllegalStateException) {
            assertTrue(e.message!!.contains("Invalid Magic Number"))
        }
    }

    @Test
    fun `test Detect Unsupported Version`() = runBlocking {
        db.putRaw("key".toByteArray(), "val".toByteArray())
        db.flushMemTableInternal()
        val sstFile = testDir.listFiles { _, name -> name.endsWith(".sst") }!!.first()
        db.close()

        // 2. Corrupt Version (Bytes -8 to -4)
        RandomAccessFile(sstFile, "rw").use { raf ->
            raf.seek(raf.length() - 8)
            raf.writeInt(999) // Version 999
        }

        // 3. Attempt to read
        try {
            SSTableReader(sstFile)
            fail("Should have thrown UnsupportedOperationException for future version")
        } catch (e: UnsupportedOperationException) {
            assertTrue(e.message!!.contains("Unsupported SSTable version"))
        }
    }
}
