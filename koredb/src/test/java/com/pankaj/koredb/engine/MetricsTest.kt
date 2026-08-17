package com.pankaj.koredb.engine

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

class MetricsTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_metrics_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun `test KoreDB Metrics Tracking`() = runBlocking {
        val initialMetrics = db.getMetrics()
        assertEquals(0L, initialMetrics.writeCount)
        assertEquals(0L, initialMetrics.readCount)

        // Perform some writes
        val kv = db.keyValue("settings")
        kv.putString("theme", "dark")
        kv.putString("language", "kotlin")

        val postWriteMetrics = db.getMetrics()
        assertTrue(postWriteMetrics.writeCount >= 2)
        assertTrue(postWriteMetrics.memTableEntries >= 2)

        // Perform reads
        val theme = kv.getString("theme")
        assertEquals("dark", theme)
        val lang = kv.getString("language")
        assertEquals("kotlin", lang)

        val postReadMetrics = db.getMetrics()
        assertTrue(postReadMetrics.readCount >= 2)
        assertTrue(postReadMetrics.totalDiskUsageBytes >= 0)
    }
}
