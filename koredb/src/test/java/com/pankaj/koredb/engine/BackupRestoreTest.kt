package com.pankaj.koredb.engine

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

@Serializable
data class BackupTestDoc(val id: String, val title: String, val count: Int)

class BackupRestoreTest {

    private lateinit var dbDir: File
    private lateinit var backupDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        dbDir = File("build/tmp/test_db_${UUID.randomUUID()}")
        backupDir = File("build/tmp/test_backup_${UUID.randomUUID()}")
        dbDir.mkdirs()
        backupDir.mkdirs()
        db = KoreDatabase(dbDir)
    }

    @After
    fun tearDown() {
        db.close()
        dbDir.deleteRecursively()
        backupDir.deleteRecursively()
    }

    @Test
    fun `test Backup and Restore with Documents and Graph`() = runBlocking {
        // 1. Populate data
        val collection = db.collection<BackupTestDoc>("docs")
        collection.insert("doc1", BackupTestDoc("doc1", "Document 1", 42))
        collection.insert("doc2", BackupTestDoc("doc2", "Document 2", 84))

        val graph = db.graph()
        graph.putNode(com.pankaj.koredb.graph.Node("user_1", setOf("User"), mapOf("role" to "admin")))
        graph.putNode(com.pankaj.koredb.graph.Node("user_2", setOf("User"), mapOf("role" to "member")))
        graph.putEdge(com.pankaj.koredb.graph.Edge("user_1", "user_2", "FOLLOWS"))

        // 2. Create Backup
        val metadata = db.createBackup(backupDir)
        assertNotNull(metadata)
        assertTrue(metadata.totalSizeBytes > 0)
        assertTrue(File(backupDir, "BACKUP.json").exists())

        // 3. Mutate original DB or delete data
        collection.delete("doc1")
        collection.insert("doc3", BackupTestDoc("doc3", "Document 3", 100))
        assertNull(collection.getById("doc1"))
        assertNotNull(collection.getById("doc3"))

        // 4. Restore from Backup
        val restored = db.restoreFromBackup(backupDir)
        assertTrue(restored)

        // 5. Verify restored state matches original snapshot
        val restoredCol = db.collection<BackupTestDoc>("docs")
        val doc1 = restoredCol.getById("doc1")
        assertNotNull("doc1 should be restored", doc1)
        assertEquals("Document 1", doc1?.title)

        val doc2 = restoredCol.getById("doc2")
        assertNotNull("doc2 should be restored", doc2)
        assertEquals("Document 2", doc2?.title)

        assertNull("doc3 should not exist in restored snapshot", restoredCol.getById("doc3"))

        val restoredGraph = db.graph()
        val node1 = restoredGraph.getNode("user_1")
        assertNotNull("user_1 should be restored", node1)
        assertEquals("admin", node1?.properties?.get("role"))

        val targets = restoredGraph.getOutboundTargetIds("user_1", "FOLLOWS")
        assertEquals(1, targets.size)
        assertEquals("user_2", targets[0])
    }

    @Test
    fun `test Restore Fails on Tampered Checksum`() = runBlocking {
        val collection = db.collection<BackupTestDoc>("docs")
        collection.insert("doc1", BackupTestDoc("doc1", "Document 1", 42))

        db.createBackup(backupDir)

        // Corrupt an SSTable in the backup
        val sstFiles = backupDir.listFiles { _, name -> name.endsWith(".sst") }
        assertNotNull(sstFiles)
        assertTrue(sstFiles!!.isNotEmpty())
        val targetFile = sstFiles.first()
        targetFile.appendText("corruption_bytes")

        try {
            db.restoreFromBackup(backupDir)
            fail("Expected BackupRestoreException due to checksum mismatch")
        } catch (e: BackupRestoreException) {
            assertTrue(e.message!!.contains("Checksum mismatch"))
        }
    }
}
