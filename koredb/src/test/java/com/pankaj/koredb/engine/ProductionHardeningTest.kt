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

/**
 * Validates the 5 Production-Hardening features:
 * 1. Process File Locking (kore.lock)
 * 2. Schema Versioning & Automated Migrations
 * 3. Torn-Write Resilience & Corrupt SSTable Auto-Quarantine (.corrupt)
 */
class ProductionHardeningTest {

    private lateinit var testDir: File

    @Serializable
    data class UserV1(val id: String, val name: String)

    @Serializable
    data class UserV2(val id: String, val fullName: String, val migrated: Boolean)

    @Before
    fun setup() {
        testDir = File("build/tmp/test_prod_${UUID.randomUUID()}").apply { mkdirs() }
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // 1. PROCESS FILE LOCKING (kore.lock)
    // ========================================================================

    @Test
    fun `test Process File Lock prevents concurrent instances on same directory`() {
        val db1 = KoreDatabase(testDir)
        // Ensure db1 acquired the lock
        assertNotNull(db1.engine)

        // Attempting to open second instance on same directory must throw DatabaseLockedException
        assertThrows(DatabaseLockedException::class.java) {
            KoreDatabase(testDir).engine
        }

        // Closing db1 releases the lock
        db1.close()

        // Opening db2 now succeeds
        val db2 = KoreDatabase(testDir)
        assertNotNull(db2.engine)
        db2.close()
    }

    // ========================================================================
    // 2. SCHEMA VERSIONING & AUTOMATED MIGRATIONS
    // ========================================================================

    @Test
    fun `test Schema Version advances and triggers onMigrate callback`() = runBlocking {
        // Step 1: Open V1 database and populate initial data
        val dbV1 = KoreDatabase(testDir, targetSchemaVersion = 1)
        val colV1 = dbV1.collection<UserV1>("users")
        colV1.insert("u1", UserV1("u1", "Alice Smith"))
        assertEquals(1, dbV1.schemaVersion)
        dbV1.close()

        // Step 2: Open V2 database with migration logic
        var migrationTriggered = false
        var observedOldVersion = -1
        var observedNewVersion = -1

        val dbV2 = KoreDatabase(
            directory = testDir,
            targetSchemaVersion = 2,
            onMigrate = { db, oldVer, newVer ->
                migrationTriggered = true
                observedOldVersion = oldVer
                observedNewVersion = newVer

                val oldCol = db.collection<UserV1>("users")
                val oldAlice = oldCol.getById("u1")
                assertNotNull(oldAlice)

                val newCol = db.collection<UserV2>("users")
                runBlocking {
                    newCol.insert("u1", UserV2(oldAlice!!.id, oldAlice.name, migrated = true))
                }
            }
        )

        assertTrue("onMigrate should have been executed", migrationTriggered)
        assertEquals(1, observedOldVersion)
        assertEquals(2, observedNewVersion)
        assertEquals(2, dbV2.schemaVersion)

        // Verify migrated document
        val colV2 = dbV2.collection<UserV2>("users")
        val aliceV2 = colV2.getById("u1")
        assertNotNull(aliceV2)
        assertEquals("Alice Smith", aliceV2!!.fullName)
        assertTrue(aliceV2.migrated)

        dbV2.close()
    }

    // ========================================================================
    // 3. TORN-WRITE RESILIENCE & CORRUPT SSTABLE QUARANTINE
    // ========================================================================

    @Test
    fun `test Corrupt SSTable is quarantined to dot-corrupt without failing database load`() = runBlocking {
        // 1. Create a valid DB with real data
        val db1 = KoreDatabase(testDir)
        val col1 = db1.collection<UserV1>("test")
        col1.insert("k1", UserV1("k1", "Valid User"))
        db1.engine.flushMemTableInternal()
        db1.close()

        // 2. Inject a corrupt/truncated SSTable simulating hard power loss
        val corruptFile = File(testDir, "segment_999.sst")
        corruptFile.writeBytes(byteArrayOf(0x00, 0x01, 0x02, 0x03, 0x04)) // truncated garbage bytes

        // 3. Reopen database - should quarantine the corrupt segment and still load valid data
        val db2 = KoreDatabase(testDir)
        val col2 = db2.collection<UserV1>("test")
        val user = col2.getById("k1")

        assertNotNull("Valid data must remain accessible despite neighboring corrupt SSTable", user)
        assertEquals("Valid User", user!!.name)

        // Verify corrupt file was quarantined
        assertFalse("Original corrupt SSTable should no longer exist", corruptFile.exists())
        val quarantinedFile = File(testDir, "segment_999.sst.corrupt")
        assertTrue("Corrupt SSTable must be renamed to .corrupt", quarantinedFile.exists())

        db2.close()
    }
}
