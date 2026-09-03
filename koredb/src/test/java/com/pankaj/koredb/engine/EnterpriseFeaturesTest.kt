/*
 * Copyright 2026 KoreDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pankaj.koredb.engine

import com.pankaj.koredb.cdc.MutationListener
import com.pankaj.koredb.cdc.MutationOp
import com.pankaj.koredb.cdc.MutationRecord
import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

class EnterpriseFeaturesTest {

    private lateinit var testDir: File

    @Serializable
    data class Account(val id: String, val holder: String, val balance: Double)

    @Serializable
    data class AuditEntry(val id: String, val action: String, val amount: Double)

    @Serializable
    data class Employee(val id: String, val name: String, val department: String, val salary: Double)

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_enterprise_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // 1. TYPED MVCC TRANSACTIONS (CROSS-COLLECTION ATOMIC COMMIT & ROLLBACK)
    // ========================================================================

    @Test
    fun testTypedMvccTransactionCommitAndRollback() = runBlocking {
        val db = KoreDatabase(testDir)
        val accounts = db.collection<Account>("accounts")
        val audit = db.collection<AuditEntry>("audit")

        // Initial state
        accounts.insert("acc_alice", Account("acc_alice", "Alice", 1000.0))
        accounts.insert("acc_bob", Account("acc_bob", "Bob", 500.0))

        // 1. Successful atomic transfer
        db.transaction { tx ->
            val txAccounts = tx.collection<Account>("accounts")
            val txAudit = tx.collection<AuditEntry>("audit")

            val alice = txAccounts.getById("acc_alice")!!
            val bob = txAccounts.getById("acc_bob")!!

            txAccounts.update("acc_alice") { it.copy(balance = alice.balance - 200.0) }
            txAccounts.update("acc_bob") { it.copy(balance = bob.balance + 200.0) }
            txAudit.insert("audit_1", AuditEntry("audit_1", "Transfer Alice -> Bob", 200.0))
        }

        assertEquals(800.0, accounts.getById("acc_alice")?.balance ?: 0.0, 0.001)
        assertEquals(700.0, accounts.getById("acc_bob")?.balance ?: 0.0, 0.001)
        assertNotNull(audit.getById("audit_1"))

        // 2. Failed transfer with automatic rollback
        try {
            db.transaction { tx ->
                val txAccounts = tx.collection<Account>("accounts")
                txAccounts.update("acc_alice") { it.copy(balance = it.balance - 10000.0) }
                throw IllegalStateException("Insufficient funds simulated!")
            }
            fail("Expected exception")
        } catch (_: IllegalStateException) {
            // Expected
        }

        // Verify state is completely unchanged
        assertEquals(800.0, accounts.getById("acc_alice")?.balance ?: 0.0, 0.001)
        db.close()
    }

    // ========================================================================
    // 2. OFFLINE-FIRST CHANGE DATA CAPTURE (CDC / DELTA REPLICATION)
    // ========================================================================

    @Test
    fun testChangeDataCaptureRecordingAndPruning() = runBlocking {
        val db = KoreDatabase(testDir)
        val employees = db.collection<Employee>("employees")

        val streamedEvents = mutableListOf<MutationRecord>()
        val listener = MutationListener { streamedEvents.add(it) }
        db.registerMutationListener(listener)

        val startTime = System.currentTimeMillis()

        employees.insert("e1", Employee("e1", "Diana Prince", "Engineering", 120000.0))
        employees.insert("e2", Employee("e2", "Clark Kent", "Journalism", 85000.0))
        employees.delete("e1")

        // Check real-time listener
        assertEquals(3, streamedEvents.size)
        assertEquals(MutationOp.INSERT, streamedEvents[0].operation)
        assertEquals("e1", streamedEvents[0].documentId)
        assertEquals(MutationOp.INSERT, streamedEvents[1].operation)
        assertEquals(MutationOp.DELETE, streamedEvents[2].operation)

        // Query delta since startTime
        val mutations = db.getMutationsSince(startTime)
        assertEquals(3, mutations.size)

        // Prune first 2 mutations
        val lastSyncSeq = mutations[1].sequence
        db.acknowledgeMutations(lastSyncSeq)

        // Verify pruned
        val remaining = db.getMutationsSince(startTime)
        assertEquals(1, remaining.size)
        assertEquals("e1", remaining[0].documentId)
        assertEquals(MutationOp.DELETE, remaining[0].operation)

        db.unregisterMutationListener(listener)
        db.close()
    }

    // ========================================================================
    // 3. REACTIVE FILTERED QUERIES (query().asFlow())
    // ========================================================================

    @Test
    fun testReactiveQueryAsFlow() = runBlocking {
        val db = KoreDatabase(testDir)
        val employees = db.collection<Employee>("employees")
        employees.createIndex("department") { it.department }

        employees.insert("e1", Employee("e1", "Alice", "Engineering", 100000.0))
        employees.insert("e2", Employee("e2", "Bob", "HR", 70000.0))

        val engineeringFlow = employees.query()
            .whereEq("department", "Engineering")
            .asFlow()

        val emissions = mutableListOf<List<Employee>>()
        val collectJob = launch {
            engineeringFlow.collect { list ->
                emissions.add(list)
            }
        }

        // Wait for initial emission
        while (emissions.isEmpty()) {
            kotlinx.coroutines.delay(10)
        }
        assertEquals(1, emissions.last().size)
        assertEquals("Alice", emissions.last()[0].name)

        // Add matching document -> flow emits updated list
        employees.insert("e3", Employee("e3", "Charlie", "Engineering", 110000.0))
        kotlinx.coroutines.delay(50)
        assertEquals(2, emissions.last().size)

        // Add non-matching document -> distinctUntilChanged ensures no spurious emission
        val emissionCountBefore = emissions.size
        employees.insert("e4", Employee("e4", "David", "Marketing", 90000.0))
        kotlinx.coroutines.delay(50)
        assertEquals(emissionCountBefore, emissions.size)

        collectJob.cancel()
        db.close()
    }

    // ========================================================================
    // 4. INDEX BACKFILLING & MAINTENANCE (rebuildIndexes)
    // ========================================================================

    @Test
    fun testIndexBackfillingOnExistingData() = runBlocking {
        val db = KoreDatabase(testDir)
        val employees = db.collection<Employee>("employees")

        // Insert documents BEFORE registering index
        for (i in 1..20) {
            val dept = if (i % 2 == 0) "Engineering" else "Sales"
            employees.insert("emp_$i", Employee("emp_$i", "Emp $i", dept, i * 5000.0))
        }

        // Register index retroactively
        employees.createIndex("department") { it.department }
        employees.createNumericIndex("salary") { it.salary }

        // At this point, new index has not indexed old documents
        // Run rebuildIndexes()
        employees.rebuildIndexes()

        // Verify secondary index works instantly with pushdown
        val engineers = employees.query().whereEq("department", "Engineering").execute()
        assertEquals(10, engineers.size)

        val highEarners = employees.query().whereGte("salary", 50000.0).execute()
        assertEquals(11, highEarners.size)

        db.close()
    }

    // ========================================================================
    // 5. PROACTIVE DATABASE INTEGRITY VERIFICATION (verifyIntegrity)
    // ========================================================================

    @Test
    fun testDatabaseIntegrityVerification() = runBlocking {
        val db = KoreDatabase(testDir)
        val employees = db.collection<Employee>("employees")

        for (i in 1..50) {
            employees.insert("emp_$i", Employee("emp_$i", "Emp $i", "Tech", 1000.0))
        }
        db.engine.flushMemTableInternal()

        // 1. Verify healthy database
        val report = db.verifyIntegrity()
        assertTrue("Healthy DB should pass verification", report.isHealthy)
        assertTrue(report.issues.isEmpty())
        assertTrue(report.sstablesChecked >= 1)
        assertTrue(report.totalKeysChecked > 0)

        // 2. Corrupt an SSTable directly on disk
        val sst = testDir.listFiles { _, name -> name.endsWith(".sst") }!!.first()
        java.io.RandomAccessFile(sst, "rw").use { raf ->
            raf.seek(sst.length() - 4)
            raf.writeInt(0xDEADBEEF.toInt()) // Damage footer magic
        }

        val badReport = db.verifyIntegrity()
        assertFalse("Damaged SSTable should fail verification", badReport.isHealthy)
        assertTrue(badReport.issues.any { it.contains("Corrupt SSTable") || it.contains("Magic Number") })

        db.close()
    }

    // ========================================================================
    // 6. STORAGE QUOTA & ENOSPC GUARDRAILS (minFreeSpaceBytes)
    // ========================================================================

    @Test
    fun testStorageQuotaGuardrailRejectsWrites() = runBlocking {
        // Set minFreeSpaceBytes to an exorbitantly large number (1 Exabyte)
        // This guarantees usableSpace < minFreeSpaceBytes and triggers the guardrail
        val safeDb = KoreDatabase(
            directory = testDir,
            minFreeSpaceBytes = Long.MAX_VALUE / 2
        )

        assertThrows(DiskSpaceExhaustedException::class.java) {
            runBlocking {
                val col = safeDb.collection<Employee>("employees")
                col.insert("e1", Employee("e1", "Test", "IT", 50000.0))
            }
        }

        safeDb.close()
    }
}
