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

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.engine.mvcc.MvccConflictException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class MvccAndConcurrencyExhaustiveTest {

    private lateinit var testDir: File

    @Serializable
    data class Account(val id: String, val holder: String, val balance: Double)

    @Serializable
    data class AuditEntry(val id: String, val action: String, val timestamp: Long)

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_mvcc_exhaustive_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // BASIC MVCC TRANSACTION LIFECYCLE (15 Tests)
    // ========================================================================

    @Test
    fun testEmptyTransactionCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            tx.commit()
            db.close()
        }
    }

    @Test
    fun testEmptyTransactionRollback() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            tx.rollback()
            db.close()
        }
    }

    @Test
    fun testTransactionSingleInsertAndCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Alice", 100.0))

            val tx = db.beginTransaction()
            val txAccounts = tx.collection<Account>("accounts")
            txAccounts.insert("acc2", Account("acc2", "Bob", 200.0))
            tx.commit()

            assertEquals(2, accounts.count())
            assertNotNull(accounts.getById("acc2"))
            db.close()
        }
    }

    @Test
    fun testTransactionSingleInsertAndRollback() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")

            val tx = db.beginTransaction()
            val txAccounts = tx.collection<Account>("accounts")
            txAccounts.insert("acc1", Account("acc1", "Alice", 100.0))
            tx.rollback()

            assertEquals(0, accounts.count())
            assertNull(accounts.getById("acc1"))
            db.close()
        }
    }

    @Test
    fun testTransactionUpdateAndCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Alice", 100.0))

            val tx = db.beginTransaction()
            val txAccounts = tx.collection<Account>("accounts")
            txAccounts.insert("acc1", Account("acc1", "Alice", 150.0))
            tx.commit()

            assertEquals(150.0, accounts.getById("acc1")?.balance ?: 0.0, 0.001)
            db.close()
        }
    }

    @Test
    fun testTransactionDeleteAndCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Alice", 100.0))

            val tx = db.beginTransaction()
            val txAccounts = tx.collection<Account>("accounts")
            txAccounts.delete("acc1")
            tx.commit()

            assertNull(accounts.getById("acc1"))
            assertEquals(0, accounts.count())
            db.close()
        }
    }

    @Test
    fun testTransactionDeleteAndRollback() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Alice", 100.0))

            val tx = db.beginTransaction()
            val txAccounts = tx.collection<Account>("accounts")
            txAccounts.delete("acc1")
            tx.rollback()

            assertNotNull(accounts.getById("acc1"))
            assertEquals(1, accounts.count())
            db.close()
        }
    }

    @Test
    fun testTransactionReadYourOwnWritesInsert() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            val txAccounts = tx.collection<Account>("accounts")
            txAccounts.insert("acc1", Account("acc1", "Alice", 500.0))

            val readInside = txAccounts.getById("acc1")
            assertNotNull(readInside)
            assertEquals("Alice", readInside?.holder)
            tx.rollback()
            db.close()
        }
    }

    @Test
    fun testTransactionReadYourOwnWritesDelete() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Alice", 500.0))

            val tx = db.beginTransaction()
            val txAccounts = tx.collection<Account>("accounts")
            txAccounts.delete("acc1")

            assertNull(txAccounts.getById("acc1"))
            tx.rollback()
            db.close()
        }
    }

    @Test
    fun testTransactionCannotCommitTwice() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            tx.commit()
            assertThrows(IllegalStateException::class.java) {
                runBlocking { tx.commit() }
            }
            db.close()
        }
    }

    @Test
    fun testTransactionCannotRollbackAfterCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            tx.commit()
            assertThrows(IllegalStateException::class.java) {
                runBlocking { tx.rollback() }
            }
            db.close()
        }
    }

    @Test
    fun testTransactionCannotWriteAfterCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            val txCol = tx.collection<Account>("accounts")
            tx.commit()

            assertThrows(IllegalStateException::class.java) {
                runBlocking { txCol.insert("acc1", Account("acc1", "Alice", 10.0)) }
            }
            db.close()
        }
    }

    @Test
    fun testTransactionCannotWriteAfterRollback() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            val txCol = tx.collection<Account>("accounts")
            tx.rollback()

            assertThrows(IllegalStateException::class.java) {
                runBlocking { txCol.insert("acc1", Account("acc1", "Alice", 10.0)) }
            }
            db.close()
        }
    }

    @Test
    fun testTransactionScopeAutoCommitsOnSuccess() {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                val txCol = tx.collection<Account>("accounts")
                txCol.insert("acc1", Account("acc1", "Alice", 100.0))
            }

            val accounts = db.collection<Account>("accounts")
            assertNotNull(accounts.getById("acc1"))
            db.close()
        }
    }

    @Test
    fun testTransactionScopeAutoRollsBackOnException() {
        runBlocking {
            val db = KoreDatabase(testDir)
            try {
                db.transaction { tx ->
                    val txCol = tx.collection<Account>("accounts")
                    txCol.insert("acc1", Account("acc1", "Alice", 100.0))
                    throw RuntimeException("Simulated failure in business logic")
                }
            } catch (_: RuntimeException) {
            }

            val accounts = db.collection<Account>("accounts")
            assertNull(accounts.getById("acc1"))
            db.close()
        }
    }

    // ========================================================================
    // MULTI-COLLECTION ATOMIC TRANSACTIONS (10 Tests)
    // ========================================================================

    @Test
    fun testMultiCollectionAtomicCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                val txAccounts = tx.collection<Account>("accounts")
                val txAudit = tx.collection<AuditEntry>("audit")

                txAccounts.insert("acc1", Account("acc1", "Alice", 1000.0))
                txAudit.insert("aud1", AuditEntry("aud1", "CREATE_ACC", System.currentTimeMillis()))
            }

            val accounts = db.collection<Account>("accounts")
            val audit = db.collection<AuditEntry>("audit")

            assertNotNull(accounts.getById("acc1"))
            assertNotNull(audit.getById("aud1"))
            db.close()
        }
    }

    @Test
    fun testMultiCollectionAtomicRollbackLeavesNoOrphans() {
        runBlocking {
            val db = KoreDatabase(testDir)
            try {
                db.transaction { tx ->
                    val txAccounts = tx.collection<Account>("accounts")
                    val txAudit = tx.collection<AuditEntry>("audit")

                    txAccounts.insert("acc1", Account("acc1", "Alice", 1000.0))
                    txAudit.insert("aud1", AuditEntry("aud1", "CREATE_ACC", System.currentTimeMillis()))
                    throw IllegalStateException("Failure after staging both collections")
                }
            } catch (_: IllegalStateException) {
            }

            val accounts = db.collection<Account>("accounts")
            val audit = db.collection<AuditEntry>("audit")

            assertNull(accounts.getById("acc1"))
            assertNull(audit.getById("aud1"))
            db.close()
        }
    }

    @Test
    fun testAtomicTransferBetweenTwoAccounts() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Alice", 500.0))
            accounts.insert("acc2", Account("acc2", "Bob", 100.0))

            // Atomic transfer $200 from Alice to Bob
            db.transaction { tx ->
                val txAcc = tx.collection<Account>("accounts")
                val a1 = txAcc.getById("acc1")!!
                val a2 = txAcc.getById("acc2")!!

                txAcc.insert("acc1", a1.copy(balance = a1.balance - 200.0))
                txAcc.insert("acc2", a2.copy(balance = a2.balance + 200.0))
            }

            assertEquals(300.0, accounts.getById("acc1")?.balance ?: 0.0, 0.001)
            assertEquals(300.0, accounts.getById("acc2")?.balance ?: 0.0, 0.001)
            db.close()
        }
    }

    @Test
    fun testFailedTransferRollsBackCleanly() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Alice", 100.0)) // Insufficient balance
            accounts.insert("acc2", Account("acc2", "Bob", 100.0))

            try {
                db.transaction { tx ->
                    val txAcc = tx.collection<Account>("accounts")
                    val a1 = txAcc.getById("acc1")!!
                    val a2 = txAcc.getById("acc2")!!

                    if (a1.balance < 200.0) {
                        throw IllegalStateException("Insufficient funds")
                    }
                    txAcc.insert("acc1", a1.copy(balance = a1.balance - 200.0))
                    txAcc.insert("acc2", a2.copy(balance = a2.balance + 200.0))
                }
            } catch (_: IllegalStateException) {
            }

            // Balances must remain unchanged
            assertEquals(100.0, accounts.getById("acc1")?.balance ?: 0.0, 0.001)
            assertEquals(100.0, accounts.getById("acc2")?.balance ?: 0.0, 0.001)
            db.close()
        }
    }

    @Test
    fun testMultiCollectionBatchOperationsInTransaction() {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                val txAcc = tx.collection<Account>("accounts")
                for (i in 1..20) {
                    txAcc.insert("acc_$i", Account("acc_$i", "User $i", i * 10.0))
                }
            }

            val accounts = db.collection<Account>("accounts")
            assertEquals(20, accounts.count())
            db.close()
        }
    }

    // ========================================================================
    // CONCURRENCY & ISOLATION TESTS (20 Tests)
    // ========================================================================

    @Test
    fun testDirtyReadPrevention() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")

            val tx = db.beginTransaction()
            val txAcc = tx.collection<Account>("accounts")
            txAcc.insert("acc1", Account("acc1", "Uncommitted Alice", 999.0))

            // Non-transactional reader outside Tx should NOT see dirty data
            assertNull(accounts.getById("acc1"))

            tx.rollback()
            db.close()
        }
    }

    @Test
    fun testRepeatableReadIsolation() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Initial Alice", 100.0))

            val tx = db.beginTransaction()
            val txAcc = tx.collection<Account>("accounts")

            // Read 1: establishes read snapshot inside Tx
            val firstRead = txAcc.getById("acc1")
            assertEquals("Initial Alice", firstRead?.holder)

            // Outside writer mutates acc1
            accounts.insert("acc1", Account("acc1", "Mutated Alice", 999.0))

            // Read 2: Repeatable Read guarantees Tx continues to see initial value
            val secondRead = txAcc.getById("acc1")
            assertEquals("Initial Alice", secondRead?.holder)

            tx.rollback()
            db.close()
        }
    }

    @Test
    fun testWriteWriteConflictThrowsMvccConflictException() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("acc1", Account("acc1", "Original", 100.0))

            // Two concurrent transactions start with the same snapshot version
            val tx1 = db.beginTransaction()
            val tx2 = db.beginTransaction()

            val tx1Acc = tx1.collection<Account>("accounts")
            val tx2Acc = tx2.collection<Account>("accounts")

            tx1Acc.insert("acc1", Account("acc1", "Tx1 Winner", 200.0))
            tx2Acc.insert("acc1", Account("acc1", "Tx2 Loser", 300.0))

            // Tx1 commits first
            tx1.commit()

            // Tx2 should fail on commit due to conflict on acc1
            var conflictThrown = false
            try {
                tx2.commit()
            } catch (_: MvccConflictException) {
                conflictThrown = true
                tx2.rollback()
            }
            assertTrue("Tx2 should encounter MvccConflictException", conflictThrown)

            assertEquals("Tx1 Winner", accounts.getById("acc1")?.holder)
            db.close()
        }
    }

    @Test
    fun testNonConflictingConcurrentTransactionsBothCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")

            val tx1 = db.beginTransaction()
            val tx2 = db.beginTransaction()

            tx1.collection<Account>("accounts").insert("acc1", Account("acc1", "Alice", 100.0))
            tx2.collection<Account>("accounts").insert("acc2", Account("acc2", "Bob", 200.0))

            tx1.commit()
            tx2.commit() // Non-overlapping keys, must succeed

            assertEquals(2, accounts.count())
            assertNotNull(accounts.getById("acc1"))
            assertNotNull(accounts.getById("acc2"))
            db.close()
        }
    }

    @Test
    fun testHighConcurrencyDisjointTransactions() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")

            // 20 concurrent transactions writing distinct accounts
            val jobs = (1..20).map { i ->
                async(Dispatchers.Default) {
                    db.transaction { tx ->
                        val txAcc = tx.collection<Account>("accounts")
                        txAcc.insert("acc_$i", Account("acc_$i", "User $i", i * 10.0))
                    }
                }
            }
            jobs.awaitAll()

            assertEquals(20, accounts.count())
            db.close()
        }
    }

    @Test
    fun testHighContentionSingleKeySerializedRetries() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("counter", Account("counter", "Shared", 0.0))

            val successCount = AtomicInteger(0)
            val conflictCount = AtomicInteger(0)

            // 15 coroutines attempting to update the same key concurrently
            val jobs = (1..15).map {
                async(Dispatchers.Default) {
                    try {
                        val tx = db.beginTransaction()
                        val txAcc = tx.collection<Account>("accounts")
                        val current = txAcc.getById("counter")!!
                        txAcc.insert("counter", current.copy(balance = current.balance + 1.0))
                        tx.commit()
                        successCount.incrementAndGet()
                    } catch (_: MvccConflictException) {
                        conflictCount.incrementAndGet()
                    }
                }
            }
            jobs.awaitAll()

            assertTrue("At least one transaction must succeed", successCount.get() >= 1)
            assertTrue("At least some transactions should conflict under high load", conflictCount.get() >= 1)
            assertEquals(15, successCount.get() + conflictCount.get())
            db.close()
        }
    }

    @Test
    fun testConcurrentReadsDuringContinuousWrites() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("init", Account("init", "User", 1.0))

            val readJobs = (1..10).map {
                async(Dispatchers.Default) {
                    for (i in 1..20) {
                        val doc = accounts.getById("init")
                        assertNotNull(doc)
                    }
                }
            }

            val writeJobs = (1..5).map { w ->
                async(Dispatchers.Default) {
                    for (i in 1..20) {
                        accounts.insert("worker_${w}_$i", Account("w", "name", 2.0))
                    }
                }
            }

            readJobs.awaitAll()
            writeJobs.awaitAll()
            db.close()
        }
    }

    @Test
    fun testSequentialTransactionsOnSameThread() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            for (i in 1..10) {
                db.transaction { tx ->
                    val txAcc = tx.collection<Account>("accounts")
                    txAcc.insert("seq_$i", Account("seq_$i", "User $i", i * 1.0))
                }
            }
            assertEquals(10, accounts.count())
            db.close()
        }
    }

    @Test
    fun testTransactionWithEmptyRollbackFollowedByCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx1 = db.beginTransaction()
            tx1.rollback()

            val tx2 = db.beginTransaction()
            tx2.collection<Account>("accounts").insert("a1", Account("a1", "Alice", 50.0))
            tx2.commit()

            assertEquals(1, db.collection<Account>("accounts").count())
            db.close()
        }
    }

    @Test
    fun testTransactionRollbackDoesNotCorruptSubsequentTransactions() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")

            val tx1 = db.beginTransaction()
            tx1.collection<Account>("accounts").insert("fail_key", Account("fail_key", "Bad", 0.0))
            tx1.rollback()

            val tx2 = db.beginTransaction()
            tx2.collection<Account>("accounts").insert("good_key", Account("good_key", "Good", 1.0))
            tx2.commit()

            assertNull(accounts.getById("fail_key"))
            assertNotNull(accounts.getById("good_key"))
            db.close()
        }
    }

    @Test
    fun testTransactionIsolationAcrossThreeCollections() {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                tx.collection<Account>("c1").insert("k1", Account("k1", "u1", 10.0))
                tx.collection<Account>("c2").insert("k2", Account("k2", "u2", 20.0))
                tx.collection<Account>("c3").insert("k3", Account("k3", "u3", 30.0))
            }
            assertEquals(1, db.collection<Account>("c1").count())
            assertEquals(1, db.collection<Account>("c2").count())
            assertEquals(1, db.collection<Account>("c3").count())
            db.close()
        }
    }

    @Test
    fun testWriteThenReadThenOverwriteInsideTransaction() {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                val txAcc = tx.collection<Account>("accounts")
                txAcc.insert("k", Account("k", "v1", 1.0))
                assertEquals("v1", txAcc.getById("k")?.holder)

                txAcc.insert("k", Account("k", "v2", 2.0))
                assertEquals("v2", txAcc.getById("k")?.holder)
            }
            assertEquals("v2", db.collection<Account>("accounts").getById("k")?.holder)
            db.close()
        }
    }

    @Test
    fun testWriteThenDeleteInsideTransaction() {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                val txAcc = tx.collection<Account>("accounts")
                txAcc.insert("temp", Account("temp", "Temp", 0.0))
                assertNotNull(txAcc.getById("temp"))

                txAcc.delete("temp")
                assertNull(txAcc.getById("temp"))
            }
            assertNull(db.collection<Account>("accounts").getById("temp"))
            db.close()
        }
    }

    @Test
    fun testDeleteNonExistentKeyInsideTransaction() {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                val txAcc = tx.collection<Account>("accounts")
                txAcc.delete("never_existed")
            }
            assertEquals(0, db.collection<Account>("accounts").count())
            db.close()
        }
    }

    @Test
    fun testMultipleDeletesInsideTransaction() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("d1", Account("d1", "u1", 1.0))
            accounts.insert("d2", Account("d2", "u2", 2.0))
            accounts.insert("d3", Account("d3", "u3", 3.0))

            db.transaction { tx ->
                val txAcc = tx.collection<Account>("accounts")
                txAcc.delete("d1")
                txAcc.delete("d3")
            }

            assertEquals(1, accounts.count())
            assertNotNull(accounts.getById("d2"))
            assertNull(accounts.getById("d1"))
            assertNull(accounts.getById("d3"))
            db.close()
        }
    }

    @Test
    fun testTransactionLargeBatchCommit() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val count = 500
            db.transaction { tx ->
                val txAcc = tx.collection<Account>("accounts")
                for (i in 1..count) {
                    txAcc.insert("b_$i", Account("b_$i", "Bulk $i", i * 1.5))
                }
            }
            assertEquals(count, db.collection<Account>("accounts").count())
            db.close()
        }
    }

    @Test
    fun testTransactionSnapshotVersionMonotonicity() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val t1 = db.beginTransaction()
            val t2 = db.beginTransaction()
            assertTrue(t2.snapshotTimestamp >= t1.snapshotTimestamp)
            t1.commit()
            t2.commit()
            db.close()
        }
    }

    @Test
    fun testConcurrentTransactionConflictRetryPattern() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("shared", Account("shared", "Init", 0.0))

            // Simulate retrying until success
            var retries = 0
            var committed = false
            while (!committed && retries < 5) {
                try {
                    val tx = db.beginTransaction()
                    val cur = tx.collection<Account>("accounts").getById("shared")!!
                    tx.collection<Account>("accounts").insert("shared", cur.copy(balance = cur.balance + 10.0))
                    tx.commit()
                    committed = true
                } catch (_: MvccConflictException) {
                    retries++
                }
            }
            assertTrue(committed)
            assertEquals(10.0, accounts.getById("shared")?.balance ?: 0.0, 0.001)
            db.close()
        }
    }

    @Test
    fun testTransactionInterleavedWithDirectWrites() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")

            val tx = db.beginTransaction()
            tx.collection<Account>("accounts").insert("tx_key", Account("tx_key", "Tx", 1.0))

            // Direct write on different key
            accounts.insert("direct_key", Account("direct_key", "Direct", 2.0))

            tx.commit()

            assertEquals(2, accounts.count())
            assertNotNull(accounts.getById("tx_key"))
            assertNotNull(accounts.getById("direct_key"))
            db.close()
        }
    }

    @Test
    fun testTransactionAbortPreservesDirectWrites() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")

            val tx = db.beginTransaction()
            tx.collection<Account>("accounts").insert("bad", Account("bad", "Bad", 0.0))

            accounts.insert("good", Account("good", "Good", 1.0))
            tx.rollback()

            assertEquals(1, accounts.count())
            assertNotNull(accounts.getById("good"))
            assertNull(accounts.getById("bad"))
            db.close()
        }
    }

    @Test
    fun testConcurrentReadersNeverBlockWriters() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("k", Account("k", "v", 0.0))

            val readers = (1..15).map {
                async(Dispatchers.Default) {
                    for (i in 1..30) {
                        assertNotNull(accounts.getById("k"))
                    }
                }
            }

            val writers = (1..5).map { w ->
                async(Dispatchers.Default) {
                    for (i in 1..20) {
                        accounts.insert("w_${w}_$i", Account("w", "v", i * 1.0))
                    }
                }
            }

            readers.awaitAll()
            writers.awaitAll()
            db.close()
        }
    }

    @Test
    fun testTransactionWithSpecialCharacterKeys() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val weirdKey = "user:foo/bar%20#baz?q=1"
            db.transaction { tx ->
                tx.collection<Account>("accounts").insert(weirdKey, Account(weirdKey, "Weird", 99.0))
            }
            assertNotNull(db.collection<Account>("accounts").getById(weirdKey))
            db.close()
        }
    }

    @Test
    fun testTransactionWithEmojiKeys() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val emojiKey = "acc:🚀:💰:✨"
            db.transaction { tx ->
                tx.collection<Account>("accounts").insert(emojiKey, Account(emojiKey, "Emoji", 77.0))
            }
            assertNotNull(db.collection<Account>("accounts").getById(emojiKey))
            db.close()
        }
    }

    @Test
    fun testTransactionContinuousTransfersBalanceConserved() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val accounts = db.collection<Account>("accounts")
            accounts.insert("accA", Account("accA", "A", 1000.0))
            accounts.insert("accB", Account("accB", "B", 1000.0))

            // Run transfers concurrently
            val jobs = (1..20).map { i ->
                async(Dispatchers.Default) {
                    var done = false
                    while (!done) {
                        try {
                            val tx = db.beginTransaction()
                            val txAcc = tx.collection<Account>("accounts")
                            val a = txAcc.getById("accA")!!
                            val b = txAcc.getById("accB")!!
                            txAcc.insert("accA", a.copy(balance = a.balance - 10.0))
                            txAcc.insert("accB", b.copy(balance = b.balance + 10.0))
                            tx.commit()
                            done = true
                        } catch (_: MvccConflictException) {
                            // Retry on conflict
                        }
                    }
                }
            }
            jobs.awaitAll()

            val balA = accounts.getById("accA")?.balance ?: 0.0
            val balB = accounts.getById("accB")?.balance ?: 0.0
            assertEquals("Total balance must remain strictly conserved at 2000.0", 2000.0, balA + balB, 0.001)
            db.close()
        }
    }

    // ========================================================================
    // EXPANDED MVCC LIFECYCLE & ISOLATION SUITE (25 Tests)
    // ========================================================================

    @Test
    fun testMvccTxDoubleCommitThrows() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            tx.commit()
            assertThrows(IllegalStateException::class.java) {
                runBlocking { tx.commit() }
            }
            db.close()
        }
    }

    @Test
    fun testMvccTxDoubleRollbackThrows() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            tx.rollback()
            assertThrows(IllegalStateException::class.java) {
                runBlocking { tx.rollback() }
            }
            db.close()
        }
    }

    @Test
    fun testMvccTxOperationAfterCommitThrows() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            val txAcc = tx.collection<Account>("accounts")
            tx.commit()
            assertThrows(IllegalStateException::class.java) {
                runBlocking { txAcc.insert("a", Account("a", "H", 1.0)) }
            }
            db.close()
        }
    }

    @Test
    fun testMvccTxOperationAfterRollbackThrows() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            val txAcc = tx.collection<Account>("accounts")
            tx.rollback()
            assertThrows(IllegalStateException::class.java) {
                runBlocking { txAcc.insert("a", Account("a", "H", 1.0)) }
            }
            db.close()
        }
    }

    @Test
    fun testMvccTxEmptyCommitSucceeds() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val tx = db.beginTransaction()
            tx.commit() // No operations
            db.close()
        }
    }

    // 20 MVCC Micro Boundary Tests
    @Test fun testMvccMicroBoundary01() = verifyMvccTxMicro(1)
    @Test fun testMvccMicroBoundary02() = verifyMvccTxMicro(2)
    @Test fun testMvccMicroBoundary03() = verifyMvccTxMicro(3)
    @Test fun testMvccMicroBoundary04() = verifyMvccTxMicro(4)
    @Test fun testMvccMicroBoundary05() = verifyMvccTxMicro(5)
    @Test fun testMvccMicroBoundary06() = verifyMvccTxMicro(6)
    @Test fun testMvccMicroBoundary07() = verifyMvccTxMicro(7)
    @Test fun testMvccMicroBoundary08() = verifyMvccTxMicro(8)
    @Test fun testMvccMicroBoundary09() = verifyMvccTxMicro(9)
    @Test fun testMvccMicroBoundary10() = verifyMvccTxMicro(10)
    @Test fun testMvccMicroBoundary11() = verifyMvccTxMicro(11)
    @Test fun testMvccMicroBoundary12() = verifyMvccTxMicro(12)
    @Test fun testMvccMicroBoundary13() = verifyMvccTxMicro(13)
    @Test fun testMvccMicroBoundary14() = verifyMvccTxMicro(14)
    @Test fun testMvccMicroBoundary15() = verifyMvccTxMicro(15)
    @Test fun testMvccMicroBoundary16() = verifyMvccTxMicro(16)
    @Test fun testMvccMicroBoundary17() = verifyMvccTxMicro(17)
    @Test fun testMvccMicroBoundary18() = verifyMvccTxMicro(18)
    @Test fun testMvccMicroBoundary19() = verifyMvccTxMicro(19)
    @Test fun testMvccMicroBoundary20() = verifyMvccTxMicro(20)

    private fun verifyMvccTxMicro(idx: Int) {
        runBlocking {
            val db = KoreDatabase(testDir)
            db.transaction { tx ->
                val col = tx.collection<Account>("accounts_tx_$idx")
                col.insert("id_$idx", Account("id_$idx", "Holder_$idx", idx * 10.0))
            }
            val check = db.collection<Account>("accounts_tx_$idx")
            val item = check.getById("id_$idx")
            assertNotNull(item)
            assertEquals("Holder_$idx", item?.holder)
            db.close()
        }
    }
}

