---
name: koredb-transactions
description: Manage ACID MVCC transactions, Snapshot Isolation, direct commit pipelines, and Change Data Capture (CDC) in KoreDB. Use when executing multi-step atomic writes, financial/transfer operations, handling optimistic concurrency conflicts, subscribing to live CDC event streams, creating point-in-time backups, or verifying database integrity.
---

# KoreDB Transactions, MVCC & CDC Guide

KoreDB implements Multi-Version Concurrency Control (MVCC) with **Snapshot Isolation**, providing strict ACID guarantees without blocking concurrent readers.

---

## 1. ACID MVCC Transactions

Transactions operate on a consistent snapshot of the database established at the start of the transaction. Writes are buffered locally and committed atomically:

```kotlin
val accounts = db.collection("accounts", Account.serializer()) { it.id }

val committed = db.transaction {
    val sender = accounts.getById("acc_alice") ?: return@transaction false
    val receiver = accounts.getById("acc_bob") ?: return@transaction false

    if (sender.balance < 100.0) {
        return@transaction false // Rollback transaction
    }

    // Atomic debit and credit
    accounts.insert("acc_alice", sender.copy(balance = sender.balance - 100.0))
    accounts.insert("acc_bob", receiver.copy(balance = receiver.balance + 100.0))

    true // Returning true commits the transaction
}

if (!committed) {
    println("Transaction rolled back or conflicted.")
}
```

### Snapshot Isolation & First-Committer-Wins
* **Non-Blocking Readers**: Readers never block writers, and writers never block readers.
* **Conflict Resolution**: If two transactions attempt to update the same key concurrently, the first transaction to commit succeeds; the second receives a conflict and aborts cleanly without data corruption.

---

## 2. Low-Latency Direct Commits (`writeBatchDirect`)

For single writes or high-frequency telemetry where coroutine context switching is undesirable, KoreDB features a synchronous direct commit pipeline:

```kotlin
// Commits directly to active MemTable and WAL on the caller thread
// Throughput: >102,000 transactions/sec on ARM64
db.writeBatchDirect(listOf(key to value))
```

---

## 3. Change Data Capture (CDC) Event Streams

Subscribe to real-time database mutations to power audit logs, remote server sync, or cross-screen UI reactivity:

```kotlin
import com.pankaj.koredb.cdc.ChangeEvent
import com.pankaj.koredb.cdc.OperationType
import kotlinx.coroutines.flow.collect

// In your background sync service or repository
suspend fun monitorChanges() {
    db.cdcStream().collect { event: ChangeEvent ->
        when (event.type) {
            OperationType.INSERT -> {
                println("Key ${event.key} inserted at ${event.timestamp}")
            }
            OperationType.UPDATE -> {
                println("Key ${event.key} updated")
            }
            OperationType.DELETE -> {
                println("Key ${event.key} deleted")
            }
        }
    }
}
```

---

## 4. Consistent Point-in-Time Backups

Create encrypted, verified point-in-time snapshots of the database without stopping running transactions:

### Create Backup
```kotlin
val backupDestination = File(context.filesDir, "backups/snapshot_2026")

// Flushes in-memory state, syncs with compactor, and writes BACKUP.json with CRC32 checksums
val metadata = db.createBackup(backupDestination)

println("Backup created: ${metadata.sstableFiles.size} SSTables (${metadata.totalSizeBytes} bytes)")
```

### Restore Backup
```kotlin
// Safely restores all SSTables and verifies CRC32 checksums
db.restoreBackup(backupDestination)
```

---

## 5. Storage Health & Integrity Verification

Run the built-in integrity auditor to verify data block checksums and detect potential flash storage corruption:

```kotlin
val report = db.engine.verifyIntegrity()

if (report.isHealthy) {
    println("Database storage is 100% verified and healthy.")
} else {
    println("Storage issues detected: ${report.issues.joinToString()}")
}
```

---

## 6. WAL Durability & Crash Recovery

* **Framing**: Every WAL batch is framed with `RECORD_BEGIN (1)`, `RECORD_PUT (2)`, `BATCH_CRC (4)` (32-bit checksum), and `RECORD_COMMIT (3)`.
* **Automatic Tail Truncation**: If the app is killed by the OS (SIGKILL/LMK) mid-write, the truncated uncommitted bytes at the tail are automatically discarded during startup, rolling back the database to the last valid commit with zero manual intervention.
