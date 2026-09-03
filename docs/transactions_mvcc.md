# Transactions & Multi-Version Concurrency Control (MVCC)

KoreDB features full ACID transaction support backed by **Snapshot Isolation (MVCC)** with optimistic concurrency control.

In our comparative benchmarks against Room / SQLite, KoreDB executes 1,000 discrete transactions in **9.77 ms** (**>102,000 transactions/sec**), outperforming SQLite's 42.11 ms by **4.3x**.

---

## 1. Snapshot Isolation Guarantee

When a transaction begins, KoreDB records a logical monotonic snapshot timestamp:
- **Consistent Read View**: The transaction sees all data committed prior to its snapshot timestamp. Mutations committed by concurrent transactions after this timestamp are isolated.
- **Repeatable Reads**: Re-reading the same document during the transaction always returns identical state.
- **First-Committer-Wins (Write-Write Conflict Detection)**: If two concurrent transactions attempt to modify the same key, the first transaction to commit succeeds; the second transaction throws an `MvccConflictException`.

```
  Time ───────────────►

  Tx 1: [Begin Snapshot: T1] ───► Mutate "acc_1" ───► Commit (T2) [SUCCESS]
  
  Tx 2:        [Begin Snapshot: T1] ───► Mutate "acc_1" ───► Commit (T3)
                                                               │
                                                               ▼
                                               [MvccConflictException]
                                          Key "acc_1" was committed at T2 > T1
```

---

## 2. Using Transactions

Wrap operations in `db.transaction { tx -> ... }`. Unhandled exceptions automatically abort and discard all uncommitted writes.

```kotlin
try {
    db.transaction { tx ->
        val accounts = tx.binaryCollection<Account>("accounts")

        val sender = accounts.getById("acc_alice") ?: error("Sender not found")
        val receiver = accounts.getById("acc_bob") ?: error("Receiver not found")

        val transferAmount = 100.0
        require(sender.balance >= transferAmount) { "Insufficient funds" }

        // Mutate accounts atomically within transaction
        accounts.insert("acc_alice", sender.copy(balance = sender.balance - transferAmount))
        accounts.insert("acc_bob", receiver.copy(balance = receiver.balance + transferAmount))
    }
    println("Transfer successfully committed!")
} catch (e: MvccConflictException) {
    println("Concurrent transaction conflict detected. Retrying...")
} catch (e: Exception) {
    println("Transaction aborted: ${e.message}")
}
```

---

## 3. High-Performance Commit Architecture

KoreDB eliminates coroutine scheduling latency during discrete commits:

1. **Zero-Allocation Write Buffer**:
   - Transaction write buffers bypass string allocations by referencing raw byte arrays directly.
   - Commit batches are transferred directly to the WAL without intermediate `.toList()` or `.toSet()` memory copies.
2. **Direct Synchronous Fast-Path (`writeBatchDirect`)**:
   - In uncontended environments (single-threaded transaction execution), commits append directly to the WAL and insert into the MemTable without allocating `CompletableDeferred` handles or suspending coroutines.
   - Latency per discrete commit is under **10 microseconds**.
3. **Automatic Cache Invalidation**:
   - Upon successful commit, modified document keys are invalidated across all active `KoreCollection` in-memory LRU caches, preventing stale reads.
