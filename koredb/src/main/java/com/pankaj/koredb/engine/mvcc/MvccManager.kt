package com.pankaj.koredb.engine.mvcc

import com.pankaj.koredb.engine.KoreDB
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Coordinates MVCC snapshot timestamps, transaction commits, and write-write conflict detection.
 */
class MvccManager {

    private val monotonicClock = AtomicLong(1)
    val commitLock = Any()

    // Tracks the commit timestamp of the latest mutation for each key
    private val keyLastCommit = ConcurrentHashMap<String, Long>()

    /**
     * Obtains the current logical timestamp to establish a consistent read snapshot.
     */
    fun beginSnapshot(): Long = monotonicClock.get()

    /**
     * Checks for write-write conflicts before committing a transaction.
     * Throws [MvccConflictException] if any key modified in this transaction
     * was committed by another concurrent transaction after [snapshotTimestamp].
     */
    fun validateNoConflicts(snapshotTimestamp: Long, writeKeys: Set<String>) {
        for (key in writeKeys) {
            val lastCommit = keyLastCommit[key]
            if (lastCommit != null && lastCommit > snapshotTimestamp) {
                throw MvccConflictException(
                    "Write-write conflict detected on key '$key'. " +
                    "Committed at timestamp $lastCommit > transaction snapshot $snapshotTimestamp."
                )
            }
        }
    }

    /**
     * Advances the clock and records the commit timestamp for all modified keys.
     */
    fun recordCommit(writeKeys: Set<String>): Long {
        val commitTimestamp = monotonicClock.incrementAndGet()
        for (key in writeKeys) {
            keyLastCommit[key] = commitTimestamp
        }
        return commitTimestamp
    }

    fun clear() {
        keyLastCommit.clear()
        monotonicClock.set(1)
    }
}
