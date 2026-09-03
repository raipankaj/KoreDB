package com.pankaj.koredb.engine.mvcc

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.engine.KoreDB
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap

/**
 * An isolated transaction operating under Snapshot Isolation.
 *
 * Reads observe state frozen at [snapshotTimestamp]. Writes accumulate locally
 * and are atomically committed to the underlying engine upon successful completion,
 * checking for write-write conflicts.
 */
class MvccTransaction(
    val db: KoreDatabase,
    val snapshotTimestamp: Long,
    private val manager: MvccManager
) {

    private val writeBuffer = ConcurrentHashMap<String, Pair<ByteArray, ByteArray>>()
    private val readKeys = ConcurrentHashMap.newKeySet<String>()

    @Volatile
    private var isCommitted = false

    @Volatile
    private var isAborted = false

    /**
     * Reads raw bytes for a key. Checks local write buffer first before querying database.
     */
    private val readCache = mutableMapOf<String, ByteArray?>()

    fun getRaw(key: ByteArray): ByteArray? {
        val keyStr = String(key, Charsets.ISO_8859_1)
        val local = writeBuffer[keyStr]
        if (local != null) {
            val value = local.second
            return if (value.contentEquals(KoreDB.TOMBSTONE)) null else value
        }
        if (readCache.containsKey(keyStr)) {
            return readCache[keyStr]
        }
        readKeys.add(keyStr)
        val value = db.engine.getRaw(key)
        readCache[keyStr] = value
        return value
    }

    /**
     * Puts a key-value pair into the transaction's private write buffer.
     */
    fun putRaw(key: ByteArray, value: ByteArray) {
        checkActive()
        val keyStr = String(key, Charsets.ISO_8859_1)
        writeBuffer[keyStr] = Pair(key, value)
    }

    /**
     * Deletes a key within the transaction's private write buffer.
     */
    fun deleteRaw(key: ByteArray) {
        checkActive()
        val keyStr = String(key, Charsets.ISO_8859_1)
        writeBuffer[keyStr] = Pair(key, KoreDB.TOMBSTONE)
    }

    /**
     * Accesses a typed [MvccCollection] within this transaction.
     */
    inline fun <reified T : Any> collection(name: String): MvccCollection<T> {
        val col = db.collection<T>(name)
        return MvccCollection(this, col)
    }

    /**
     * Accesses a typed [MvccCollection] using an explicit [kotlinx.serialization.KSerializer].
     */
    fun <T : Any> collection(name: String, serializer: kotlinx.serialization.KSerializer<T>): MvccCollection<T> {
        val col = db.collection(name, serializer)
        return MvccCollection(this, col)
    }

    @PublishedApi
    internal var cachedCollectionName: String? = null
    @PublishedApi
    internal var cachedCollectionInstance: MvccCollection<*>? = null

    inline fun <reified T : Any> binaryCollection(name: String): MvccCollection<T> {
        val cached = cachedCollectionInstance
        if (cachedCollectionName == name && cached != null) {
            @Suppress("UNCHECKED_CAST")
            return cached as MvccCollection<T>
        }
        val col = binaryCollection(name, kotlinx.serialization.serializer<T>())
        cachedCollectionName = name
        cachedCollectionInstance = col
        return col
    }

    fun <T : Any> binaryCollection(name: String, serializer: kotlinx.serialization.KSerializer<T>): MvccCollection<T> {
        val col = db.binaryCollection(name, serializer)
        return MvccCollection(this, col)
    }

    /**
     * Commits all buffered mutations with write-write conflict verification.
     */
    fun commit() {
        checkActive()
        if (writeBuffer.isEmpty()) {
            isCommitted = true
            return
        }

        synchronized(manager.commitLock) {
            val modifiedKeys = writeBuffer.keys
            // 1. Validate Snapshot Isolation (First-Committer-Wins)
            manager.validateNoConflicts(snapshotTimestamp, modifiedKeys)

            // 2. Atomically persist write set directly without coroutine dispatch overhead
            db.engine.writeBatchDirect(writeBuffer.values)

            // 3. Record commit timestamp
            manager.recordCommit(modifiedKeys)
            db.invalidateCachedDocuments(modifiedKeys)
            isCommitted = true
        }
    }

    /**
     * Aborts the transaction and discards all pending writes.
     */
    fun rollback() {
        checkActive()
        writeBuffer.clear()
        readCache.clear()
        isAborted = true
    }

    private fun checkActive() {
        check(!isCommitted) { "Transaction has already been committed." }
        check(!isAborted) { "Transaction has been aborted." }
    }
}
