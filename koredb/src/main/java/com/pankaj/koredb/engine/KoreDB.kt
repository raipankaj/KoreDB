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

import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.foundation.ByteArrayComparator
import com.pankaj.koredb.foundation.KoreIterator
import com.pankaj.koredb.foundation.MemTable
import com.pankaj.koredb.foundation.MemTableIterator
import com.pankaj.koredb.foundation.SSTable
import com.pankaj.koredb.foundation.SSTableIterator
import com.pankaj.koredb.foundation.SSTableReader
import com.pankaj.koredb.log.WriteAheadLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.File
import java.io.RandomAccessFile
import java.util.PriorityQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * The core engine for KoreDB, implementing a Log-Structured Merge-tree (LSM-tree).
 *
 * KoreDB manages the lifecycle of data through three primary layers:
 * 1. **MemTable**: An in-memory data structure for low-latency writes and recent reads.
 * 2. **Write-Ahead Log (WAL)**: An append-only file ensuring durability for in-memory data.
 * 3. **SSTables (Sorted String Tables)**: Immutable disk-resident files for long-term storage.
 *
 * Design Architecture:
 * - **Writes**: Appended to the WAL and inserted into the MemTable. When the MemTable exceeds
 *   [MEMTABLE_FLUSH_THRESHOLD_BYTES], it is flushed to disk as a new SSTable.
 * - **Reads**: Traverses the hierarchy from newest to oldest (MemTable -> SSTable segments).
 * - **Compaction**: Periodic background merging of SSTables to reduce fragmentation and 
 *   improve read performance.
 *
 * @property directory The root directory where database segments and logs are persisted.
 */
class KoreDB(val directory: File) {

    private var memTable = MemTable()
    private val sstFileCounter = AtomicInteger(0)
    private val MEMTABLE_FLUSH_THRESHOLD_BYTES = 16 * 1024 * 1024

    private val walFile: File
    private lateinit var wal: WriteAheadLog

    private val writeMutex = kotlinx.coroutines.sync.Mutex()

    private val sstReaders = mutableListOf<SSTableReader>()

    @Volatile
    private var isCompacting = false

    init {
        // Ensure the storage directory exists.
        if (!directory.exists()) directory.mkdirs()
        walFile = File(directory, "kore.wal")

        // Initialize state from the MANIFEST file, which tracks active SSTable segments.
        val manifestFile = File(directory, "MANIFEST")
        val activeFiles = if (manifestFile.exists()) {
            manifestFile.readLines().filter { it.isNotBlank() }.map { File(directory, it) }
        } else {
            // Fallback: Discovery via file system scan if MANIFEST is missing.
            directory.listFiles { _, name -> name.endsWith(".sst") }?.toList() ?: emptyList()
        }

        // Determine the next file index based on existing segments.
        val maxIndex = activeFiles
            .mapNotNull {
                it.name.removePrefix("segment_")
                    .removeSuffix(".sst")
                    .toIntOrNull()
            }
            .maxOrNull() ?: -1

        sstFileCounter.set(maxIndex + 1)

        // Load active SSTable segments into memory-mapped readers.
        activeFiles.sortedBy { it.name }.forEach { file ->
            if (file.exists()) {
                try {
                    sstReaders.add(SSTableReader(file))
                } catch (e: Exception) {
                    println("❌ Skipping corrupt file: ${file.name}")
                }
            }
        }

        // Recovery: Replay the Write-Ahead Log to restore data not yet flushed to SSTables.
        WriteAheadLog.replay(walFile) { key, value ->
            memTable.put(key, value)
        }

        // Initialize the active WAL for new incoming writes.
        wal = WriteAheadLog(walFile)
    }

    /**
     * Persists the current list of active SSTable segments to the MANIFEST file.
     * Uses a temporary file and atomic rename to ensure consistency during crashes.
     */
    private fun writeManifest() {
        val tempManifest = File(directory, "MANIFEST.tmp")
        tempManifest.writeText(sstReaders.joinToString("\n") { it.file.name })

        // Force the manifest update to physical storage.
        java.io.RandomAccessFile(tempManifest, "rw").use { raf ->
            raf.channel.force(true)
        }

        // Atomic rename is guaranteed by the OS to be durable.
        val manifest = File(directory, "MANIFEST")
        tempManifest.renameTo(manifest)

        // Sync directory metadata to ensure the rename is persisted.
        fsyncDirectory()
    }

    /**
     * Synchronizes the directory descriptor to ensure file system metadata changes
     * (like renames or creations) survive a power loss.
     */
    private fun fsyncDirectory() {
        try {
            val channel = java.nio.channels.FileChannel.open(
                directory.toPath(),
                java.nio.file.StandardOpenOption.READ
            )
            channel.force(true)
            channel.close()
        } catch (e: Exception) {
            // Logged or ignored depending on OS support for directory syncing.
        }
    }

    /**
     * Replays the Write-Ahead Log to populate the MemTable during initialization.
     */
    private fun restoreFromWal() {
        if (!walFile.exists()) return

        try {
            val raf = RandomAccessFile(walFile, "r")
            while (raf.filePointer < raf.length()) {
                val keySize = raf.readInt()
                val valueSize = raf.readInt()

                val key = ByteArray(keySize)
                raf.readFully(key)

                val value = ByteArray(valueSize)
                raf.readFully(value)

                memTable.put(key, value)
            }
            raf.close()
        } catch (e: Exception) {
            // Stop recovery at the first sign of log corruption.
        }
    }

    /**
     * Writes a batch of entries to the database. 
     * The operation is first logged to the WAL, then applied to the MemTable.
     *
     * Uses [MemTable.putAll] for bulk insertion to minimize atomic counter
     * overhead and maximize throughput for indexed batch operations.
     *
     * @param batch A list of key-value pairs to persist.
     * @param urgent If true, forces an immediate hardware-level sync of the WAL.
     */
    suspend fun writeBatchRaw(batch: List<Pair<ByteArray, ByteArray>>,
                              urgent: Boolean = false) = writeMutex.withLock {

        withContext(Dispatchers.IO) {
            wal.appendBatch(batch)

            // Bulk insert: single atomic size update instead of N individual updates
            memTable.putAll(batch)

            if (urgent) {
                wal.flush()
            }

            // Trigger a flush to disk if the MemTable has grown beyond its capacity.
            if (memTable.sizeInBytes() >= MEMTABLE_FLUSH_THRESHOLD_BYTES) {
                flushMemTableInternal()
            }
        }
    }

    /**
     * Persists a single key-value pair.
     */
    suspend fun putRaw(key: ByteArray, value: ByteArray) {
        writeBatchRaw(listOf(Pair(key, value)))
    }

    /**
     * Deletes a key by writing a tombstone record (an empty byte array).
     */
    suspend fun deleteRaw(key: ByteArray) {
        writeBatchRaw(listOf(Pair(key, TOMBSTONE)))
    }

    /**
     * Retrieves the value for a key by searching the tiered storage hierarchy.
     * 
     * @return The value associated with [key], or null if not found or deleted.
     */
    fun getRaw(key: ByteArray): ByteArray? {
        // Tier 1: MemTable lookup (O(log N))
        val ramResult = memTable.get(key)
        if (ramResult != null) {
            return if (ramResult.isEmpty()) null else ramResult
        }

        // Tier 2: SSTable lookup (Newest to Oldest)
        for (i in sstReaders.indices.reversed()) {
            val reader = sstReaders[i]
            
            // Fast path: skip segments where the key is definitively out of bounds
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, key) < 0) continue
            if (reader.minKey != null && ByteArrayComparator.compare(reader.minKey!!, key) > 0) continue

            val diskResult = reader.find(key)
            if (diskResult != null) {
                return if (diskResult.isEmpty()) null else diskResult
            }
        }
        return null
    }

    /**
     * Flushes the current MemTable to a new SSTable segment on disk.
     * This process involves rotating the WAL and updating the MANIFEST.
     *
     * (Internal visibility for testing)
     */
    internal suspend fun flushMemTableInternal() = withContext(Dispatchers.IO) {
        val sstFile = File(directory, "segment_${sstFileCounter.getAndIncrement()}.sst")

        // Write the sorted MemTable to disk and register the new reader.
        SSTable.writeFromMemTable(memTable, sstFile)
        sstReaders.add(SSTableReader(sstFile))

        // Commit the new segment list to the MANIFEST.
        writeManifest()

        // Rotate the WAL: Close the current log, rename it, and initialize a new one.
        if (this@KoreDB::wal.isInitialized) {
            wal.close()
        }

        val oldWalFile = File(directory, "kore.wal.old")
        if (walFile.exists()) {
            walFile.renameTo(oldWalFile)
            fsyncDirectory()
        }

        wal = WriteAheadLog(walFile)
        fsyncDirectory()

        // Clean up the old WAL now that data is safely in the SSTable.
        if (oldWalFile.exists()) {
            oldWalFile.delete()
        }

        memTable.clear()

        // Check if the number of segments warrants a compaction run.
        if (sstReaders.size >= COMPACTION_THRESHOLD) {
            if (!isCompacting) {
                isCompacting = true
                try {
                    performCompaction()
                } finally {
                    isCompacting = false
                }
            }
        }
    }

    /**
     * Merges multiple SSTable segments into a single, optimized segment.
     * This reduces disk usage by removing stale versions and tombstones.
     *
     * (Internal visibility for testing)
     */
    internal fun performCompaction() {
        println("🚧 STARTING COMPACTION...")
        val compactedFile = File(directory, "compacted_${System.currentTimeMillis()}.sst")

        // Pass a "Truth Oracle" to the compactor so it can drop stale index entries.
        Compactor.compact(sstReaders, compactedFile) { rptrKey ->
            // Use getRaw to check the most recent value for a reverse pointer.
            getRaw(rptrKey)
        }

        // Ensure the compacted file is fully written to disk.
        java.io.RandomAccessFile(compactedFile, "rw").use { raf ->
            raf.channel.force(true)
        }

        val newReader = SSTableReader(compactedFile)
        val oldReaders = sstReaders.toList()

        // Replace old readers with the new compacted reader.
        sstReaders.clear()
        sstReaders.add(newReader)

        writeManifest()

        // Delete the redundant source files.
        oldReaders.forEach { it.file.delete() }
        println("♻️ COMPACTION COMPLETE.")
    }

    /**
     * Convenience method to retrieve a UTF-8 string value.
     */
    fun get(key: String): String? {
        val keyBytes = key.toByteArray(Charsets.UTF_8)
        val result = getRaw(keyBytes)
        return result?.let { String(it, Charsets.UTF_8) }
    }

    /**
     * Returns all values whose keys fall within the range [startKey, endKey).
     */
    fun getRangeRaw(startKey: ByteArray, endKey: ByteArray): List<ByteArray> {
        val iterators = PriorityQueue<KoreIterator>()

        // 1. Initialize SSTable iterators
        for (i in sstReaders.indices) {
            val reader = sstReaders[i]
            
            // Fast path: skip segments that do not overlap with the range
            // maxKey < startKey -> entirely before range
            // minKey >= endKey -> entirely after range (endKey is exclusive)
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, startKey) < 0) continue
            if (reader.minKey != null && ByteArrayComparator.compare(reader.minKey!!, endKey) >= 0) continue

            val offset = reader.findBlockStartOffset(startKey)
            val it = SSTableIterator(reader, priority = i, startOffset = offset, startKey = startKey, endKey = endKey)
            if (it.currentKey != null) iterators.add(it)
        }

        // 2. Initialize MemTable iterator (highest priority)
        val memIt = MemTableIterator(memTable.getTailEntries(startKey).iterator(), sstReaders.size, endKey)
        if (memIt.currentKey != null) iterators.add(memIt)

        val results = mutableListOf<ByteArray>()
        var lastKey: ByteArray? = null

        // 3. K-Way Merge Loop
        while (iterators.isNotEmpty()) {
            val top = iterators.poll()!!
            val key = top.currentKey!!

            // Check if we've already processed this key (from a newer segment)
            if (lastKey == null || ByteArrayComparator.compare(key, lastKey) != 0) {
                val value = top.value() ?: KoreDB.TOMBSTONE
                if (value.isNotEmpty()) { // Skip tombstones
                    results.add(value)
                }
                lastKey = key
            }

            if (top.advance()) {
                iterators.add(top)
            }
        }

        return results
    }

    /**
     * Returns all key-value pairs whose keys fall within the range [startKey, endKey).
     */
    fun getRangeWithKeysRaw(startKey: ByteArray, endKey: ByteArray): List<Pair<ByteArray, ByteArray>> {
        val iterators = PriorityQueue<KoreIterator>()

        for (i in sstReaders.indices) {
            val reader = sstReaders[i]
            
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, startKey) < 0) continue
            if (reader.minKey != null && ByteArrayComparator.compare(reader.minKey!!, endKey) >= 0) continue

            val offset = reader.findBlockStartOffset(startKey)
            val it = SSTableIterator(reader, priority = i, startOffset = offset, startKey = startKey, endKey = endKey)
            if (it.currentKey != null) iterators.add(it)
        }

        val memIt = MemTableIterator(memTable.getTailEntries(startKey).iterator(), sstReaders.size, endKey)
        if (memIt.currentKey != null) iterators.add(memIt)

        val results = mutableListOf<Pair<ByteArray, ByteArray>>()
        var lastKey: ByteArray? = null

        while (iterators.isNotEmpty()) {
            val top = iterators.poll()!!
            val key = top.currentKey!!

            if (lastKey == null || ByteArrayComparator.compare(key, lastKey) != 0) {
                val value = top.value() ?: KoreDB.TOMBSTONE
                if (value.isNotEmpty()) {
                    results.add(Pair(key, value))
                }
                lastKey = key
            }

            if (top.advance()) {
                iterators.add(top)
            }
        }

        return results
    }

    /**
     * Returns all values whose keys match the specified prefix.
     */
    fun getByPrefixRaw(prefix: ByteArray): List<ByteArray> {
        val endKey = prefix.copyOf()
        var i = endKey.size - 1
        var carry = true
        while (i >= 0 && carry) {
            val v = (endKey[i].toInt() and 0xFF) + 1
            endKey[i] = v.toByte()
            carry = v > 255
            i--
        }
        
        // If carry is true, it means prefix was something like [0xFF, 0xFF], 
        // in which case endKey should be null (infinity).
        val actualEndKey = if (carry) null else endKey
        
        val iterators = PriorityQueue<KoreIterator>()

        // 1. Initialize SSTable iterators
        for (i in sstReaders.indices) {
            val reader = sstReaders[i]
            
            // Fast path: skip segments that do not overlap with the prefix
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, prefix) < 0) continue
            if (reader.minKey != null && actualEndKey != null && ByteArrayComparator.compare(reader.minKey!!, actualEndKey) >= 0) continue

            // Prefix Bloom Filter fast rejection
            if (!reader.mightContain(prefix)) continue

            val offset = reader.findBlockStartOffset(prefix)
            val it = SSTableIterator(reader, priority = i, startOffset = offset, startKey = prefix, endKey = actualEndKey)
            if (it.currentKey != null && it.currentKey!!.startsWith(prefix)) {
                iterators.add(it)
            }
        }

        // 2. Initialize MemTable iterator
        val memIt = MemTableIterator(memTable.getTailEntries(prefix).iterator(), sstReaders.size, actualEndKey)
        if (memIt.currentKey != null && memIt.currentKey!!.startsWith(prefix)) {
            iterators.add(memIt)
        }

        val results = mutableListOf<ByteArray>()
        var lastKey: ByteArray? = null

        while (iterators.isNotEmpty()) {
            val top = iterators.poll()!!
            val key = top.currentKey!!

            if (!key.startsWith(prefix)) {
                continue // Should be handled by endKey, but safety first
            }

            if (lastKey == null || ByteArrayComparator.compare(key, lastKey) != 0) {
                val value = top.value() ?: KoreDB.TOMBSTONE
                if (value.isNotEmpty()) {
                    results.add(value)
                }
                lastKey = key
            }

            if (top.advance() && top.currentKey!!.startsWith(prefix)) {
                iterators.add(top)
            }
        }

        return results
    }

    /**
     * Returns all key-value pairs whose keys match the specified prefix.
     */
    fun getByPrefixWithKeysRaw(prefix: ByteArray): List<Pair<ByteArray, ByteArray>> {
        val endKey = prefix.copyOf()
        var i = endKey.size - 1
        var carry = true
        while (i >= 0 && carry) {
            val v = (endKey[i].toInt() and 0xFF) + 1
            endKey[i] = v.toByte()
            carry = v > 255
            i--
        }
        val actualEndKey = if (carry) null else endKey

        val iterators = PriorityQueue<KoreIterator>()

        for (i in sstReaders.indices) {
            val reader = sstReaders[i]
            
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, prefix) < 0) continue
            if (reader.minKey != null && actualEndKey != null && ByteArrayComparator.compare(reader.minKey!!, actualEndKey) >= 0) continue

            if (!reader.mightContain(prefix)) continue

            val offset = reader.findBlockStartOffset(prefix)
            val it = SSTableIterator(reader, priority = i, startOffset = offset, startKey = prefix, endKey = actualEndKey)
            if (it.currentKey != null && it.currentKey!!.startsWith(prefix)) {
                iterators.add(it)
            }
        }

        val memIt = MemTableIterator(memTable.getTailEntries(prefix).iterator(), sstReaders.size, actualEndKey)
        if (memIt.currentKey != null && memIt.currentKey!!.startsWith(prefix)) {
            iterators.add(memIt)
        }

        val results = mutableListOf<Pair<ByteArray, ByteArray>>()
        var lastKey: ByteArray? = null

        while (iterators.isNotEmpty()) {
            val top = iterators.poll()!!
            val key = top.currentKey!!

            if (!key.startsWith(prefix)) {
                continue
            }

            if (lastKey == null || ByteArrayComparator.compare(key, lastKey) != 0) {
                val value = top.value() ?: KoreDB.TOMBSTONE
                if (value.isNotEmpty()) {
                    results.add(Pair(key, value))
                }
                lastKey = key
            }

            if (top.advance() && top.currentKey!!.startsWith(prefix)) {
                iterators.add(top)
            }
        }

        return results
    }

    private fun ByteArray.startsWith(prefix: ByteArray): Boolean {
        if (size < prefix.size) return false
        for (i in prefix.indices) {
            if (this[i] != prefix[i]) return false
        }
        return true
    }

    /**
     * Returns all keys that match the specified prefix.
     */
    fun getKeysByPrefixRaw(prefix: ByteArray): List<ByteArray> {
        val endKey = prefix.copyOf()
        var i = endKey.size - 1
        var carry = true
        while (i >= 0 && carry) {
            val v = (endKey[i].toInt() and 0xFF) + 1
            endKey[i] = v.toByte()
            carry = v > 255
            i--
        }
        val actualEndKey = if (carry) null else endKey

        val iterators = PriorityQueue<KoreIterator>()

        for (i in sstReaders.indices) {
            val reader = sstReaders[i]
            
            // Fast path: skip segments that do not overlap with the prefix
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, prefix) < 0) continue
            if (reader.minKey != null && actualEndKey != null && ByteArrayComparator.compare(reader.minKey!!, actualEndKey) >= 0) continue

            // Prefix Bloom Filter fast rejection
            if (!reader.mightContain(prefix)) continue

            val offset = reader.findBlockStartOffset(prefix)
            val it = SSTableIterator(reader, priority = i, startOffset = offset, startKey = prefix, endKey = actualEndKey)
            if (it.currentKey != null && it.currentKey!!.startsWith(prefix)) {
                iterators.add(it)
            }
        }

        val memIt = MemTableIterator(memTable.getTailEntries(prefix).iterator(), sstReaders.size, actualEndKey)
        if (memIt.currentKey != null && memIt.currentKey!!.startsWith(prefix)) {
            iterators.add(memIt)
        }

        val results = mutableListOf<ByteArray>()
        var lastKey: ByteArray? = null

        while (iterators.isNotEmpty()) {
            val top = iterators.poll()!!
            val key = top.currentKey!!

            if (!key.startsWith(prefix)) continue

            if (lastKey == null || ByteArrayComparator.compare(key, lastKey) != 0) {
                val value = top.value() ?: KoreDB.TOMBSTONE
                if (value.isNotEmpty()) {
                    results.add(key)
                }
                lastKey = key
            }

            if (top.advance() && top.currentKey!!.startsWith(prefix)) {
                iterators.add(top)
            }
        }

        return results
    }

    /**
     * Performs a vector similarity search across MemTable and SSTable tiers.
     *
     * @param prefix The collection prefix to scope the search.
     * @param query The query vector for similarity matching.
     * @param limit Maximum number of results to return.
     * @return A list of matching key-score pairs, sorted by similarity descending.
     */
    suspend fun searchVectorsRaw(prefix: ByteArray, query: FloatArray, limit: Int): List<Pair<ByteArray, Float>> = coroutineScope {
        val topKHeap = java.util.PriorityQueue<Pair<ByteArray, Float>>(compareBy { it.second })
        val queryMag = VectorMath.getMagnitude(query)

        // 🚀 PARALLEL SSTABLE SCAN
        val sstResults = sstReaders.map { reader ->
            async(Dispatchers.Default) {
                reader.findTopVectors(prefix, query, limit)
            }
        }.awaitAll()

        // Aggregate results from disk segments.
        for (diskResults in sstResults) {
            for (res in diskResults) {
                if (topKHeap.size < limit) topKHeap.add(res)
                else if (res.second > topKHeap.peek()!!.second) {
                    topKHeap.poll(); topKHeap.add(res)
                }
            }
        }

        // 🏎️ OPTIMIZED MEMTABLE SCAN
        for (entry in memTable.getTailEntries(prefix)) {
            val keyBytes = entry.key
            
            // Inline Prefix Check
            if (keyBytes.size < prefix.size) break
            var match = true
            for (i in prefix.indices) {
                if (keyBytes[i] != prefix[i]) {
                    match = false; break
                }
            }
            if (!match) break

            if (entry.value.isNotEmpty()) { // Skip tombstones.
                val valBytes = entry.value
                val buf = java.nio.ByteBuffer.wrap(valBytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                val storedMag = buf.getFloat()
                val vectorLength = (valBytes.size - 4) / 4

                val dot = VectorMath.dotProduct(query, buf, 4, vectorLength)
                val score = if (queryMag == 0f || storedMag == 0f) 0f else dot / (queryMag * storedMag)

                if (score > -1.5f) {
                    if (topKHeap.size < limit) topKHeap.add(Pair(keyBytes, score))
                    else if (score > topKHeap.peek()!!.second) {
                        topKHeap.poll(); topKHeap.add(Pair(keyBytes, score))
                    }
                }
            }
        }

        topKHeap.toList().sortedByDescending { it.second }
    }

    /**
     * Synchronizes the Write-Ahead Log to persistent storage.
     */
    fun flushHardware() {
        wal.flush()
    }

    /**
     * Deletes all data and resets the database state. Primarily used for testing.
     */
    fun nuke() {
        wal.close()
        sstReaders.clear()

        directory.listFiles()?.forEach { it.delete() }

        memTable.clear()
        sstFileCounter.set(0)

        val walFile = File(directory, "kore.wal")
        wal = WriteAheadLog(walFile)
    }

    /**
     * Releases all resources and closes active file handles.
     */
    fun close() {
        wal.close()
    }

    companion object {
        /**
         * Represents a deleted entry in the LSM-tree.
         */
        val TOMBSTONE = ByteArray(0)
        
        /**
         * The number of SSTable segments that triggers a compaction run.
         */
        private const val COMPACTION_THRESHOLD = 3
    }
}
