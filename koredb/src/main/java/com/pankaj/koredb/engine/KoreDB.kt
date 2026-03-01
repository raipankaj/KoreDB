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
import com.pankaj.koredb.foundation.MemTable
import com.pankaj.koredb.foundation.SSTable
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
class KoreDB(private val directory: File) {

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
     * @param batch A list of key-value pairs to persist.
     * @param urgent If true, forces an immediate hardware-level sync of the WAL.
     */
    suspend fun writeBatchRaw(batch: List<Pair<ByteArray, ByteArray>>,
                              urgent: Boolean = false) = writeMutex.withLock {

        withContext(Dispatchers.IO) {
            wal.appendBatch(batch)

            for (pair in batch) {
                memTable.put(pair.first, pair.second)
            }

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
            val diskResult = sstReaders[i].find(key)
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

        Compactor.compact(sstReaders, compactedFile)

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
     * Returns all values whose keys match the specified prefix.
     */
    fun getByPrefixRaw(prefix: ByteArray): List<ByteArray> {
        val resultMap = mutableMapOf<String, ByteArray>()

        // Scan disk segments.
        for (reader in sstReaders) {
            reader.scanByPrefix(prefix) { keyBytes, valueBytes ->
                val keyStr = String(keyBytes, Charsets.UTF_8)
                if (valueBytes.isEmpty()) resultMap.remove(keyStr)
                else resultMap[keyStr] = valueBytes
            }
        }

        // Scan the MemTable starting from the prefix.
        for (entry in memTable.getTailEntries(prefix)) {
            val keyBytes = entry.key

            var isMatch = keyBytes.size >= prefix.size
            if (isMatch) {
                for (i in prefix.indices) {
                    if (keyBytes[i] != prefix[i]) { isMatch = false; break }
                }
            }

            // Alphabetical ordering allows early termination if the prefix is passed.
            if (!isMatch) break

            val keyStr = String(keyBytes, Charsets.UTF_8)
            if (entry.value.isEmpty()) resultMap.remove(keyStr)
            else resultMap[keyStr] = entry.value
        }

        return resultMap.values.toList()
    }

    /**
     * Returns all keys that match the specified prefix.
     */
    fun getKeysByPrefixRaw(prefix: ByteArray): List<ByteArray> {
        val resultMap = mutableMapOf<String, ByteArray>()

        for (reader in sstReaders) {
            reader.scanByPrefix(prefix) { keyBytes, valueBytes ->
                val keyStr = String(keyBytes, Charsets.UTF_8)
                if (valueBytes.isEmpty()) resultMap.remove(keyStr)
                else resultMap[keyStr] = keyBytes
            }
        }

        for (entry in memTable.getTailEntries(prefix)) {
            val keyBytes = entry.key

            var isMatch = keyBytes.size >= prefix.size
            if (isMatch) {
                for (i in prefix.indices) {
                    if (keyBytes[i] != prefix[i]) { isMatch = false; break }
                }
            }

            if (!isMatch) break

            val keyStr = String(keyBytes, Charsets.UTF_8)
            if (entry.value.isEmpty()) resultMap.remove(keyStr)
            else resultMap[keyStr] = keyBytes
        }

        return resultMap.values.toList()
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
