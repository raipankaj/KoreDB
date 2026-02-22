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
import com.pankaj.koredb.core.VectorSerializer
import com.pankaj.koredb.foundation.MemTable
import com.pankaj.koredb.foundation.SSTable
import com.pankaj.koredb.foundation.SSTableReader
import com.pankaj.koredb.log.WriteAheadLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.RandomAccessFile
import java.util.concurrent.atomic.AtomicInteger

/**
 * The core Log-Structured Merge-tree (LSM) database engine.
 *
 * KoreDB coordinates the flow of data between the in-memory [MemTable], the 
 * durability-focused [WriteAheadLog], and the disk-resident [SSTable] segments.
 *
 * Key Design Principles:
 * 1. **Write Path:** Data is appended to the WAL and updated in the MemTable (O(1)).
 * 2. **Read Path:** Multi-tiered lookup starting from MemTable, then traversing 
 *    SSTables from newest to oldest.
 * 3. **Durability:** WAL ensures zero data loss on crashes.
 * 4. **Scalability:** Periodic compaction prevents disk fragmentation and 
 *    maintains read performance.
 *
 * @param directory The directory where all database files (SSTables, WAL) are stored.
 */
class KoreDB(private val directory: File) {

    private var memTable = MemTable()
    private val sstFileCounter = AtomicInteger(0)
    private val MEMTABLE_FLUSH_THRESHOLD_BYTES = 1 * 1024 * 1024

    private val walFile: File
    private var wal: WriteAheadLog
    private val sstReaders = mutableListOf<SSTableReader>()

    init {
        // Ensure directory exists before defining file paths
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw java.io.IOException("Unable to create database directory: ${directory.absolutePath}")
            }
        }

        walFile = File(directory, "kore.wal")

        // Load existing SSTable segments into memory-mapped readers
        val existingFiles = directory.listFiles { _, name -> name.endsWith(".sst") }
            ?.sortedBy { it.name }

        existingFiles?.forEach { file ->
            try {
                sstReaders.add(SSTableReader(file))
                sstFileCounter.incrementAndGet()
            } catch (e: Exception) {
                // Silently skip corrupt segments in production; could be logged to a monitoring service
            }
        }

        // Restore un-flushed data from the Write-Ahead Log
        restoreFromWal()

        // Initialize WAL for incoming writes
        wal = WriteAheadLog(walFile)
    }

    /**
     * Reads the existing WAL file and replays data into the MemTable.
     * This ensures data isn't lost if the app was killed before a flush occurred.
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
            // If WAL is corrupt, we stop recovery at the point of corruption 
            // and proceed with available data.
        }
    }

    /**
     * Persists a batch of key-value pairs atomically to the WAL and MemTable.
     *
     * @param batch A list of key-value byte array pairs.
     * @param urgent If true, forces an immediate hardware flush to disk.
     */
    suspend fun writeBatchRaw(batch: List<Pair<ByteArray, ByteArray>>,
                              urgent: Boolean = false) = withContext(Dispatchers.IO) {
        for (pair in batch) {
            val keyBytes = pair.first
            val valueBytes = pair.second

            wal.append(keyBytes, valueBytes)
            memTable.put(keyBytes, valueBytes)
        }

        if (urgent) {
            wal.flush()
        }

        if (memTable.sizeInBytes() >= MEMTABLE_FLUSH_THRESHOLD_BYTES) {
            flushMemTable()
        }
    }

    /**
     * Convenience method to insert a single raw record.
     */
    suspend fun putRaw(key: ByteArray, value: ByteArray) {
        writeBatchRaw(listOf(Pair(key, value)))
    }

    /**
     * Marks a record for deletion by writing a tombstone.
     */
    suspend fun deleteRaw(key: ByteArray) {
        writeBatchRaw(listOf(Pair(key, TOMBSTONE)))
    }

    /**
     * Retrieves the raw byte value for a given key using the multi-tiered lookup algorithm.
     *
     * @param key The key to look up.
     * @return The value associated with the key, or null if it doesn't exist or was deleted.
     */
    fun getRaw(key: ByteArray): ByteArray? {
        // Tier 1: Search RAM (Fastest)
        val ramResult = memTable.get(key)
        if (ramResult != null) {
            return if (ramResult.isEmpty()) null else ramResult
        }

        // Tier 2: Search Disk Segments (Newest to Oldest)
        for (i in sstReaders.indices.reversed()) {
            val diskResult = sstReaders[i].find(key)
            if (diskResult != null) {
                return if (diskResult.isEmpty()) null else diskResult
            }
        }
        return null
    }

    private suspend fun flushMemTable() = withContext(Dispatchers.IO) {
        val sstFile = File(directory, "segment_${sstFileCounter.getAndIncrement()}.sst")
        SSTable.writeFromMemTable(memTable, sstFile)

        memTable.clear()
        wal.close()
        walFile.delete()

        wal = WriteAheadLog(walFile)

        val newReader = SSTableReader(sstFile)
        sstReaders.add(newReader)

        if (sstReaders.size >= COMPACTION_THRESHOLD) {
            performCompaction()
        }
    }

    private fun performCompaction() {
        val compactedFile = File(directory, "compacted_${System.currentTimeMillis()}.sst")

        // Merge all current segments into a single deduplicated file
        Compactor.compact(sstReaders, compactedFile)

        // Atomic switch: drop old readers and delete old files
        sstReaders.forEach { it.file.delete() }
        sstReaders.clear()
        
        // Load the new compacted segment
        sstReaders.add(SSTableReader(compactedFile))
    }

    /**
     * Convenience method to retrieve a UTF-8 string value by key.
     */
    fun get(key: String): String? {
        val keyBytes = key.toByteArray(Charsets.UTF_8)

        // Tier 1: Search RAM (MemTable) -> Fastest
        val ramResult = memTable.get(keyBytes)
        if (ramResult != null) return String(ramResult, Charsets.UTF_8)

        // Tier 2: Search Disk (SSTables) from Newest to Oldest
        // We go backwards because if a key was updated, the newest value is in the latest file!
        for (i in sstReaders.indices.reversed()) {
            val diskResult = sstReaders[i].find(keyBytes)
            if (diskResult != null) {
                return String(diskResult, Charsets.UTF_8)
            }
        }

        return null // Doesn't exist anywhere
    }

    /**
     * Returns all active values whose keys start with the given prefix.
     */
    fun getByPrefixRaw(prefix: ByteArray): List<ByteArray> {
        val resultMap = mutableMapOf<String, ByteArray>()

        // Scan Disk (Oldest to Newest)
        for (reader in sstReaders) {
            val entries = reader.scanByPrefix(prefix)
            for (entry in entries) {
                val keyStr = String(entry.first, Charsets.UTF_8)
                if (entry.second.isEmpty()) {
                    resultMap.remove(keyStr)
                } else {
                    resultMap[keyStr] = entry.second
                }
            }
        }

        // Scan RAM (Newest overrides disk)
        for (entry in memTable.getSortedEntries()) {
            val keyBytes = entry.key
            if (keyBytes.size >= prefix.size && keyBytes.sliceArray(prefix.indices).contentEquals(prefix)) {
                val keyStr = String(keyBytes, Charsets.UTF_8)
                if (entry.value.isEmpty()) {
                    resultMap.remove(keyStr)
                } else {
                    resultMap[keyStr] = entry.value
                }
            }
        }

        return resultMap.values.toList()
    }

    /**
     * Returns all active keys that start with the given prefix.
     */
    fun getKeysByPrefixRaw(prefix: ByteArray): List<ByteArray> {
        val resultMap = mutableMapOf<String, ByteArray>()

        for (reader in sstReaders) {
            val entries = reader.scanByPrefix(prefix)
            for (entry in entries) {
                val keyStr = String(entry.first, Charsets.UTF_8)
                if (entry.second.isEmpty()) {
                    resultMap.remove(keyStr)
                } else {
                    resultMap[keyStr] = entry.first
                }
            }
        }

        for (entry in memTable.getSortedEntries()) {
            val keyBytes = entry.key
            if (keyBytes.size >= prefix.size && keyBytes.sliceArray(prefix.indices).contentEquals(prefix)) {
                val keyStr = String(keyBytes, Charsets.UTF_8)
                if (entry.value.isEmpty()) {
                    resultMap.remove(keyStr)
                } else {
                    resultMap[keyStr] = keyBytes
                }
            }
        }

        return resultMap.values.toList()
    }

    /**
     * Performs a vector similarity search across all memory and disk tiers.
     *
     * @param prefix The collection prefix to search within.
     * @param query The query vector.
     * @param limit The maximum number of results to return.
     * @return A list of matching key-value pairs sorted by similarity score.
     */
    fun searchVectorsRaw(prefix: ByteArray, query: FloatArray, limit: Int): List<Pair<ByteArray, Float>> {
        val topKHeap = java.util.PriorityQueue<Pair<ByteArray, Float>>(compareBy { it.second })
        val queryMag = VectorMath.getMagnitude(query)

        // 1. Search Disk Segments
        for (reader in sstReaders) {
            val diskResults = reader.findTopVectors(prefix, query, limit)
            for (res in diskResults) {
                if (topKHeap.size < limit) topKHeap.add(res)
                else if (res.second > topKHeap.peek()!!.second) {
                    topKHeap.poll(); topKHeap.add(res)
                }
            }
        }

        // 2. Search RAM (MemTable)
        for (entry in memTable.getSortedEntries()) {
            val keyBytes = entry.key

            var isMatch = keyBytes.size >= prefix.size
            if (isMatch) {
                for (i in prefix.indices) if (keyBytes[i] != prefix[i]) { isMatch = false; break }
            }

            if (isMatch && entry.value.isNotEmpty()) { // Skip tombstones
                val vector = VectorSerializer.fromByteArray(entry.value)
                val score = VectorMath.cosineSimilarity(
                    query, queryMag,
                    java.nio.ByteBuffer.wrap(entry.value), 0
                )
                if (topKHeap.size < limit) topKHeap.add(Pair(keyBytes, score))
                else if (score > topKHeap.peek()!!.second) {
                    topKHeap.poll(); topKHeap.add(Pair(keyBytes, score))
                }
            }
        }

        return topKHeap.toList().sortedByDescending { it.second }
    }

    /**
     * Forces the Write-Ahead Log to sync with physical storage.
     */
    fun flushHardware() {
        wal.flush()
    }

    /**
     * Safely closes the database and releases all file resources.
     */
    fun close() {
        wal.close()
    }

    companion object {
        /**
         * A 0-byte array represents a Deleted Record (Tombstone) in the LSM-tree.
         */
        val TOMBSTONE = ByteArray(0)
        
        private const val COMPACTION_THRESHOLD = 3
    }
}
