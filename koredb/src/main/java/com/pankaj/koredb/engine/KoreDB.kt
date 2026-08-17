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
import com.pankaj.koredb.log.KoreLogger
import com.pankaj.koredb.log.WriteAheadLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import java.io.File
import java.io.RandomAccessFile
import java.util.PriorityQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32

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
class KoreDB(
    val directory: File,
    val crypto: com.pankaj.koredb.crypto.KoreCrypto? = null,
    val compressionCodec: com.pankaj.koredb.compression.CompressionCodec = com.pankaj.koredb.compression.NoOpCompressionCodec
) {

    private val logger = KoreLogger.getLogger("KoreDB")

    @Volatile
    private var memTable = MemTable()

    @Volatile
    private var immutableMemTable: MemTable? = null
    
    private val sstFileCounter = AtomicInteger(0)
    private val MEMTABLE_FLUSH_THRESHOLD_BYTES = 16 * 1024 * 1024

    private val walFile: File
    @Volatile
    private lateinit var wal: WriteAheadLog

    // Metrics counters
    private val readCounter = AtomicLong(0)
    private val writeCounter = AtomicLong(0)
    private val compactionCounter = AtomicLong(0)

    private class WriteRequest(
        val batch: List<Pair<ByteArray, ByteArray>>,
        val urgent: Boolean,
        val deferred: kotlinx.coroutines.CompletableDeferred<Unit> = kotlinx.coroutines.CompletableDeferred()
    )

    private val writeQueue = java.util.concurrent.ConcurrentLinkedQueue<WriteRequest>()
    private val isLeaderActive = java.util.concurrent.atomic.AtomicBoolean(false)
    private val flushMutex = kotlinx.coroutines.sync.Mutex()
    private val backgroundScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val sstReaders = mutableListOf<SSTableReader>()

    @Volatile
    internal var isCompacting = false

    init {
        walFile = File(directory, "kore.wal")
        loadStorageState()
    }

    /**
     * Loads or reloads the active SSTable readers and replays Write-Ahead Logs.
     */
    @Synchronized
    private fun loadStorageState() {
        synchronized(sstReaders) {
            sstReaders.clear()
        }
        memTable.clear()
        immutableMemTable = null

        // Ensure the storage directory exists.
        if (!directory.exists()) directory.mkdirs()

        // Initialize state from the MANIFEST file, which tracks active SSTable segments.
        val manifestFile = File(directory, "MANIFEST")
        val levelsMap = mutableMapOf<String, Int>()
        val activeFiles = if (manifestFile.exists()) {
            manifestFile.readLines().filter { it.isNotBlank() }.map { line ->
                val parts = line.split(":")
                val filename = parts[0]
                val level = parts.getOrNull(1)?.toIntOrNull() ?: 0
                levelsMap[filename] = level
                File(directory, filename)
            }
        } else {
            // Fallback: Discovery via file system scan if MANIFEST is missing.
            directory.listFiles { _, name -> name.endsWith(".sst") }?.toList() ?: emptyList()
        }

        // Determine the next file index based on existing segments.
        val maxIndex = activeFiles
            .mapNotNull {
                val name = it.name
                if (name.startsWith("segment_")) {
                    name.removePrefix("segment_")
                        .removeSuffix(".sst")
                        .toIntOrNull()
                } else {
                    null
                }
            }
            .maxOrNull() ?: -1

        sstFileCounter.set(maxIndex + 1)

        // Load active SSTable segments into memory-mapped readers.
        activeFiles.sortedBy { it.name }.forEach { file ->
            if (file.exists()) {
                try {
                    val reader = SSTableReader(file)
                    reader.level = levelsMap[file.name] ?: 0
                    sstReaders.add(reader)
                } catch (e: Exception) {
                    logger.warn("Skipping corrupt file: ${file.name}", e)
                }
            }
        }

        // Recovery: Replay the old Write-Ahead Log if a crash occurred during a background flush
        val oldWalFile = File(directory, "kore.wal.old")
        if (oldWalFile.exists()) {
            try {
                WriteAheadLog.replay(oldWalFile) { key, value ->
                    memTable.put(key, value)
                }
                oldWalFile.delete()
            } catch (e: Exception) {
                logger.warn("Failed to replay or delete old WAL: ${e.message}", e)
            }
        }

        // Recovery: Replay the active Write-Ahead Log to restore data not yet flushed to SSTables.
        if (walFile.exists()) {
            WriteAheadLog.replay(walFile) { key, value ->
                memTable.put(key, value)
            }
        }

        // Initialize the active WAL for new incoming writes.
        wal = WriteAheadLog(walFile)
    }

    /**
     * Persists the current list of active SSTable segments to the MANIFEST file.
     * Uses a temporary file and atomic rename to ensure consistency during crashes.
     */
    @Synchronized
    private fun writeManifest() {
        val tempManifest = File(directory, "MANIFEST.tmp")
        val readersSnapshot = synchronized(sstReaders) { sstReaders.toList() }
        tempManifest.writeText(readersSnapshot.joinToString("\n") { "${it.file.name}:${it.level}" })

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
     * Writes a batch of entries to the database. 
     * The operation is first logged to the WAL, then applied to the MemTable.
     *
     * Uses [MemTable.putAll] for bulk insertion to minimize atomic counter
     * overhead and maximize throughput for indexed batch operations.
     *
     * @param batch A list of key-value pairs to persist.
     * @param urgent If true, forces an immediate hardware-level sync of the WAL.
     */
    suspend fun writeBatchRaw(batch: List<Pair<ByteArray, ByteArray>>, urgent: Boolean = false) {
        if (batch.isEmpty()) return

        val finalBatch = if (crypto != null) {
            batch.map { (key, value) ->
                if (value.isNotEmpty()) {
                    Pair(key, crypto.encrypt(value, aad = key))
                } else {
                    Pair(key, value)
                }
            }
        } else {
            batch
        }

        val request = WriteRequest(finalBatch, urgent)
        writeQueue.add(request)
        
        if (isLeaderActive.compareAndSet(false, true)) {
            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
                try {
                    processGroupCommits()
                } finally {
                    isLeaderActive.set(false)
                    if (!writeQueue.isEmpty() && isLeaderActive.compareAndSet(false, true)) {
                        backgroundScope.launch {
                            try {
                                processGroupCommits()
                            } finally {
                                isLeaderActive.set(false)
                            }
                        }
                    }
                }
            }
        }
        
        request.deferred.await()
    }

    private suspend fun processGroupCommits() {
        while (true) {
            val requests = mutableListOf<WriteRequest>()
            var req = writeQueue.poll()
            if (req == null) break
            
            while (req != null) {
                requests.add(req)
                if (requests.size >= 1000) break
                req = writeQueue.poll()
            }
            
            if (requests.isNotEmpty()) {
                val mergedBatch = mutableListOf<Pair<ByteArray, ByteArray>>()
                var anyUrgent = false
                for (r in requests) {
                    mergedBatch.addAll(r.batch)
                    if (r.urgent) anyUrgent = true
                }
                
                try {
                    withContext(Dispatchers.IO) {
                        while (immutableMemTable != null && memTable.sizeInBytes() >= MEMTABLE_FLUSH_THRESHOLD_BYTES) {
                            Thread.sleep(10)
                        }
                        
                        wal.appendBatch(mergedBatch)
                        memTable.putAll(mergedBatch)
                        writeCounter.addAndGet(mergedBatch.size.toLong())
                        
                        if (anyUrgent) {
                            wal.flush()
                        }
                        
                        if (memTable.sizeInBytes() >= MEMTABLE_FLUSH_THRESHOLD_BYTES) {
                            flushMutex.withLock {
                                if (memTable.sizeInBytes() >= MEMTABLE_FLUSH_THRESHOLD_BYTES) {
                                    triggerBackgroundFlush()
                                }
                            }
                        }
                    }
                    
                    for (r in requests) {
                        r.deferred.complete(Unit)
                    }
                } catch (e: Exception) {
                    for (r in requests) {
                        r.deferred.completeExceptionally(e)
                    }
                }
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
        readCounter.incrementAndGet()
        val rawResult = getInternalRaw(key) ?: return null
        return if (crypto != null && rawResult.isNotEmpty()) {
            crypto.decrypt(rawResult, aad = key)
        } else {
            rawResult
        }
    }

    private fun getInternalRaw(key: ByteArray): ByteArray? {
        // Tier 1: MemTable lookup (O(log N))
        val ramResult = memTable.get(key)
        if (ramResult != null) {
            return if (ramResult.isEmpty()) null else ramResult
        }

        // Tier 1.5 & Tier 2: Atomic retrieval of immutable MemTable and SSTable snapshot
        val imm: MemTable?
        val readersSnapshot: List<SSTableReader>
        synchronized(sstReaders) {
            imm = immutableMemTable
            readersSnapshot = sstReaders.toList()
        }

        if (imm != null) {
            val immResult = imm.get(key)
            if (immResult != null) {
                return if (immResult.isEmpty()) null else immResult
            }
        }

        // Tier 2: Leveled SSTable lookup
        // 1. Search Level 0 (newest to oldest)
        val l0Readers = readersSnapshot.filter { it.level == 0 }
        for (i in l0Readers.indices.reversed()) {
            val reader = l0Readers[i]
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, key) < 0) continue
            if (reader.minKey != null && ByteArrayComparator.compare(reader.minKey!!, key) > 0) continue

            val diskResult = reader.find(key)
            if (diskResult != null) {
                return if (diskResult.isEmpty()) null else diskResult
            }
        }

        // 2. Search levels 1, 2, ...
        val maxLevel = readersSnapshot.map { it.level }.maxOrNull() ?: 0
        for (lvl in 1..maxLevel) {
            val lvlReaders = readersSnapshot.filter { it.level == lvl }
            val reader = lvlReaders.find { r ->
                val min = r.minKey
                val max = r.maxKey
                min != null && max != null &&
                        ByteArrayComparator.compare(min, key) <= 0 &&
                        ByteArrayComparator.compare(max, key) >= 0
            }
            if (reader != null) {
                val diskResult = reader.find(key)
                if (diskResult != null) {
                    return if (diskResult.isEmpty()) null else diskResult
                }
            }
        }
        return null
    }

    internal suspend fun flushMemTableInternal() = withContext(Dispatchers.IO) {
        flushMutex.withLock {
            val oldMemTable = memTable
            memTable = MemTable()
            immutableMemTable = oldMemTable

            wal.close()

            val oldWalFile = File(directory, "kore.wal.old")
            if (walFile.exists()) {
                walFile.renameTo(oldWalFile)
                fsyncDirectory()
            }

            wal = WriteAheadLog(walFile)
            fsyncDirectory()

            try {
                val sstFile = File(directory, "segment_${sstFileCounter.getAndIncrement()}.sst")
                SSTable.writeFromMemTable(oldMemTable, sstFile, compressionCodec)

                val newReader = SSTableReader(sstFile)
                newReader.level = 0
                synchronized(sstReaders) {
                    sstReaders.add(newReader)
                    immutableMemTable = null
                }

                writeManifest()

                if (oldWalFile.exists()) {
                    oldWalFile.delete()
                }
            } catch (e: Exception) {
                logger.error("Synchronous flush failed: ${e.message}", e)
                throw e
            } finally {
                synchronized(sstReaders) {
                    immutableMemTable = null
                }
                checkAndTriggerCompaction()
            }
        }
    }

    /**
     * Initiates a background asynchronous MemTable flush and WAL rotation.
     */
    private fun triggerBackgroundFlush() {
        val oldMemTable = memTable
        memTable = MemTable()
        immutableMemTable = oldMemTable

        wal.close()

        val oldWalFile = File(directory, "kore.wal.old")
        if (walFile.exists()) {
            walFile.renameTo(oldWalFile)
            fsyncDirectory()
        }

        wal = WriteAheadLog(walFile)
        fsyncDirectory()

        backgroundScope.launch {
            try {
                val sstFile = File(directory, "segment_${sstFileCounter.getAndIncrement()}.sst")
                SSTable.writeFromMemTable(oldMemTable, sstFile, compressionCodec)
                
                val newReader = SSTableReader(sstFile)
                newReader.level = 0
                synchronized(sstReaders) {
                    sstReaders.add(newReader)
                    immutableMemTable = null
                }

                writeManifest()

                if (oldWalFile.exists()) {
                    oldWalFile.delete()
                }
            } catch (e: Exception) {
                logger.error("Background flush failed: ${e.message}", e)
            } finally {
                synchronized(sstReaders) {
                    immutableMemTable = null
                }
                checkAndTriggerCompaction()
            }
        }
    }

    /**
     * Performs a leveled compaction. Merges Level 0 files with Level 1 files, 
     * then cascades further compaction as levels exceed their capacity thresholds.
     */
    internal fun performLeveledCompaction() {
        logger.info("STARTING LEVELED COMPACTION...")
        val readersSnapshot = synchronized(sstReaders) { sstReaders.toList() }
        val l0Readers = readersSnapshot.filter { it.level == 0 }
        
        // Merge L0 files and any L1 files.
        val l1Readers = readersSnapshot.filter { it.level == 1 }
        val filesToMerge = l0Readers + l1Readers

        if (filesToMerge.isEmpty()) return

        val compactedFile = File(directory, "compacted_l1_${System.currentTimeMillis()}.sst")
        Compactor.compact(filesToMerge, compactedFile, truthOracle = { rptrKey -> getRaw(rptrKey) }, compressionCodec = compressionCodec)

        // Ensure written to disk
        java.io.RandomAccessFile(compactedFile, "rw").use { raf ->
            raf.channel.force(true)
        }

        val newL1Reader = SSTableReader(compactedFile)
        newL1Reader.level = 1

        synchronized(sstReaders) {
            sstReaders.removeAll(filesToMerge)
            sstReaders.add(newL1Reader)
        }

        writeManifest()
        filesToMerge.forEach { it.file.delete() }
        compactionCounter.incrementAndGet()
        logger.info("L0 -> L1 COMPACTION COMPLETE. File size: ${compactedFile.length() / 1024} KB")

        // Cascade down Level 1 -> Level 2 -> Level 3 if capacity limits are breached
        checkAndCascadeCompaction()
    }

    internal fun performCompaction() {
        performLeveledCompaction()
    }

    private fun checkAndCascadeCompaction() {
        var readersSnapshot = synchronized(sstReaders) { sstReaders.toList() }

        // Level 1 Threshold check (10MB)
        val l1Files = readersSnapshot.filter { it.level == 1 }
        val l1Size = l1Files.sumOf { it.file.length() }
        if (l1Size > 10 * 1024 * 1024) {
            logger.info("L1 size ($l1Size) exceeds 10MB threshold. Cascading L1 -> L2...")
            val l2Files = readersSnapshot.filter { it.level == 2 }
            val filesToMerge = l1Files + l2Files
            val compactedFile = File(directory, "compacted_l2_${System.currentTimeMillis()}.sst")

            Compactor.compact(filesToMerge, compactedFile, truthOracle = { rptrKey -> getRaw(rptrKey) }, compressionCodec = compressionCodec)

            java.io.RandomAccessFile(compactedFile, "rw").use { raf ->
                raf.channel.force(true)
            }

            val newL2Reader = SSTableReader(compactedFile)
            newL2Reader.level = 2

            synchronized(sstReaders) {
                sstReaders.removeAll(filesToMerge)
                sstReaders.add(newL2Reader)
            }

            writeManifest()
            filesToMerge.forEach { it.file.delete() }
            compactionCounter.incrementAndGet()
            logger.info("L1 -> L2 COMPACTION COMPLETE. File size: ${compactedFile.length() / 1024} KB")

            // Re-read snapshot
            readersSnapshot = synchronized(sstReaders) { sstReaders.toList() }
            // Level 2 Threshold check (100MB)
            val l2FilesPost = readersSnapshot.filter { it.level == 2 }
            val l2Size = l2FilesPost.sumOf { it.file.length() }
            if (l2Size > 100 * 1024 * 1024) {
                logger.info("L2 size ($l2Size) exceeds 100MB threshold. Cascading L2 -> L3...")
                val l3Files = readersSnapshot.filter { it.level == 3 }
                val filesToMergeL3 = l2FilesPost + l3Files
                val compactedFileL3 = File(directory, "compacted_l3_${System.currentTimeMillis()}.sst")

                Compactor.compact(filesToMergeL3, compactedFileL3, truthOracle = { rptrKey -> getRaw(rptrKey) }, compressionCodec = compressionCodec)

                java.io.RandomAccessFile(compactedFileL3, "rw").use { raf ->
                    raf.channel.force(true)
                }

                val newL3Reader = SSTableReader(compactedFileL3)
                newL3Reader.level = 3

                synchronized(sstReaders) {
                    sstReaders.removeAll(filesToMergeL3)
                    sstReaders.add(newL3Reader)
                }

                writeManifest()
                filesToMergeL3.forEach { it.file.delete() }
                compactionCounter.incrementAndGet()
                logger.info("L2 -> L3 COMPACTION COMPLETE. File size: ${compactedFileL3.length() / 1024} KB")
            }
        }
    }

    private fun checkAndTriggerCompaction() {
        val l0Count = synchronized(sstReaders) { sstReaders.count { it.level == 0 } }
        if (l0Count >= 4) { // Trigger Leveled Compaction when Level 0 has 4 or more files
            if (!isCompacting) {
                isCompacting = true
                backgroundScope.launch {
                    try {
                        performLeveledCompaction()
                    } catch (e: Exception) {
                        logger.error("Compaction failed: ${e.message}", e)
                    } finally {
                        isCompacting = false
                    }
                }
            }
        }
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
        readCounter.incrementAndGet()
        val iterators = PriorityQueue<KoreIterator>()
        val imm: MemTable?
        val readersSnapshot: List<SSTableReader>
        synchronized(sstReaders) {
            imm = immutableMemTable
            readersSnapshot = sstReaders.toList()
        }

        // 1. Initialize SSTable iterators
        for (i in readersSnapshot.indices) {
            val reader = readersSnapshot[i]
            
            // Fast path: skip segments that do not overlap with the range
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, startKey) < 0) continue
            if (reader.minKey != null && ByteArrayComparator.compare(reader.minKey!!, endKey) >= 0) continue

            val offset = reader.findBlockStartOffset(startKey)
            val priority = (10 - reader.level) * 1000000 + i
            val it = SSTableIterator(reader, priority = priority, startOffset = offset, startKey = startKey, endKey = endKey)
            if (it.currentKey != null) iterators.add(it)
        }

        // 2. Initialize Immutable MemTable iterator (priority: 11000000)
        if (imm != null) {
            val immIt = MemTableIterator(imm.getTailEntries(startKey).iterator(), 11000000, endKey)
            if (immIt.currentKey != null) iterators.add(immIt)
        }

        // 3. Initialize MemTable iterator (highest priority)
        val memIt = MemTableIterator(memTable.getTailEntries(startKey).iterator(), 12000000, endKey)
        if (memIt.currentKey != null) iterators.add(memIt)

        val results = mutableListOf<ByteArray>()
        var lastKey: ByteArray? = null

        // 4. K-Way Merge Loop
        while (iterators.isNotEmpty()) {
            val top = iterators.poll()!!
            val key = top.currentKey!!

            // Check if we've already processed this key (from a newer segment)
            if (lastKey == null || ByteArrayComparator.compare(key, lastKey) != 0) {
                val value = top.value() ?: KoreDB.TOMBSTONE
                if (value.isNotEmpty()) { // Skip tombstones
                    val plainValue = if (crypto != null) crypto.decrypt(value, aad = key) else value
                    results.add(plainValue)
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
        readCounter.incrementAndGet()
        val iterators = PriorityQueue<KoreIterator>()
        val imm: MemTable?
        val readersSnapshot: List<SSTableReader>
        synchronized(sstReaders) {
            imm = immutableMemTable
            readersSnapshot = sstReaders.toList()
        }

        for (i in readersSnapshot.indices) {
            val reader = readersSnapshot[i]
            
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, startKey) < 0) continue
            if (reader.minKey != null && ByteArrayComparator.compare(reader.minKey!!, endKey) >= 0) continue

            val offset = reader.findBlockStartOffset(startKey)
            val priority = (10 - reader.level) * 1000000 + i
            val it = SSTableIterator(reader, priority = priority, startOffset = offset, startKey = startKey, endKey = endKey)
            if (it.currentKey != null) iterators.add(it)
        }

        if (imm != null) {
            val immIt = MemTableIterator(imm.getTailEntries(startKey).iterator(), 11000000, endKey)
            if (immIt.currentKey != null) iterators.add(immIt)
        }

        val memIt = MemTableIterator(memTable.getTailEntries(startKey).iterator(), 12000000, endKey)
        if (memIt.currentKey != null) iterators.add(memIt)

        val results = mutableListOf<Pair<ByteArray, ByteArray>>()
        var lastKey: ByteArray? = null

        while (iterators.isNotEmpty()) {
            val top = iterators.poll()!!
            val key = top.currentKey!!

            if (lastKey == null || ByteArrayComparator.compare(key, lastKey) != 0) {
                val value = top.value() ?: KoreDB.TOMBSTONE
                if (value.isNotEmpty()) {
                    val plainValue = if (crypto != null) crypto.decrypt(value, aad = key) else value
                    results.add(Pair(key, plainValue))
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
        readCounter.incrementAndGet()
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
        val imm: MemTable?
        val readersSnapshot: List<SSTableReader>
        synchronized(sstReaders) {
            imm = immutableMemTable
            readersSnapshot = sstReaders.toList()
        }

        // 1. Initialize SSTable iterators
        for (idx in readersSnapshot.indices) {
            val reader = readersSnapshot[idx]
            
            // Fast path: skip segments that do not overlap with the prefix
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, prefix) < 0) continue
            if (reader.minKey != null && actualEndKey != null && ByteArrayComparator.compare(reader.minKey!!, actualEndKey) >= 0) continue

            val offset = reader.findBlockStartOffset(prefix)
            val priority = (10 - reader.level) * 1000000 + idx
            val it = SSTableIterator(reader, priority = priority, startOffset = offset, startKey = prefix, endKey = actualEndKey)
            if (it.currentKey != null && it.currentKey!!.startsWith(prefix)) {
                iterators.add(it)
            }
        }

        // 2. Initialize Immutable MemTable iterator
        if (imm != null) {
            val immIt = MemTableIterator(imm.getTailEntries(prefix).iterator(), 11000000, actualEndKey)
            if (immIt.currentKey != null && immIt.currentKey!!.startsWith(prefix)) {
                iterators.add(immIt)
            }
        }

        // 3. Initialize MemTable iterator
        val memIt = MemTableIterator(memTable.getTailEntries(prefix).iterator(), 12000000, actualEndKey)
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
                    val plainValue = if (crypto != null) crypto.decrypt(value, aad = key) else value
                    results.add(plainValue)
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
        val imm: MemTable?
        val readersSnapshot: List<SSTableReader>
        synchronized(sstReaders) {
            imm = immutableMemTable
            readersSnapshot = sstReaders.toList()
        }

        for (idx in readersSnapshot.indices) {
            val reader = readersSnapshot[idx]
            
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, prefix) < 0) continue
            if (reader.minKey != null && actualEndKey != null && ByteArrayComparator.compare(reader.minKey!!, actualEndKey) >= 0) continue

            val offset = reader.findBlockStartOffset(prefix)
            val priority = (10 - reader.level) * 1000000 + idx
            val it = SSTableIterator(reader, priority = priority, startOffset = offset, startKey = prefix, endKey = actualEndKey)
            if (it.currentKey != null && it.currentKey!!.startsWith(prefix)) {
                iterators.add(it)
            }
        }

        if (imm != null) {
            val immIt = MemTableIterator(imm.getTailEntries(prefix).iterator(), 11000000, actualEndKey)
            if (immIt.currentKey != null && immIt.currentKey!!.startsWith(prefix)) {
                iterators.add(immIt)
            }
        }

        val memIt = MemTableIterator(memTable.getTailEntries(prefix).iterator(), 12000000, actualEndKey)
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
                    val plainValue = if (crypto != null) crypto.decrypt(value, aad = key) else value
                    results.add(Pair(key, plainValue))
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
        readCounter.incrementAndGet()
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
        val imm: MemTable?
        val readersSnapshot: List<SSTableReader>
        synchronized(sstReaders) {
            imm = immutableMemTable
            readersSnapshot = sstReaders.toList()
        }

        for (idx in readersSnapshot.indices) {
            val reader = readersSnapshot[idx]
            
            // Fast path: skip segments that do not overlap with the prefix
            if (reader.maxKey != null && ByteArrayComparator.compare(reader.maxKey!!, prefix) < 0) continue
            if (reader.minKey != null && actualEndKey != null && ByteArrayComparator.compare(reader.minKey!!, actualEndKey) >= 0) continue

            val offset = reader.findBlockStartOffset(prefix)
            val priority = (10 - reader.level) * 1000000 + idx
            val it = SSTableIterator(reader, priority = priority, startOffset = offset, startKey = prefix, endKey = actualEndKey)
            if (it.currentKey != null && it.currentKey!!.startsWith(prefix)) {
                iterators.add(it)
            }
        }

        if (imm != null) {
            val immIt = MemTableIterator(imm.getTailEntries(prefix).iterator(), 11000000, actualEndKey)
            if (immIt.currentKey != null && immIt.currentKey!!.startsWith(prefix)) {
                iterators.add(immIt)
            }
        }

        val memIt = MemTableIterator(memTable.getTailEntries(prefix).iterator(), 12000000, actualEndKey)
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
        readCounter.incrementAndGet()
        val topKHeap = java.util.PriorityQueue<Pair<ByteArray, Float>>(compareBy { it.second })
        val queryMag = VectorMath.getMagnitude(query)

        val imm: MemTable?
        val readersSnapshot: List<SSTableReader>
        synchronized(sstReaders) {
            imm = immutableMemTable
            readersSnapshot = sstReaders.toList()
        }

        // 🚀 PARALLEL SSTABLE SCAN
        val sstResults = readersSnapshot.map { reader ->
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

        // 🏎️ OPTIMIZED IMMUTABLE MEMTABLE SCAN (if any)
        if (imm != null) {
            scanMemTableForVectors(imm, prefix, query, queryMag, limit, topKHeap)
        }

        // 🏎️ OPTIMIZED ACTIVE MEMTABLE SCAN
        scanMemTableForVectors(memTable, prefix, query, queryMag, limit, topKHeap)

        topKHeap.toList().sortedByDescending { it.second }
    }

    private fun scanMemTableForVectors(
        mTable: MemTable,
        prefix: ByteArray,
        query: FloatArray,
        queryMag: Float,
        limit: Int,
        topKHeap: PriorityQueue<Pair<ByteArray, Float>>
    ) {
        for (entry in mTable.getTailEntries(prefix)) {
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
                if (query.size != vectorLength) continue

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
        synchronized(sstReaders) {
            sstReaders.clear()
        }

        directory.listFiles()?.forEach { it.delete() }

        memTable.clear()
        immutableMemTable = null
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

    /**
     * Retrieves runtime storage engine and operational metrics.
     */
    fun getMetrics(): KoreDBMetrics {
        val sstCount = synchronized(sstReaders) { sstReaders.size }
        val diskUsage = directory.listFiles()?.sumOf { it.length() } ?: 0L
        return KoreDBMetrics(
            readCount = readCounter.get(),
            writeCount = writeCounter.get(),
            compactionCount = compactionCounter.get(),
            activeSSTables = sstCount,
            memTableEntries = memTable.size(),
            memTableSizeBytes = memTable.sizeInBytes().toLong(),
            totalDiskUsageBytes = diskUsage
        )
    }

    /**
     * Creates a consistent point-in-time snapshot backup in [destDir].
     * Flushes in-memory entries and verifies file integrity via CRC32.
     *
     * @param destDir Target directory where backup files and BACKUP.json will be written.
     * @return [BackupMetadata] containing file lists, sizes, and integrity checksums.
     */
    suspend fun createBackup(destDir: File): BackupMetadata = withContext(Dispatchers.IO) {
        if (!destDir.exists()) {
            destDir.mkdirs()
        }

        // 1. Ensure all memory table entries are flushed to SSTables
        flushMemTableInternal()

        val snapshotReaders = synchronized(sstReaders) { sstReaders.toList() }
        val manifestFile = File(directory, "MANIFEST")
        val filesToCopy = mutableListOf<File>()
        if (manifestFile.exists()) {
            filesToCopy.add(manifestFile)
        }
        for (reader in snapshotReaders) {
            if (reader.file.exists()) {
                filesToCopy.add(reader.file)
            }
        }

        val checksumMap = mutableMapOf<String, Long>()
        val sstableNames = mutableListOf<String>()
        var totalSize = 0L

        for (file in filesToCopy) {
            val destFile = File(destDir, file.name)
            file.copyTo(destFile, overwrite = true)
            val crc = computeCrc32(destFile)
            checksumMap[file.name] = crc
            totalSize += destFile.length()
            if (file.name.endsWith(".sst")) {
                sstableNames.add(file.name)
            }
        }

        val metadata = BackupMetadata(
            timestamp = System.currentTimeMillis(),
            version = "0.1.3",
            sstableFiles = sstableNames,
            fileChecksums = checksumMap,
            totalSizeBytes = totalSize
        )

        val jsonString = backupJson.encodeToString(BackupMetadata.serializer(), metadata)
        File(destDir, "BACKUP.json").writeText(jsonString)

        logger.info("Created database backup with ${sstableNames.size} SSTables at ${destDir.absolutePath}")
        metadata
    }

    /**
     * Restores database state from a backup directory created by [createBackup].
     * Verifies CRC32 checksums before swapping files.
     *
     * @param srcDir Directory containing backup SSTables and BACKUP.json.
     * @return true if restoration succeeded.
     */
    suspend fun restoreFromBackup(srcDir: File): Boolean = withContext(Dispatchers.IO) {
        val backupJsonFile = File(srcDir, "BACKUP.json")
        if (!backupJsonFile.exists()) {
            throw BackupRestoreException("Missing BACKUP.json in source directory: ${srcDir.absolutePath}")
        }
        val metadata = try {
            backupJson.decodeFromString<BackupMetadata>(backupJsonFile.readText())
        } catch (e: Exception) {
            throw BackupRestoreException("Failed to parse BACKUP.json", e)
        }

        // Verify checksums of all backup files
        for ((fileName, expectedCrc) in metadata.fileChecksums) {
            val srcFile = File(srcDir, fileName)
            if (!srcFile.exists()) {
                throw BackupRestoreException("Backup file missing: $fileName")
            }
            val actualCrc = computeCrc32(srcFile)
            if (actualCrc != expectedCrc) {
                throw BackupRestoreException("Checksum mismatch for backup file: $fileName (expected $expectedCrc, got $actualCrc)")
            }
        }

        // Close current WAL and clear readers
        wal.close()
        synchronized(sstReaders) {
            sstReaders.clear()
        }

        // Clear existing database directory files
        directory.listFiles()?.forEach { it.delete() }

        // Copy all SSTables and MANIFEST from backup
        for (fileName in metadata.fileChecksums.keys) {
            val srcFile = File(srcDir, fileName)
            val destFile = File(directory, fileName)
            srcFile.copyTo(destFile, overwrite = true)
        }

        // Reload DB state
        loadStorageState()
        logger.info("Database successfully restored from backup at ${srcDir.absolutePath}")
        true
    }

    private fun computeCrc32(file: File): Long {
        val crc = CRC32()
        file.inputStream().use { input ->
            val buffer = ByteArray(8192)
            var bytesRead: Int
            while (input.read(buffer).also { bytesRead = it } != -1) {
                crc.update(buffer, 0, bytesRead)
            }
        }
        return crc.value
    }

    companion object {
        private val backupJson = kotlinx.serialization.json.Json {
            prettyPrint = true
            ignoreUnknownKeys = true
        }

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
