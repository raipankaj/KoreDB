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

class KoreDB(private val directory: File) {

    private var memTable = MemTable()
    private val sstFileCounter = AtomicInteger(0)
    private val MEMTABLE_FLUSH_THRESHOLD_BYTES = 1 * 1024 * 1024

    private val walFile: File
    private var wal: WriteAheadLog
    private val sstReaders = mutableListOf<SSTableReader>()

    init {
        // 1. 🛡️ CRITICAL: Create directory FIRST.
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw java.io.IOException("Unable to create database directory: ${directory.absolutePath}")
            }
        }

        // 2. Now it is safe to define file paths
        walFile = File(directory, "kore.wal")

        // On startup, load all existing .sst files into our readers!
        val existingFiles = directory.listFiles { _, name -> name.endsWith(".sst") }
            ?.sortedBy { it.name } // Ensure we read in order (segment_0, segment_1...)

        existingFiles?.forEach { file ->
            try {
                sstReaders.add(SSTableReader(file))
                sstFileCounter.incrementAndGet()
            } catch (e: Exception) {
                println("❌ Corrupt SSTable found skipped: ${file.name}")
            }
        }

        // 3. 🚨 CRITICAL FIX: Restore MemTable from WAL before opening it for writing!
        restoreFromWal()

        // 4. Open WAL for new writes
        wal = WriteAheadLog(walFile)
    }

    /**
     * Reads the existing WAL file and replays data into the MemTable.
     * This ensures data isn't lost if the app was killed before a flush.
     */
    private fun restoreFromWal() {
        if (!walFile.exists()) return

        println("🔄 Recovering from WAL...")
        try {
            val raf = RandomAccessFile(walFile, "r")
            // Read until end of file
            while (raf.filePointer < raf.length()) {
                // Format: [KeySize: Int] [ValueSize: Int] [Key] [Value]
                val keySize = raf.readInt()
                val valueSize = raf.readInt()

                val key = ByteArray(keySize)
                raf.readFully(key)

                val value = ByteArray(valueSize)
                raf.readFully(value)

                // Replay into memory!
                memTable.put(key, value)
            }
            raf.close()
            println("✅ WAL Recovery Complete. MemTable restored with ${memTable.sizeInBytes()} bytes.")
        } catch (e: Exception) {
            println("⚠️ WAL seems corrupt or partial. Starting fresh for new writes. Error: ${e.message}")
            // In a strict DB, we might truncate the corrupt tail.
            // For now, we assume what we read so far is good.
        }
    }


    suspend fun writeBatchRaw(batch: List<Pair<ByteArray, ByteArray>>,
                              urgent: Boolean = false) = withContext(Dispatchers.IO) {
        for (pair in batch) {
            val keyBytes = pair.first
            val valueBytes = pair.second

            wal.append(keyBytes, valueBytes)
            memTable.put(keyBytes, valueBytes)
        }

        // Only force the hardware to wait if the developer explicitly asks.
        // Otherwise, let the OS handle the write-back timing (like Room).
        if (urgent) {
            wal.flush()
        }

        if (memTable.sizeInBytes() >= MEMTABLE_FLUSH_THRESHOLD_BYTES) {
            flushMemTable()
        }
    }

    suspend fun putRaw(key: ByteArray, value: ByteArray) {
        writeBatchRaw(listOf(Pair(key, value)))
    }

    suspend fun deleteRaw(key: ByteArray) {
        // Deleting is just writing a Tombstone! Blazing fast O(1) delete.
        writeBatchRaw(listOf(Pair(key, TOMBSTONE)))
    }

    fun getRaw(key: ByteArray): ByteArray? {
        // Tier 1: Search RAM
        val ramResult = memTable.get(key)
        if (ramResult != null) {
            return if (ramResult.isEmpty()) null else ramResult // 🚨 Respect Tombstone
        }

        // Tier 2: Search Disk (Newest to Oldest)
        for (i in sstReaders.indices.reversed()) {
            val diskResult = sstReaders[i].find(key)
            if (diskResult != null) {
                return if (diskResult.isEmpty()) null else diskResult // 🚨 Respect Tombstone
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

        // Add new reader
        val newReader = SSTableReader(sstFile)
        sstReaders.add(newReader)

        println("✅ Flushed to disk. Total Segments: ${sstReaders.size}")

        // 🚨 COMPACTION TRIGGER 🚨
        if (sstReaders.size >= COMPACTION_THRESHOLD) {
            performCompaction()
        }
    }

    private fun performCompaction() {
        println("\n🚧 STARTING COMPACTION (Merging ${sstReaders.size} files)...")

        val compactedFile = File(directory, "compacted_${System.currentTimeMillis()}.sst")

        // 1. Run the merge algorithm
        Compactor.compact(sstReaders, compactedFile)

        // 2. Atomic Switch
        // Close old readers
        sstReaders.forEach {
            // We can't easily "close" mmap without JVM tricks,
            // but we can drop the reference. The OS handles cleanup eventually.
        }

        // Delete old files
        sstReaders.forEach { it.file.delete() }

        // 3. Reset State
        sstReaders.clear()
        sstReaders.add(SSTableReader(compactedFile))

        println("♻️ COMPACTION COMPLETE. Disk Space Reclaimed. Active Files: 1\n")
    }

    /**
     * The Multi-Tiered Read Algorithm
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

        // 1. Scan Disk (Oldest to Newest)
        for (reader in sstReaders) {
            val entries = reader.scanByPrefix(prefix)
            for (entry in entries) {
                val keyStr = String(entry.first, Charsets.UTF_8)
                if (entry.second.isEmpty()) {
                    resultMap.remove(keyStr) // Tombstone! Remove older version.
                } else {
                    resultMap[keyStr] = entry.second
                }
            }
        }

        // 2. Scan RAM (Newest)
        for (entry in memTable.getSortedEntries()) {
            val keyBytes = entry.key
            if (keyBytes.size >= prefix.size && keyBytes.sliceArray(prefix.indices).contentEquals(prefix)) {
                val keyStr = String(keyBytes, Charsets.UTF_8)
                if (entry.value.isEmpty()) {
                    resultMap.remove(keyStr) // Tombstone!
                } else {
                    resultMap[keyStr] = entry.value
                }
            }
        }

        return resultMap.values.toList()
    }

    /**
     * Returns all active KEYS that start with the given prefix.
     * Used for bulk operations like DeleteAll.
     */
    fun getKeysByPrefixRaw(prefix: ByteArray): List<ByteArray> {
        val resultMap = mutableMapOf<String, ByteArray>()

        // 1. Scan Disk
        for (reader in sstReaders) {
            val entries = reader.scanByPrefix(prefix)
            for (entry in entries) {
                val keyStr = String(entry.first, Charsets.UTF_8)
                if (entry.second.isEmpty()) {
                    resultMap.remove(keyStr) // It's a Tombstone, ignore it
                } else {
                    resultMap[keyStr] = entry.first // Save the KEY
                }
            }
        }

        // 2. Scan RAM
        for (entry in memTable.getSortedEntries()) {
            val keyBytes = entry.key
            if (keyBytes.size >= prefix.size && keyBytes.sliceArray(prefix.indices).contentEquals(prefix)) {
                val keyStr = String(keyBytes, Charsets.UTF_8)
                if (entry.value.isEmpty()) {
                    resultMap.remove(keyStr) // Tombstone
                } else {
                    resultMap[keyStr] = keyBytes // Save the KEY
                }
            }
        }

        return resultMap.values.toList()
    }


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

    fun flushHardware() {
        wal.flush()
    }

    fun close() {
        wal.close()
    }

    companion object {
        // A 0-byte array represents a Deleted Record (Tombstone)
        val TOMBSTONE = ByteArray(0)
        private const val COMPACTION_THRESHOLD = 3

    }
}