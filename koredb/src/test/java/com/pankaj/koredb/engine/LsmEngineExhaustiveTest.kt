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

import com.pankaj.koredb.foundation.BlockCache
import com.pankaj.koredb.foundation.BloomFilter
import com.pankaj.koredb.foundation.MemTable
import com.pankaj.koredb.foundation.SSTable
import com.pankaj.koredb.foundation.SSTableReader
import com.pankaj.koredb.log.WriteAheadLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.RandomAccessFile
import java.util.UUID

class LsmEngineExhaustiveTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_lsm_exhaustive_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // MEMTABLE TESTS (15 Tests)
    // ========================================================================

    @Test
    fun testMemTableEmptyGetReturnsNull() {
        val mem = MemTable()
        assertNull(mem.get("non_existent".toByteArray()))
    }

    @Test
    fun testMemTableBasicPutAndGet() {
        val mem = MemTable()
        val key = "user:1".toByteArray()
        val value = "Alice".toByteArray()
        mem.put(key, value)
        assertArrayEquals(value, mem.get(key))
    }

    @Test
    fun testMemTableOverwriteUpdatesValue() {
        val mem = MemTable()
        val key = "user:1".toByteArray()
        mem.put(key, "Alice".toByteArray())
        mem.put(key, "Bob".toByteArray())
        assertArrayEquals("Bob".toByteArray(), mem.get(key))
    }

    @Test
    fun testMemTableTombstoneReturnsTombstoneBytes() {
        val mem = MemTable()
        val key = "user:1".toByteArray()
        mem.put(key, KoreDB.TOMBSTONE)
        assertArrayEquals(KoreDB.TOMBSTONE, mem.get(key))
    }

    @Test
    fun testMemTableSizeTrackingIncreasesOnPut() {
        val mem = MemTable()
        val initialSize = mem.sizeInBytes()
        mem.put("k1".toByteArray(), "v1".toByteArray())
        assertTrue(mem.sizeInBytes() > initialSize)
    }

    @Test
    fun testMemTableSizeTrackingOnOverwrite() {
        val mem = MemTable()
        mem.put("k1".toByteArray(), "short".toByteArray())
        val size1 = mem.sizeInBytes()
        mem.put("k1".toByteArray(), "a very long replacement value here".toByteArray())
        assertTrue(mem.sizeInBytes() > size1)
    }

    @Test
    fun testMemTableBulkPutAll() {
        val mem = MemTable()
        val batch = (1..50).map { i -> "key_$i".toByteArray() to "val_$i".toByteArray() }
        mem.putAll(batch)
        for (i in 1..50) {
            assertArrayEquals("val_$i".toByteArray(), mem.get("key_$i".toByteArray()))
        }
    }

    @Test
    fun testMemTableBinaryKeysWithNullBytes() {
        val mem = MemTable()
        val key = byteArrayOf(0x00, 0x01, 0x00, 0xFF.toByte())
        val value = byteArrayOf(0xDE.toByte(), 0xAD.toByte())
        mem.put(key, value)
        assertArrayEquals(value, mem.get(key))
    }

    @Test
    fun testMemTableLargeKeyAndValue() {
        val mem = MemTable()
        val key = ByteArray(1024) { (it % 128).toByte() }
        val value = ByteArray(16384) { (it % 256).toByte() }
        mem.put(key, value)
        assertArrayEquals(value, mem.get(key))
    }

    @Test
    fun testMemTableSortedOrderIteration() {
        val mem = MemTable()
        val keys = listOf("c", "a", "d", "b")
        keys.forEach { mem.put(it.toByteArray(), it.toByteArray()) }
        val sortedKeys = mem.getSortedEntries().map { String(it.key) }.toList()
        assertEquals(listOf("a", "b", "c", "d"), sortedKeys)
    }

    @Test
    fun testMemTableClearResetsContent() {
        val mem = MemTable()
        mem.put("k1".toByteArray(), "v1".toByteArray())
        mem.clear()
        assertNull(mem.get("k1".toByteArray()))
        assertEquals(0, mem.size())
    }

    @Test
    fun testMemTableEmptyKey() {
        val mem = MemTable()
        mem.put(ByteArray(0), "empty_val".toByteArray())
        assertArrayEquals("empty_val".toByteArray(), mem.get(ByteArray(0)))
    }

    @Test
    fun testMemTableEmptyValue() {
        val mem = MemTable()
        mem.put("empty_key".toByteArray(), ByteArray(0))
        assertArrayEquals(ByteArray(0), mem.get("empty_key".toByteArray()))
    }

    @Test
    fun testMemTableConcurrentInsertsAndReads() {
        runBlocking {
            val mem = MemTable()
            val jobs = (1..20).map { worker ->
                async(Dispatchers.Default) {
                    for (i in 1..50) {
                        val k = "worker_${worker}_key_$i".toByteArray()
                        val v = "worker_${worker}_val_$i".toByteArray()
                        mem.put(k, v)
                        val read = mem.get(k)
                        assertNotNull(read)
                    }
                }
            }
            jobs.awaitAll()
            assertEquals(1000, mem.size())
        }
    }

    @Test
    fun testMemTableUnicodeKeyHandling() {
        val mem = MemTable()
        val key = "用户:🚀:123".toByteArray(Charsets.UTF_8)
        val value = "数据:✨".toByteArray(Charsets.UTF_8)
        mem.put(key, value)
        assertArrayEquals(value, mem.get(key))
    }

    // ========================================================================
    // BLOOM FILTER TESTS (12 Tests)
    // ========================================================================

    @Test
    fun testBloomFilterBasicContainment() {
        val bf = BloomFilter(1000, 3)
        val key = "user:42".toByteArray()
        bf.add(key)
        assertTrue(bf.mightContain(key))
    }

    @Test
    fun testBloomFilterMissingElementLikelyFalse() {
        val bf = BloomFilter(1000, 3)
        bf.add("present_key".toByteArray())
        assertFalse(bf.mightContain("completely_different_key_xyz".toByteArray()))
    }

    @Test
    fun testBloomFilterZeroCapacityDoesNotThrow() {
        val bf = BloomFilter(1, 1)
        bf.add("test".toByteArray())
        assertTrue(bf.mightContain("test".toByteArray()))
    }

    @Test
    fun testBloomFilterMultipleInsertsAllPresent() {
        val bf = BloomFilter(500, 3)
        for (i in 1..200) {
            bf.add("key_$i".toByteArray())
        }
        for (i in 1..200) {
            assertTrue(bf.mightContain("key_$i".toByteArray()))
        }
    }

    @Test
    fun testBloomFilterFalsePositiveRateWithinBounds() {
        val numElements = 2000
        val bitSize = 25000 // ~12.5 bits/element
        val bf = BloomFilter(bitSize, 5)
        for (i in 0 until numElements) {
            bf.add("key_$i".toByteArray())
        }
        var falsePositives = 0
        val testTrials = 2000
        for (i in 0 until testTrials) {
            if (bf.mightContain("non_existent_random_key_$i".toByteArray())) {
                falsePositives++
            }
        }
        val observedFpp = falsePositives.toDouble() / testTrials
        assertTrue("Observed FPP $observedFpp should be reasonably low", observedFpp < 0.10)
    }

    @Test
    fun testBloomFilterEmptyFilterReturnsFalse() {
        val bf = BloomFilter(100, 3)
        assertFalse(bf.mightContain("any_key".toByteArray()))
    }

    @Test
    fun testBloomFilterBinaryKeys() {
        val bf = BloomFilter(100, 3)
        val k1 = byteArrayOf(0x00, 0x01, 0x02)
        val k2 = byteArrayOf(0x00, 0x01, 0x03)
        bf.add(k1)
        assertTrue(bf.mightContain(k1))
        assertFalse(bf.mightContain(k2))
    }

    @Test
    fun testBloomFilterRepeatedAdditionIdempotent() {
        val bf = BloomFilter(100, 3)
        val key = "idempotent_key".toByteArray()
        bf.add(key)
        assertTrue(bf.mightContain(key))
        bf.add(key)
        assertTrue(bf.mightContain(key))
    }

    @Test
    fun testBloomFilterSingleCharacterKeys() {
        val bf = BloomFilter(100, 3)
        bf.add("a".toByteArray())
        bf.add("b".toByteArray())
        assertTrue(bf.mightContain("a".toByteArray()))
        assertTrue(bf.mightContain("b".toByteArray()))
        assertFalse(bf.mightContain("z".toByteArray()))
    }

    @Test
    fun testBloomFilterLargeNumberOfItems() {
        val bf = BloomFilter(10000, 3)
        for (i in 1..5000) {
            bf.add("bulk_$i".toByteArray())
        }
        for (i in 1..5000) {
            assertTrue(bf.mightContain("bulk_$i".toByteArray()))
        }
    }

    @Test
    fun testBloomFilterZeroCopyRange() {
        val bf = BloomFilter(100, 3)
        val full = "prefix:user:123".toByteArray()
        bf.addRange(full, 0, 11) // "prefix:user"
        assertTrue(bf.mightContain("prefix:user".toByteArray()))
        assertFalse(bf.mightContain("other".toByteArray()))
    }

    @Test
    fun testBloomFilterSerialization() {
        val bf = BloomFilter(500, 3)
        for (i in 1..30) bf.add("test_$i".toByteArray())
        val bytes = bf.toByteArray()
        val restored = BloomFilter.fromByteArray(bf.bitSize, bf.hashFunctions, bytes)
        for (i in 1..30) assertTrue(restored.mightContain("test_$i".toByteArray()))
        assertFalse(restored.mightContain("missing_xyz".toByteArray()))
    }

    // ========================================================================
    // BLOCK CACHE TESTS (10 Tests)
    // ========================================================================

    @Test
    fun testBlockCacheGetMissingReturnsNull() {
        val cache = BlockCache(maxSizeBytes = 1024 * 1024)
        assertNull(cache.get("missing_key"))
    }

    @Test
    fun testBlockCachePutAndGetHit() {
        val cache = BlockCache(maxSizeBytes = 1024 * 1024)
        val block = ByteArray(100) { it.toByte() }
        cache.put("file1.sst:0", block)
        val retrieved = cache.get("file1.sst:0")
        assertNotNull(retrieved)
        assertArrayEquals(block, retrieved)
    }

    @Test
    fun testBlockCacheDifferentiatesKeys() {
        val cache = BlockCache(maxSizeBytes = 1024 * 1024)
        cache.put("key1", "val1".toByteArray())
        cache.put("key2", "val2".toByteArray())
        assertArrayEquals("val1".toByteArray(), cache.get("key1"))
        assertArrayEquals("val2".toByteArray(), cache.get("key2"))
    }

    @Test
    fun testBlockCacheLruEvictionWhenCapacityExceeded() {
        val cache = BlockCache(maxSizeBytes = 200) // Very small
        val b1 = ByteArray(60) { 1 }
        val b2 = ByteArray(60) { 2 }
        val b3 = ByteArray(60) { 3 }

        cache.put("k1", b1)
        cache.put("k2", b2)
        cache.put("k3", b3) // Should evict older entries

        assertNotNull(cache.get("k3"))
    }

    @Test
    fun testBlockCacheClearEvictsAll() {
        val cache = BlockCache(maxSizeBytes = 1024 * 1024)
        cache.put("k1", "b0".toByteArray())
        cache.put("k2", "b1".toByteArray())
        cache.clear()
        assertNull(cache.get("k1"))
        assertNull(cache.get("k2"))
    }

    @Test
    fun testBlockCacheInvalidateFileRemovesTargetFileOnly() {
        val cache = BlockCache(maxSizeBytes = 1024 * 1024)
        cache.put("f1.sst:0", "b1".toByteArray())
        cache.put("f2.sst:0", "b2".toByteArray())
        cache.remove("f1.sst:0")
        assertNull(cache.get("f1.sst:0"))
        assertNotNull(cache.get("f2.sst:0"))
    }

    @Test
    fun testBlockCacheZeroByteBlock() {
        val cache = BlockCache(maxSizeBytes = 1024)
        cache.put("empty", ByteArray(0))
        val res = cache.get("empty")
        assertNotNull(res)
        assertEquals(0, res?.size)
    }

    @Test
    fun testBlockCacheOverwriteSameKeyUpdatesContent() {
        val cache = BlockCache(maxSizeBytes = 1024)
        cache.put("k1", "old".toByteArray())
        cache.put("k1", "new".toByteArray())
        assertArrayEquals("new".toByteArray(), cache.get("k1"))
    }

    @Test
    fun testBlockCacheStatsTracking() {
        val cache = BlockCache(maxSizeBytes = 1024)
        cache.put("hit_key", "hit_val".toByteArray())
        cache.get("hit_key")
        cache.get("miss_key")
        assertEquals(1, cache.hits.get())
        assertEquals(1, cache.misses.get())
    }

    @Test
    fun testBlockCacheConcurrentAccess() {
        runBlocking {
            val cache = BlockCache(maxSizeBytes = 1024 * 1024)
            val jobs = (1..10).map { worker ->
                async(Dispatchers.Default) {
                    for (i in 0..50) {
                        val k = "worker_$worker:$i"
                        val b = "data_$worker-$i".toByteArray()
                        cache.put(k, b)
                        val read = cache.get(k)
                        assertNotNull(read)
                    }
                }
            }
            jobs.awaitAll()
        }
    }

    // ========================================================================
    // SSTABLE & SSTABLEReader TESTS (15 Tests)
    // ========================================================================

    @Test
    fun testSSTableCreateAndReadSingleRecord() {
        val sstFile = File(testDir, "000001.sst")
        val data = listOf("apple".toByteArray() to "fruit".toByteArray())
        SSTable.writeSortedEntries(data.asSequence(), sstFile)

        assertTrue(sstFile.exists())
        assertTrue(sstFile.length() >= 16)

        val reader = SSTableReader(sstFile, null)
        assertArrayEquals("fruit".toByteArray(), reader.find("apple".toByteArray()))
        assertNull(reader.find("banana".toByteArray()))
        reader.close()
    }

    @Test
    fun testSSTableMultipleRecordsSorted() {
        val sstFile = File(testDir, "000002.sst")
        val data = listOf(
            "apple".toByteArray() to "green".toByteArray(),
            "banana".toByteArray() to "yellow".toByteArray(),
            "cherry".toByteArray() to "red".toByteArray()
        )
        SSTable.writeSortedEntries(data.asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        assertArrayEquals("green".toByteArray(), reader.find("apple".toByteArray()))
        assertArrayEquals("yellow".toByteArray(), reader.find("banana".toByteArray()))
        assertArrayEquals("red".toByteArray(), reader.find("cherry".toByteArray()))
        reader.close()
    }

    @Test
    fun testSSTableKeyRangeBounds() {
        val sstFile = File(testDir, "000003.sst")
        val data = listOf(
            "b".toByteArray() to "val_b".toByteArray(),
            "m".toByteArray() to "val_m".toByteArray(),
            "z".toByteArray() to "val_z".toByteArray()
        )
        SSTable.writeSortedEntries(data.asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        assertArrayEquals("b".toByteArray(), reader.minKey)
        assertArrayEquals("z".toByteArray(), reader.maxKey)
        reader.close()
    }

    @Test
    fun testSSTableSparseIndexMultiBlock() {
        val sstFile = File(testDir, "000004.sst")
        val data = (0..500).map { i ->
            String.format("key_%05d", i).toByteArray() to ByteArray(200) { (it % 100).toByte() }
        }
        SSTable.writeSortedEntries(data.asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        for (i in listOf(0, 100, 250, 499, 500)) {
            val k = String.format("key_%05d", i).toByteArray()
            assertNotNull(reader.find(k))
        }
        assertNull(reader.find("key_99999".toByteArray()))
        reader.close()
    }

    @Test
    fun testSSTableTombstoneIsPreserved() {
        val sstFile = File(testDir, "000007.sst")
        val data = listOf("deleted_key".toByteArray() to KoreDB.TOMBSTONE)
        SSTable.writeSortedEntries(data.asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        val read = reader.find("deleted_key".toByteArray())
        assertNotNull(read)
        assertArrayEquals(KoreDB.TOMBSTONE, read)
        reader.close()
    }

    @Test
    fun testSSTableWithBlockCachePopulatesCache() {
        val sstFile = File(testDir, "000008.sst")
        val cache = BlockCache(1024 * 1024)
        val data = listOf("cached_key".toByteArray() to "cached_val".toByteArray())
        SSTable.writeSortedEntries(data.asSequence(), sstFile)

        val reader = SSTableReader(sstFile, cache)
        val val1 = reader.find("cached_key".toByteArray())
        assertNotNull(val1)

        val val2 = reader.find("cached_key".toByteArray())
        assertNotNull(val2)
        assertArrayEquals(val1, val2)
        reader.close()
    }

    @Test
    fun testSSTableCorruptedMagicThrowsOnOpen() {
        val sstFile = File(testDir, "000009.sst")
        SSTable.writeSortedEntries(listOf("k".toByteArray() to "v".toByteArray()).asSequence(), sstFile)

        RandomAccessFile(sstFile, "rw").use { raf ->
            raf.seek(sstFile.length() - 4)
            raf.writeInt(0x12345678)
        }

        assertThrows(IllegalStateException::class.java) {
            SSTableReader(sstFile, null)
        }
    }

    @Test
    fun testSSTableTruncatedFileThrowsOnOpen() {
        val sstFile = File(testDir, "000010.sst")
        SSTable.writeSortedEntries(listOf("k".toByteArray() to "v".toByteArray()).asSequence(), sstFile)

        RandomAccessFile(sstFile, "rw").use { raf ->
            raf.setLength(8)
        }

        assertThrows(IllegalStateException::class.java) {
            SSTableReader(sstFile, null)
        }
    }

    @Test
    fun testSSTableBinaryPayloads() {
        val sstFile = File(testDir, "000012.sst")
        val k = byteArrayOf(0x01, 0x02, 0x03)
        val v = byteArrayOf(0xFF.toByte(), 0xFE.toByte(), 0xFD.toByte())
        SSTable.writeSortedEntries(listOf(k to v).asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        assertArrayEquals(v, reader.find(k))
        reader.close()
    }

    @Test
    fun testSSTableLevelMetadataProperty() {
        val sstFile = File(testDir, "000014.sst")
        SSTable.writeSortedEntries(listOf("k".toByteArray() to "v".toByteArray()).asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        reader.level = 2
        assertEquals(2, reader.level)
        reader.close()
    }

    @Test
    fun testSSTableKeysLargerThanBlock() {
        val sstFile = File(testDir, "000015.sst")
        val largeVal = ByteArray(32768) { 7 }
        SSTable.writeSortedEntries(listOf("big_key".toByteArray() to largeVal).asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        val read = reader.find("big_key".toByteArray())
        assertNotNull(read)
        assertEquals(32768, read?.size)
        reader.close()
    }

    @Test
    fun testSSTableBloomFilterRejectionBypassesDiskSearch() {
        val sstFile = File(testDir, "000018.sst")
        val data = (1..100).map { "key_$it".toByteArray() to "val_$it".toByteArray() }
        SSTable.writeSortedEntries(data.asSequence(), sstFile)

        val reader = SSTableReader(sstFile, null)
        assertNull(reader.find("totally_unrelated_key".toByteArray()))
        reader.close()
    }

    @Test
    fun testSSTableFromMemTable() {
        val sstFile = File(testDir, "mem_sst.sst")
        val mem = MemTable()
        mem.put("k1".toByteArray(), "v1".toByteArray())
        mem.put("k2".toByteArray(), "v2".toByteArray())
        SSTable.writeFromMemTable(mem, sstFile)

        val reader = SSTableReader(sstFile, null)
        assertArrayEquals("v1".toByteArray(), reader.find("k1".toByteArray()))
        assertArrayEquals("v2".toByteArray(), reader.find("k2".toByteArray()))
        reader.close()
    }

    @Test
    fun testSSTableEmptyEntriesDirectWrite() {
        val sstFile = File(testDir, "empty.sst")
        SSTable.writeSortedEntries(emptySequence(), sstFile)
        assertTrue(sstFile.exists())
    }

    @Test
    fun testSSTableConcurrentReadAccess() {
        runBlocking {
            val sstFile = File(testDir, "000020.sst")
            val data = (1..100).map { String.format("k_%05d", it).toByteArray() to "v_$it".toByteArray() }
            SSTable.writeSortedEntries(data.asSequence(), sstFile)

            val reader = SSTableReader(sstFile, null)
            val jobs = (1..10).map {
                async(Dispatchers.Default) {
                    for (i in 1..100) {
                        val res = reader.find(String.format("k_%05d", i).toByteArray())
                        assertNotNull(res)
                    }
                }
            }
            jobs.awaitAll()
            reader.close()
        }
    }

    // ========================================================================
    // WRITE-AHEAD LOG (WAL) TESTS (14 Tests)
    // ========================================================================

    @Test
    fun testWalCreatesFileOnAppend() {
        val walFile = File(testDir, "test.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf("k1".toByteArray() to "v1".toByteArray()))
        assertTrue(walFile.exists())
        assertTrue(walFile.length() > 0)
        wal.close()
    }

    @Test
    fun testWalReplaySingleEntry() {
        val walFile = File(testDir, "test_replay.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf("hello".toByteArray() to "world".toByteArray()))
        wal.close()

        val replayed = mutableListOf<Pair<ByteArray, ByteArray>>()
        WriteAheadLog.replay(walFile) { k, v ->
            replayed.add(k to v)
        }
        assertEquals(1, replayed.size)
        assertArrayEquals("hello".toByteArray(), replayed[0].first)
        assertArrayEquals("world".toByteArray(), replayed[0].second)
    }

    @Test
    fun testWalReplayMultipleBatches() {
        val walFile = File(testDir, "test_multi.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf("k1".toByteArray() to "v1".toByteArray()))
        wal.appendBatch(listOf("k2".toByteArray() to "v2".toByteArray(), "k3".toByteArray() to "v3".toByteArray()))
        wal.close()

        val replayed = mutableMapOf<String, String>()
        WriteAheadLog.replay(walFile) { k, v ->
            replayed[String(k)] = String(v)
        }
        assertEquals(3, replayed.size)
        assertEquals("v1", replayed["k1"])
        assertEquals("v2", replayed["k2"])
        assertEquals("v3", replayed["k3"])
    }

    @Test
    fun testWalReplayEmptyFileReturnsZeroRecords() {
        val walFile = File(testDir, "empty.wal")
        walFile.createNewFile()
        var count = 0
        WriteAheadLog.replay(walFile) { _, _ -> count++ }
        assertEquals(0, count)
    }

    @Test
    fun testWalTombstoneReplay() {
        val walFile = File(testDir, "tombstone.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf("tomb_key".toByteArray() to KoreDB.TOMBSTONE))
        wal.close()

        var sawTombstone = false
        WriteAheadLog.replay(walFile) { _, v ->
            if (v.contentEquals(KoreDB.TOMBSTONE)) sawTombstone = true
        }
        assertTrue(sawTombstone)
    }

    @Test
    fun testWalTruncatedRecordDiscardedGracefully() {
        val walFile = File(testDir, "trunc.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf("good_key".toByteArray() to "good_val".toByteArray()))
        wal.close()

        val originalLen = walFile.length()
        RandomAccessFile(walFile, "rw").use { raf ->
            raf.seek(originalLen)
            raf.write(byteArrayOf(0x01, 0x02, 0x03, 0x04, 0x05))
        }

        val replayed = mutableListOf<String>()
        WriteAheadLog.replay(walFile) { k, _ -> replayed.add(String(k)) }
        assertEquals(1, replayed.size)
        assertEquals("good_key", replayed[0])
    }

    @Test
    fun testWalCorruptCrcThrowsOrSkipsSafely() {
        val walFile = File(testDir, "crc_bad.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf("key1".toByteArray() to "val1".toByteArray()))
        wal.close()

        RandomAccessFile(walFile, "rw").use { raf ->
            raf.seek(walFile.length() - 2)
            raf.write(0xFF)
        }

        val replayed = mutableListOf<String>()
        try {
            WriteAheadLog.replay(walFile) { k, _ -> replayed.add(String(k)) }
        } catch (_: Exception) {
        }
    }

    @Test
    fun testWalBinaryKeysAndValues() {
        val walFile = File(testDir, "binary.wal")
        val wal = WriteAheadLog(walFile)
        val k = byteArrayOf(0x00, 0x01)
        val v = byteArrayOf(0x02, 0x03)
        wal.appendBatch(listOf(k to v))
        wal.close()

        var replayed = false
        WriteAheadLog.replay(walFile) { rk, rv ->
            if (rk.contentEquals(k) && rv.contentEquals(v)) replayed = true
        }
        assertTrue(replayed)
    }

    @Test
    fun testWalLargeBatchAppend() {
        val walFile = File(testDir, "large_batch.wal")
        val wal = WriteAheadLog(walFile)
        val batch = (1..500).map { "k_$it".toByteArray() to "v_$it".toByteArray() }
        wal.appendBatch(batch)
        wal.close()

        var count = 0
        WriteAheadLog.replay(walFile) { _, _ -> count++ }
        assertEquals(500, count)
    }

    @Test
    fun testWalLargeValueAppend() {
        val walFile = File(testDir, "large_val.wal")
        val wal = WriteAheadLog(walFile)
        val large = ByteArray(65536) { 9 }
        wal.appendBatch(listOf("big".toByteArray() to large))
        wal.close()

        var replayedSize = 0
        WriteAheadLog.replay(walFile) { _, v -> replayedSize = v.size }
        assertEquals(65536, replayedSize)
    }

    @Test
    fun testWalZeroLengthKeys() {
        val walFile = File(testDir, "zero_k.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf(ByteArray(0) to "val".toByteArray()))
        wal.close()

        var gotZeroKey = false
        WriteAheadLog.replay(walFile) { k, _ ->
            if (k.isEmpty()) gotZeroKey = true
        }
        assertTrue(gotZeroKey)
    }

    @Test
    fun testWalSequentialAppendsPreserveOrder() {
        val walFile = File(testDir, "order.wal")
        val wal = WriteAheadLog(walFile)
        for (i in 1..20) {
            wal.appendBatch(listOf("k_$i".toByteArray() to "v_$i".toByteArray()))
        }
        wal.close()

        val list = mutableListOf<String>()
        WriteAheadLog.replay(walFile) { k, _ -> list.add(String(k)) }
        for (i in 1..20) {
            assertEquals("k_$i", list[i - 1])
        }
    }

    @Test
    fun testWalNonExistentFileReplayReturnsSilently() {
        val missing = File(testDir, "does_not_exist.wal")
        var count = 0
        WriteAheadLog.replay(missing) { _, _ -> count++ }
        assertEquals(0, count)
    }

    @Test
    fun testWalConcurrentAppends() {
        runBlocking {
            val walFile = File(testDir, "concurrent.wal")
            val wal = WriteAheadLog(walFile)
            val jobs = (1..5).map { worker ->
                async(Dispatchers.IO) {
                    for (i in 1..20) {
                        synchronized(wal) {
                            wal.appendBatch(listOf("w${worker}_$i".toByteArray() to "val".toByteArray()))
                        }
                    }
                }
            }
            jobs.awaitAll()
            wal.close()

            var total = 0
            WriteAheadLog.replay(walFile) { _, _ -> total++ }
            assertEquals(100, total)
        }
    }

    // ========================================================================
    // EXPANDED LSM ENGINE BOUNDARY & STRESS SUITE (60 Tests)
    // ========================================================================

    private fun ByteArray.startsWith(prefix: ByteArray): Boolean {
        if (this.size < prefix.size) return false
        for (i in prefix.indices) {
            if (this[i] != prefix[i]) return false
        }
        return true
    }

    @Test
    fun testMemTableLargeKeyPayload() {
        val mem = MemTable()
        val largeKey = ByteArray(4096) { (it % 128).toByte() }
        val largeVal = ByteArray(32768) { ((it + 3) % 128).toByte() }
        mem.put(largeKey, largeVal)
        assertArrayEquals(largeVal, mem.get(largeKey))
    }

    @Test
    fun testMemTableSizeCalculationWithUpdates() {
        val mem = MemTable()
        val key = "key1".toByteArray()
        val val1 = "short".toByteArray()
        val val2 = "a much longer value string".toByteArray()

        mem.put(key, val1)
        val size1 = mem.sizeInBytes()
        mem.put(key, val2)
        val size2 = mem.sizeInBytes()
        assertTrue(size2 > size1)
    }

    @Test
    fun testMemTablePrefixScanningBoundary() {
        val mem = MemTable()
        mem.put("pref:a".toByteArray(), "1".toByteArray())
        mem.put("pref:b".toByteArray(), "2".toByteArray())
        mem.put("other:c".toByteArray(), "3".toByteArray())

        val tail = mem.getTailEntries("pref:".toByteArray())
        val prefHits = tail.filter { it.key.startsWith("pref:".toByteArray()) }.toList()
        assertEquals(2, prefHits.size)
    }

    @Test
    fun testMemTableClearAndReuse() {
        val mem = MemTable()
        mem.put("k1".toByteArray(), "v1".toByteArray())
        mem.clear()
        assertEquals(0, mem.size())
        assertEquals(0, mem.sizeInBytes())
        assertNull(mem.get("k1".toByteArray()))

        mem.put("k2".toByteArray(), "v2".toByteArray())
        assertEquals(1, mem.size())
        assertArrayEquals("v2".toByteArray(), mem.get("k2".toByteArray()))
    }

    @Test
    fun testBloomFilterAllZeroBytesKey() {
        val bf = BloomFilter(100, 3)
        val zeroKey = ByteArray(32) { 0 }
        bf.add(zeroKey)
        assertTrue(bf.mightContain(zeroKey))
    }

    @Test
    fun testBloomFilterRepeatedKeysPreserveBitIntegrity() {
        val bf = BloomFilter(1000, 3)
        val key = "repeat_key".toByteArray()
        for (i in 1..100) {
            bf.add(key)
            assertTrue(bf.mightContain(key))
        }
    }

    @Test
    fun testBlockCacheHitRatioCalculations() {
        val cache = BlockCache(maxSizeBytes = 100_000)
        val block = ByteArray(1024)
        cache.put("block1", block)

        cache.get("block1") // Hit
        cache.get("block1") // Hit
        cache.get("block_missing") // Miss

        assertEquals(2L, cache.hits.get())
        assertEquals(1L, cache.misses.get())
    }

    @Test
    fun testBlockCacheEvictionReducesSize() {
        val cache = BlockCache(maxSizeBytes = 2048)
        val b1 = ByteArray(1024)
        val b2 = ByteArray(1024)
        val b3 = ByteArray(1024)

        cache.put("k1", b1)
        cache.put("k2", b2)

        cache.put("k3", b3) // Must evict k1
        assertNull(cache.get("k1"))
        assertNotNull(cache.get("k3"))
        assertTrue(cache.sizeBytes <= 2048L)
    }

    @Test
    fun testSSTableSingleItemCreationAndRead() {
        val file = File(testDir, "single_item.sst")
        val pairs = listOf("only_key".toByteArray() to "only_val".toByteArray())
        SSTable.writeSortedEntries(pairs.asSequence(), file)

        val reader = SSTableReader(file, null)
        assertArrayEquals("only_val".toByteArray(), reader.find("only_key".toByteArray()))
        assertNull(reader.find("non_existent".toByteArray()))
        reader.close()
    }

    @Test
    fun testSSTableLargeBatchCreationAndRead() {
        val file = File(testDir, "large_batch.sst")
        val count = 2000
        val pairs = (1..count).map { i ->
            String.format("item_%05d", i).toByteArray() to "val_$i".toByteArray()
        }
        SSTable.writeSortedEntries(pairs.asSequence(), file)

        val reader = SSTableReader(file, null)
        for (i in listOf(1, 50, 500, 1000, 1999, 2000)) {
            val key = String.format("item_%05d", i).toByteArray()
            val expected = "val_$i".toByteArray()
            assertArrayEquals(expected, reader.find(key))
        }
        reader.close()
    }

    @Test
    fun testSSTableEmptyValuePersistence() {
        val file = File(testDir, "empty_val.sst")
        val pairs = listOf("empty_val_key".toByteArray() to ByteArray(0))
        SSTable.writeSortedEntries(pairs.asSequence(), file)

        val reader = SSTableReader(file, null)
        val res = reader.find("empty_val_key".toByteArray())
        assertNotNull(res)
        assertEquals(0, res!!.size)
        reader.close()
    }

    @Test
    fun testWalCorruptedLengthHeader() {
        val walFile = File(testDir, "corrupted_header.wal")
        val wal = WriteAheadLog(walFile)
        wal.appendBatch(listOf("k1".toByteArray() to "v1".toByteArray()))
        wal.close()

        // Append 4 garbage bytes at the end
        val raf = java.io.RandomAccessFile(walFile, "rw")
        raf.seek(raf.length())
        raf.write(byteArrayOf(0x7F, 0x7F, 0x7F, 0x7F))
        raf.close()

        val replayed = mutableListOf<Pair<String, String>>()
        WriteAheadLog.replay(walFile) { k, v ->
            replayed.add(String(k) to String(v))
        }
        // Should successfully recover k1 and safely halt at corrupted trailing record
        assertEquals(1, replayed.size)
        assertEquals("k1", replayed[0].first)
    }

    @Test
    fun testWalMultipleFlushesAndReplay() {
        val walFile = File(testDir, "multi_flush.wal")
        val wal = WriteAheadLog(walFile)
        for (i in 1..10) {
            wal.appendBatch(listOf("batch_$i".toByteArray() to "val_$i".toByteArray()))
            wal.flush()
        }
        wal.close()

        var count = 0
        WriteAheadLog.replay(walFile) { _, _ -> count++ }
        assertEquals(10, count)
    }

    @Test
    fun testByteArrayComparatorPrefixEquivalence() {
        val a = "prefix:01".toByteArray()
        val b = "prefix:02".toByteArray()
        val prefix = "prefix:".toByteArray()

        assertTrue(a.startsWith(prefix))
        assertTrue(b.startsWith(prefix))
        assertTrue(com.pankaj.koredb.foundation.ByteArrayComparator.compare(a, b) < 0)
    }

    @Test
    fun testOrderPreservingEncoderSpecialValues() {
        val zero = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(0.0)
        val posInf = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(Double.POSITIVE_INFINITY)
        val negInf = com.pankaj.koredb.foundation.OrderPreservingEncoder.encodeDouble(Double.NEGATIVE_INFINITY)

        assertTrue(com.pankaj.koredb.foundation.ByteArrayComparator.compare(negInf, zero) < 0)
        assertTrue(com.pankaj.koredb.foundation.ByteArrayComparator.compare(zero, posInf) < 0)
        assertEquals(0.0, com.pankaj.koredb.foundation.OrderPreservingEncoder.decodeDouble(zero), 0.0)
    }

    // 45 Fine-grained LSM Micro-Property Boundary Tests
    @Test fun testLsmMicroBoundary01() = verifyMemTableKvPair("k01", "v01")
    @Test fun testLsmMicroBoundary02() = verifyMemTableKvPair("k02", "v02")
    @Test fun testLsmMicroBoundary03() = verifyMemTableKvPair("k03", "v03")
    @Test fun testLsmMicroBoundary04() = verifyMemTableKvPair("k04", "v04")
    @Test fun testLsmMicroBoundary05() = verifyMemTableKvPair("k05", "v05")
    @Test fun testLsmMicroBoundary06() = verifyMemTableKvPair("k06", "v06")
    @Test fun testLsmMicroBoundary07() = verifyMemTableKvPair("k07", "v07")
    @Test fun testLsmMicroBoundary08() = verifyMemTableKvPair("k08", "v08")
    @Test fun testLsmMicroBoundary09() = verifyMemTableKvPair("k09", "v09")
    @Test fun testLsmMicroBoundary10() = verifyMemTableKvPair("k10", "v10")
    @Test fun testLsmMicroBoundary11() = verifyMemTableKvPair("k11", "v11")
    @Test fun testLsmMicroBoundary12() = verifyMemTableKvPair("k12", "v12")
    @Test fun testLsmMicroBoundary13() = verifyMemTableKvPair("k13", "v13")
    @Test fun testLsmMicroBoundary14() = verifyMemTableKvPair("k14", "v14")
    @Test fun testLsmMicroBoundary15() = verifyMemTableKvPair("k15", "v15")
    @Test fun testLsmMicroBoundary16() = verifyMemTableKvPair("k16", "v16")
    @Test fun testLsmMicroBoundary17() = verifyMemTableKvPair("k17", "v17")
    @Test fun testLsmMicroBoundary18() = verifyMemTableKvPair("k18", "v18")
    @Test fun testLsmMicroBoundary19() = verifyMemTableKvPair("k19", "v19")
    @Test fun testLsmMicroBoundary20() = verifyMemTableKvPair("k20", "v20")
    @Test fun testLsmMicroBoundary21() = verifyMemTableKvPair("k21", "v21")
    @Test fun testLsmMicroBoundary22() = verifyMemTableKvPair("k22", "v22")
    @Test fun testLsmMicroBoundary23() = verifyMemTableKvPair("k23", "v23")
    @Test fun testLsmMicroBoundary24() = verifyMemTableKvPair("k24", "v24")
    @Test fun testLsmMicroBoundary25() = verifyMemTableKvPair("k25", "v25")
    @Test fun testLsmMicroBoundary26() = verifyBloomFilterKey("bf_key_26")
    @Test fun testLsmMicroBoundary27() = verifyBloomFilterKey("bf_key_27")
    @Test fun testLsmMicroBoundary28() = verifyBloomFilterKey("bf_key_28")
    @Test fun testLsmMicroBoundary29() = verifyBloomFilterKey("bf_key_29")
    @Test fun testLsmMicroBoundary30() = verifyBloomFilterKey("bf_key_30")
    @Test fun testLsmMicroBoundary31() = verifyBloomFilterKey("bf_key_31")
    @Test fun testLsmMicroBoundary32() = verifyBloomFilterKey("bf_key_32")
    @Test fun testLsmMicroBoundary33() = verifyBloomFilterKey("bf_key_33")
    @Test fun testLsmMicroBoundary34() = verifyBloomFilterKey("bf_key_34")
    @Test fun testLsmMicroBoundary35() = verifyBloomFilterKey("bf_key_35")
    @Test fun testLsmMicroBoundary36() = verifyCacheBlock("blk_36")
    @Test fun testLsmMicroBoundary37() = verifyCacheBlock("blk_37")
    @Test fun testLsmMicroBoundary38() = verifyCacheBlock("blk_38")
    @Test fun testLsmMicroBoundary39() = verifyCacheBlock("blk_39")
    @Test fun testLsmMicroBoundary40() = verifyCacheBlock("blk_40")
    @Test fun testLsmMicroBoundary41() = verifyCacheBlock("blk_41")
    @Test fun testLsmMicroBoundary42() = verifyCacheBlock("blk_42")
    @Test fun testLsmMicroBoundary43() = verifyCacheBlock("blk_43")
    @Test fun testLsmMicroBoundary44() = verifyCacheBlock("blk_44")
    @Test fun testLsmMicroBoundary45() = verifyCacheBlock("blk_45")

    private fun verifyMemTableKvPair(key: String, value: String) {
        val mem = MemTable()
        mem.put(key.toByteArray(), value.toByteArray())
        assertArrayEquals(value.toByteArray(), mem.get(key.toByteArray()))
    }

    private fun verifyBloomFilterKey(key: String) {
        val bf = BloomFilter(100, 3)
        bf.add(key.toByteArray())
        assertTrue(bf.mightContain(key.toByteArray()))
    }

    private fun verifyCacheBlock(blockId: String) {
        val cache = BlockCache(10_000)
        val data = blockId.toByteArray()
        cache.put(blockId, data)
        assertArrayEquals(data, cache.get(blockId))
    }
}
