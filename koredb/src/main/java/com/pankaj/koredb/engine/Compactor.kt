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

import com.pankaj.koredb.foundation.ByteArrayComparator
import com.pankaj.koredb.foundation.SSTable
import com.pankaj.koredb.foundation.SSTableIterator
import com.pankaj.koredb.foundation.SSTableReader
import java.io.File
import java.util.PriorityQueue

/**
 * Pluggable filter invoked during SSTable compaction to drop expired or tombstoned entries.
 */
fun interface CompactionFilter {
    /**
     * Inspects a candidate key-value pair during LSM K-way merge.
     * Returns true if the entry should be physically purged from the output SSTable.
     */
    fun shouldDrop(key: ByteArray, value: ByteArray, nowMs: Long): Boolean
}

/**
 * Pluggable [CompactionFilter] that drops expired bi-temporal facts and edges
 * whose validUntilMs is older than [retentionHorizonMs] relative to compaction time.
 */
class BiTemporalCompactionFilter(
    val retentionHorizonMs: Long = 90L * 24 * 3600 * 1000 // 90 days default
) : CompactionFilter {
    override fun shouldDrop(key: ByteArray, value: ByteArray, nowMs: Long): Boolean {
        if (value.isEmpty()) return false
        val keyStr = runCatching { String(key, Charsets.UTF_8) }.getOrNull() ?: return false
        if (keyStr.startsWith("g:v:") || keyStr.startsWith("doc:") || keyStr.startsWith("g:e:")) {
            val valStr = runCatching { String(value, Charsets.UTF_8) }.getOrNull() ?: return false
            val match = Regex("\"(?:_validUntilMs|validUntilMs)\"\\s*:\\s*\"?(\\d+)\"?").find(valStr)
            val untilMs = match?.groupValues?.get(1)?.toLongOrNull()
            if (untilMs != null && untilMs < nowMs - retentionHorizonMs) {
                return true
            }
        }
        return false
    }
}

/**
 * Handles the merging and cleaning of multiple SSTable segments.
 *
 * Compaction is a critical background process in LSM-Trees that maintains performance
 * and reclaims disk space by:
 * 1. **Merging:** Combining multiple sorted segments into a single new segment.
 * 2. **Deduplication:** Keeping only the newest version of a key and discarding older ones.
 * 3. **Tombstone Removal:** Physically deleting records that were marked for deletion, 
 *    thus reclaiming space.
 */
object Compactor {

    private val IDX_PREFIX_BYTES = "idx:".toByteArray(Charsets.UTF_8)
    private val GRAPH_IDX_PREFIX_BYTES = "g:idx:v_prop:".toByteArray(Charsets.UTF_8)

    private fun startsWith(array: ByteArray, prefix: ByteArray): Boolean {
        if (array.size < prefix.size) return false
        for (i in prefix.indices) {
            if (array[i] != prefix[i]) return false
        }
        return true
    }

    /**
     * Merges multiple SSTables into a single, clean SSTable.
     *
     * This implementation uses a multi-way merge algorithm with a [PriorityQueue]
     * to efficiently process sorted data from multiple readers in a streaming fashion,
     * writing directly to disk without intermediate in-memory MemTable accumulation.
     *
     * @param readers A list of [SSTableReader]s for the segments to be compacted.
     * @param outputFile The destination file for the new, compacted SSTable.
     * @param truthOracle An optional function to validate if an index entry is still fresh.
     * @param compressionCodec Codec for output payload compression.
     * @param priorities Optional explicit priorities for readers (higher priority = newer data).
     * @param dropTombstones Whether to drop tombstones during merge.
     * @param compactionFilter Optional pluggable filter to evaluate candidate key-value pairs.
     */
    fun compact(
        readers: List<SSTableReader>,
        outputFile: File,
        truthOracle: ((ByteArray) -> ByteArray?)? = null,
        compressionCodec: com.pankaj.koredb.compression.CompressionCodec = com.pankaj.koredb.compression.NoOpCompressionCodec,
        priorities: List<Int>? = null,
        dropTombstones: Boolean = true,
        compactionFilter: CompactionFilter? = null
    ) {
        // 1. Initialize Iterators for all input files with level-aware priorities
        val queue = PriorityQueue<SSTableIterator>()
        readers.forEachIndexed { index, reader ->
            val p = priorities?.getOrNull(index) ?: ((10 - reader.level) * 1_000_000 + index)
            val iterator = SSTableIterator(reader, priority = p)
            if (iterator.currentKey != null) {
                queue.add(iterator)
            }
        }

        // 2. Streaming K-way merge sequence without intermediate MemTable
        val mergedSequence = sequence {
            var lastProcessedKey: ByteArray? = null

            while (queue.isNotEmpty()) {
                val topIterator = queue.poll()!!
                val candidateKey = topIterator.currentKey!!

                val isNewKey = lastProcessedKey == null || ByteArrayComparator.compare(candidateKey, lastProcessedKey) != 0

                if (isNewKey) {
                    lastProcessedKey = candidateKey
                    val candidateValue = topIterator.value() ?: KoreDB.TOMBSTONE

                    if (candidateValue.isNotEmpty()) {
                        // --- INDEX-AWARE COMPACTION & COMPACTION FILTER ---
                        var shouldDrop = false
                        val nowMs = System.currentTimeMillis()

                        if (compactionFilter != null && compactionFilter.shouldDrop(candidateKey, candidateValue, nowMs)) {
                            shouldDrop = true
                        }

                        if (!shouldDrop && truthOracle != null) {
                            if (startsWith(candidateKey, IDX_PREFIX_BYTES)) {
                                val colonByte = ':'.code.toByte()
                                var c1 = -1
                                var c2 = -1
                                var cLast = -1
                                var colonCount = 0
                                for (ci in candidateKey.indices) {
                                    if (candidateKey[ci] == colonByte) {
                                        colonCount++
                                        if (colonCount == 1) c1 = ci
                                        else if (colonCount == 2) c2 = ci
                                        cLast = ci
                                    }
                                }
                                if (colonCount >= 4 && c1 != -1 && c2 != -1 && cLast != -1) {
                                    var endId = candidateKey.size
                                    while (endId > cLast + 1 && candidateKey[endId - 1] == 0.toByte()) {
                                        endId--
                                    }
                                    val collLen = c2 - (c1 + 1)
                                    val idLen = endId - (cLast + 1)
                                    if (collLen > 0 && idLen > 0) {
                                        val docKey = ByteArray(4 + collLen + 1 + idLen)
                                        docKey[0] = 'd'.code.toByte()
                                        docKey[1] = 'o'.code.toByte()
                                        docKey[2] = 'c'.code.toByte()
                                        docKey[3] = ':'.code.toByte()
                                        System.arraycopy(candidateKey, c1 + 1, docKey, 4, collLen)
                                        docKey[4 + collLen] = ':'.code.toByte()
                                        System.arraycopy(candidateKey, cLast + 1, docKey, 5 + collLen, idLen)

                                        val docBytes = truthOracle(docKey)
                                        if (docBytes == null || docBytes.isEmpty()) {
                                            shouldDrop = true // Document deleted: purge index entry
                                        }
                                    }
                                }
                            } else if (startsWith(candidateKey, GRAPH_IDX_PREFIX_BYTES)) {
                                val keyStr = String(candidateKey, Charsets.UTF_8)
                                val parts = keyStr.split(":")
                                if (parts.size >= 7) {
                                    val label = parts[3]
                                    val key = parts[4]
                                    val value = parts[5]
                                    val nodeId = parts[6]

                                    val rptrKey = "g:rptr:v_prop:$label:$key:$nodeId".toByteArray(Charsets.UTF_8)
                                    val currentTruth = truthOracle(rptrKey)?.let { String(it, Charsets.UTF_8) }

                                    if (currentTruth != null && currentTruth != value) {
                                        shouldDrop = true
                                    }
                                }
                            }
                        }

                        if (!shouldDrop) {
                            yield(Pair(candidateKey, candidateValue))
                        }
                    } else if (!dropTombstones) {
                        // Preserve tombstone when deeper SSTable levels exist to prevent deleted data resurrection
                        val nowMs = System.currentTimeMillis()
                        val filterDrop = compactionFilter != null && compactionFilter.shouldDrop(candidateKey, candidateValue, nowMs)
                        if (!filterDrop) {
                            yield(Pair(candidateKey, candidateValue))
                        }
                    }
                }

                if (topIterator.advance()) {
                    queue.add(topIterator)
                }
            }
        }

        // 3. Persist the merged, deduplicated stream directly to disk
        SSTable.writeSortedEntries(mergedSequence, outputFile, compressionCodec)
    }
}
