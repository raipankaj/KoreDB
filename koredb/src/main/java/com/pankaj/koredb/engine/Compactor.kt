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
import com.pankaj.koredb.foundation.MemTable
import com.pankaj.koredb.foundation.SSTable
import com.pankaj.koredb.foundation.SSTableIterator
import com.pankaj.koredb.foundation.SSTableReader
import java.io.File
import java.util.PriorityQueue

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

    /**
     * Merges multiple SSTables into a single, clean SSTable.
     *
     * This implementation uses a multi-way merge algorithm with a [PriorityQueue]
     * to efficiently process sorted data from multiple readers.
     *
     * @param readers A list of [SSTableReader]s for the segments to be compacted.
     * @param outputFile The destination file for the new, compacted SSTable.
     */
    fun compact(
        readers: List<SSTableReader>,
        outputFile: File
    ) {
        // 1. Initialize Iterators for all input files
        val queue = PriorityQueue<SSTableIterator>()
        readers.forEachIndexed { index, reader ->
            val iterator = SSTableIterator(reader, fileIndex = index)
            if (iterator.currentKey != null) {
                queue.add(iterator)
            }
        }

        // 2. Process and Merge
        // For simplicity, we aggregate the compacted data into a fresh MemTable 
        // before flushing. In an extremely large-scale scenario, we would stream
        // directly to a FileChannel.
        val tempMemTable = MemTable()
        var lastProcessedKey: ByteArray? = null

        while (queue.isNotEmpty()) {
            // Retrieve the iterator with the smallest key (and newest version)
            val topIterator = queue.poll()
            val candidateKey = topIterator.currentKey!!
            val candidateValue = topIterator.currentValue!!

            // Deduplication: The PriorityQueue ensures we see the newest version of a key first.
            // Any subsequent appearances of the same key in older segments are ignored.
            val isNewKey = lastProcessedKey == null || ByteArrayComparator.compare(candidateKey, lastProcessedKey) != 0
            
            if (isNewKey) {
                lastProcessedKey = candidateKey

                // Tombstone Logic: If the value is empty, it represents a deletion.
                // We omit it from the new segment to reclaim disk space.
                if (candidateValue.isNotEmpty()) {
                    tempMemTable.put(candidateKey, candidateValue)
                }
            }

            // Advance the iterator and re-insert into the queue if more data is available
            if (topIterator.advance()) {
                queue.add(topIterator)
            }
        }

        // 3. Persist the merged, deduplicated data to disk
        SSTable.writeFromMemTable(tempMemTable, outputFile)
    }
}
