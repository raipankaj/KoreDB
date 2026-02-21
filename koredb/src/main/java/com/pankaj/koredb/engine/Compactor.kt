package com.pankaj.koredb.engine

import com.pankaj.koredb.foundation.ByteArrayComparator
import com.pankaj.koredb.foundation.MemTable
import com.pankaj.koredb.foundation.SSTable
import com.pankaj.koredb.foundation.SSTableIterator
import com.pankaj.koredb.foundation.SSTableReader
import java.io.File
import java.util.PriorityQueue

object Compactor {

    /**
     * Merges multiple SSTables into a single, clean SSTable.
     * Removes duplicates (keeps newest) and removes Tombstones.
     */
    fun compact(
        readers: List<SSTableReader>,
        outputFile: File
    ) {
        // 1. Create Iterators for every file
        val queue = PriorityQueue<SSTableIterator>()
        readers.forEachIndexed { index, reader ->
            val iterator = SSTableIterator(reader, fileIndex = index)
            if (iterator.currentKey != null) {
                queue.add(iterator)
            }
        }

        // 2. Prepare the Writer (using a temporary MemTable logic for simplicity, 
        //    but properly streaming via a temp file writer is better. 
        //    Here we reuse SSTable.writeFromMemTable logic but manually).
        
        // We will stream-write directly to the output file to avoid OOM
        val tempMemTable = MemTable()
        // Note: For a true 100GB DB, we would write directly to FileChannel.
        // For this Enterprise implementation, buffering into a fresh MemTable 
        // and then flushing is a safe, easy way to reuse our writing logic.

        var lastProcessedKey: ByteArray? = null

        while (queue.isNotEmpty()) {
            // Get the iterator with the smallest key (and newest version)
            val topIterator = queue.poll()
            val candidateKey = topIterator.currentKey!!
            val candidateValue = topIterator.currentValue!!

            // --- DEDUPLICATION LOGIC ---
            // Because our PriorityQueue sorts by Newest File Index when keys are equal,
            // the first time we see a key, it is GUARANTEED to be the latest version.
            // We process it, and then ignore any subsequent appearances of this key.

            val isNewKey = lastProcessedKey == null || ByteArrayComparator.compare(candidateKey, lastProcessedKey) != 0
            
            if (isNewKey) {
                // This is the latest version of this key.
                lastProcessedKey = candidateKey

                // --- TOMBSTONE LOGIC ---
                // If the value is empty, it's a delete. We do NOT write it. Space reclaimed!
                if (candidateValue.isNotEmpty()) {
                    tempMemTable.put(candidateKey, candidateValue)
                }
            }

            // Advance this iterator and put it back in the queue if it has more data
            if (topIterator.advance()) {
                queue.add(topIterator)
            }
        }

        // 3. Write the clean, merged data to disk
        SSTable.writeFromMemTable(tempMemTable, outputFile)
    }
}