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

package com.pankaj.koredb.cdc

import com.pankaj.koredb.engine.KoreDB
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

/**
 * Enterprise Change Data Capture (CDC) and delta replication manager.
 *
 * Records all collection mutations to an append-only sequence log, enabling
 * offline-first mobile sync engines (e.g. Supabase, ElectricSQL, CouchDB, custom sync)
 * to query local changes since the last sync timestamp and prune acknowledged mutations.
 */
class CdcManager(
    private val engine: KoreDB,
    @Volatile var enabled: Boolean = true
) {
    private val listeners = CopyOnWriteArrayList<MutationListener>()
    private val sequenceCounter = AtomicLong(0)
    private val json = Json { ignoreUnknownKeys = true }

    companion object {
        private val CDC_PREFIX = "cdc:rec:".toByteArray(Charsets.UTF_8)
        private val NEXT_SEQ_KEY = "cdc:meta:seq".toByteArray(Charsets.UTF_8)

        private fun formatSequence(seq: Long): String = String.format("%019d", seq)
        private fun makeRecordKey(seq: Long): ByteArray = "cdc:rec:${formatSequence(seq)}".toByteArray(Charsets.UTF_8)
    }

    init {
        // Hydrate last sequence number from disk
        try {
            val rawSeq = engine.getRaw(NEXT_SEQ_KEY)
            if (rawSeq != null) {
                val seq = String(rawSeq, Charsets.UTF_8).toLongOrNull() ?: 0L
                sequenceCounter.set(seq)
            }
        } catch (_: Exception) {
            // Gracefully ignore decryption or read failures during startup
        }
    }

    /**
     * Registers a listener for real-time mutation streaming.
     */
    fun registerListener(listener: MutationListener) {
        listeners.add(listener)
    }

    /**
     * Unregisters a previously registered listener.
     */
    fun unregisterListener(listener: MutationListener) {
        listeners.remove(listener)
    }

    /**
     * Records a mutation into the CDC log and notifies real-time listeners.
     */
    fun recordMutation(
        collection: String,
        documentId: String,
        operation: MutationOp,
        payload: ByteArray? = null,
        timestamp: Long = System.currentTimeMillis()
    ) {
        if (!enabled) return

        val seq = sequenceCounter.incrementAndGet()
        val record = MutationRecord(
            sequence = seq,
            collection = collection,
            documentId = documentId,
            operation = operation,
            timestamp = timestamp,
            payload = payload
        )

        val recordBytes = json.encodeToString(MutationRecord.serializer(), record).toByteArray(Charsets.UTF_8)
        val recKey = makeRecordKey(seq)
        val seqBytes = seq.toString().toByteArray(Charsets.UTF_8)

        runBlocking {
            engine.writeBatchRaw(listOf(
                recKey to recordBytes,
                NEXT_SEQ_KEY to seqBytes
            ))
        }

        for (listener in listeners) {
            try {
                listener.onMutation(record)
            } catch (_: Throwable) {}
        }
    }

    /**
     * Records a batch of mutations atomically in a single write batch.
     */
    fun recordMutationsBatch(
        collection: String,
        documentIds: List<String>,
        operation: MutationOp,
        timestamp: Long = System.currentTimeMillis()
    ) {
        if (!enabled || documentIds.isEmpty()) return

        val batch = ArrayList<Pair<ByteArray, ByteArray>>(documentIds.size + 1)
        val records = ArrayList<MutationRecord>(documentIds.size)
        var lastSeq = 0L

        for (docId in documentIds) {
            val seq = sequenceCounter.incrementAndGet()
            lastSeq = seq
            val record = MutationRecord(
                sequence = seq,
                collection = collection,
                documentId = docId,
                operation = operation,
                timestamp = timestamp,
                payload = null
            )
            records.add(record)
            val recordBytes = json.encodeToString(MutationRecord.serializer(), record).toByteArray(Charsets.UTF_8)
            batch.add(makeRecordKey(seq) to recordBytes)
        }

        val seqBytes = lastSeq.toString().toByteArray(Charsets.UTF_8)
        batch.add(NEXT_SEQ_KEY to seqBytes)

        runBlocking {
            engine.writeBatchRaw(batch)
        }

        if (listeners.isNotEmpty()) {
            for (listener in listeners) {
                for (record in records) {
                    try {
                        listener.onMutation(record)
                    } catch (_: Throwable) {}
                }
            }
        }
    }

    /**
     * Retrieves all mutations recorded at or after [sinceTimestamp], ordered chronologically.
     *
     * @param sinceTimestamp Epoch millisecond timestamp filter.
     * @param limit Maximum number of records to return.
     * @return List of matching [MutationRecord] instances.
     */
    fun getMutationsSince(sinceTimestamp: Long, limit: Int = 1000): List<MutationRecord> {
        val rawResults = engine.getByPrefixWithKeysRaw(CDC_PREFIX)
        val results = mutableListOf<MutationRecord>()

        for ((_, valBytes) in rawResults) {
            try {
                val recordStr = String(valBytes, Charsets.UTF_8)
                val record = json.decodeFromString(MutationRecord.serializer(), recordStr)
                if (record.timestamp >= sinceTimestamp) {
                    results.add(record)
                    if (results.size >= limit) break
                }
            } catch (_: Exception) {}
        }

        return results.sortedBy { it.sequence }
    }

    /**
     * Prunes acknowledged mutations up to [upToSequence] to free disk space.
     *
     * @param upToSequence The highest sequence number successfully synchronized to the cloud.
     */
    fun acknowledgeMutations(upToSequence: Long) {
        val rawResults = engine.getByPrefixWithKeysRaw(CDC_PREFIX)
        val toDelete = mutableListOf<Pair<ByteArray, ByteArray>>()

        for ((keyBytes, _) in rawResults) {
            val keyStr = String(keyBytes, Charsets.UTF_8)
            val seqStr = keyStr.removePrefix("cdc:rec:")
            val seq = seqStr.toLongOrNull() ?: continue
            if (seq <= upToSequence) {
                toDelete.add(keyBytes to KoreDB.TOMBSTONE)
            }
        }

        if (toDelete.isNotEmpty()) {
            runBlocking {
                engine.writeBatchRaw(toDelete)
            }
        }
    }

    /**
     * Returns the latest generated mutation sequence number.
     */
    fun getLatestSequence(): Long = sequenceCounter.get()
}
