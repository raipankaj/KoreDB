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

package com.pankaj.koredb.stream

import com.pankaj.koredb.engine.KoreDB
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.withContext
import java.util.UUID

/**
 * An append-only event stream engine built on KoreDB's LSM tree.
 * Allows for publishing events and subscribing to reactive streams.
 * Useful for Event Sourcing architectures and Pub/Sub.
 */
class KoreEventStream(
    private val topicName: String,
    private val engine: KoreDB
) {
    private val prefix = "stream_$topicName:".toByteArray(Charsets.UTF_8)
    private val eventFlow = MutableSharedFlow<Event>(extraBufferCapacity = 100)

    data class Event(val id: String, val timestamp: Long, val payload: ByteArray) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Event
            if (id != other.id) return false
            if (timestamp != other.timestamp) return false
            if (!payload.contentEquals(other.payload)) return false
            return true
        }
        override fun hashCode(): Int {
            var result = id.hashCode()
            result = 31 * result + timestamp.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }
    }

    private fun escape(value: String): String {
        return value.replace("%", "%25").replace(":", "%3A")
    }

    private fun unescape(value: String): String {
        return value.replace("%3A", ":").replace("%25", "%")
    }

    private fun constructKey(timestamp: Long, id: String): ByteArray {
        // Construct an lexicographically sortable key: prefix + timestamp + id
        val timeString = timestamp.toString().padStart(20, '0')
        val keyString = "$timeString:${escape(id)}"
        val keyBytes = keyString.toByteArray(Charsets.UTF_8)
        
        val fullKey = ByteArray(prefix.size + keyBytes.size)
        System.arraycopy(prefix, 0, fullKey, 0, prefix.size)
        System.arraycopy(keyBytes, 0, fullKey, prefix.size, keyBytes.size)
        return fullKey
    }

    /**
     * Publishes an event to the stream and persists it to disk.
     */
    suspend fun publish(payload: ByteArray): Event = withContext(Dispatchers.IO) {
        val timestamp = System.currentTimeMillis()
        val id = UUID.randomUUID().toString()
        val key = constructKey(timestamp, id)
        
        engine.putRaw(key, payload)
        
        val event = Event(id, timestamp, payload)
        eventFlow.tryEmit(event) // Notify subscribers
        return@withContext event
    }

    /**
     * Publishes a String event to the stream.
     */
    suspend fun publishString(message: String): Event {
        return publish(message.toByteArray(Charsets.UTF_8))
    }

    /**
     * Returns a reactive flow of incoming events as they happen.
     */
    fun subscribe(): Flow<Event> {
        return eventFlow.asSharedFlow()
    }

    /**
     * Retrieves historical events chronologically in a single O(N) pass.
     * LSM-tree keys with zero-padded timestamps are naturally sorted.
     */
    fun getHistory(): List<Event> {
        val rawPairs = engine.getByPrefixWithKeysRaw(prefix)
        return rawPairs.mapNotNull { (keyBytes, value) ->
            try {
                // Key format: prefix + timestamp(20 chars) + ":" + id
                val tsStart = prefix.size
                val tsEnd = tsStart + 20
                if (keyBytes.size > tsEnd && keyBytes[tsEnd] == ':'.code.toByte()) {
                    val timestampStr = String(keyBytes, tsStart, 20, Charsets.UTF_8)
                    val timestamp = timestampStr.toLong()
                    
                    val idStart = tsEnd + 1
                    val idStr = unescape(String(keyBytes, idStart, keyBytes.size - idStart, Charsets.UTF_8))
                    Event(idStr, timestamp, value)
                } else null
            } catch (e: Exception) {
                null
            }
        }
    }
}
