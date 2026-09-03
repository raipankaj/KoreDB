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

import kotlinx.serialization.Serializable

/**
 * Mutation operation type for Change Data Capture (CDC).
 */
@Serializable
enum class MutationOp {
    INSERT,
    UPDATE,
    DELETE
}

/**
 * Represents a single database mutation event for offline-first delta synchronization.
 *
 * @property sequence Monotonically increasing sequence number.
 * @property collection Name of the collection modified.
 * @property documentId Unique document identifier.
 * @property operation Operation performed (INSERT, UPDATE, or DELETE).
 * @property timestamp Epoch millisecond timestamp of the mutation.
 * @property payload Document data bytes (null for DELETE operations).
 */
@Serializable
data class MutationRecord(
    val sequence: Long,
    val collection: String,
    val documentId: String,
    val operation: MutationOp,
    val timestamp: Long,
    val payload: ByteArray? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MutationRecord
        if (sequence != other.sequence) return false
        if (collection != other.collection) return false
        if (documentId != other.documentId) return false
        if (operation != other.operation) return false
        if (timestamp != other.timestamp) return false
        if (payload != null) {
            if (other.payload == null) return false
            if (!payload.contentEquals(other.payload)) return false
        } else if (other.payload != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = sequence.hashCode()
        result = 31 * result + collection.hashCode()
        result = 31 * result + documentId.hashCode()
        result = 31 * result + operation.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + (payload?.contentHashCode() ?: 0)
        return result
    }
}

/**
 * Listener interface for streaming mutations in real time to network sync workers.
 */
fun interface MutationListener {
    fun onMutation(record: MutationRecord)
}
