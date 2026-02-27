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

package com.pankaj.koredb.graph

import com.pankaj.koredb.engine.KoreDB
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/**
 * Provides an ACID-compliant transaction scope for high-level graph operations.
 *
 * [GraphTransaction] buffers node and edge mutations in memory, ensuring that 
 * complex graph updates (such as dual-writing edges and updating indices) are 
 * applied to the underlying LSM-tree as a single atomic unit.
 *
 * @property db The underlying [KoreDB] engine where mutations are persisted.
 */
class GraphTransaction(private val db: KoreDB) {

    private val json = Json { ignoreUnknownKeys = true }
    private val PRESENCE_MARKER = ByteArray(1) { 1 }
    
    /**
     * The internal batch buffer storing pending key-value updates.
     */
    private val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
    
    private var isCommitted = false
    private var isRolledBack = false

    /**
     * Buffers a [Node] for insertion or update, including all secondary indices.
     */
    fun putNode(node: Node) {
        checkState()
        val nodeKey = "g:v:${node.id}".toByteArray(Charsets.UTF_8)
        val nodeValue = json.encodeToString(node).toByteArray(Charsets.UTF_8)
        
        batch.add(Pair(nodeKey, nodeValue))
        
        for (label in node.labels) {
            val labelKey = "g:idx:v:$label:${node.id}".toByteArray(Charsets.UTF_8)
            batch.add(Pair(labelKey, PRESENCE_MARKER))
            
            for ((key, value) in node.properties) {
                val propKey = "g:idx:v_prop:$label:$key:$value:${node.id}".toByteArray(Charsets.UTF_8)
                batch.add(Pair(propKey, PRESENCE_MARKER))
            }
        }
    }

    /**
     * Buffers an [Edge] for insertion or update, maintaining bidirectional relationship records.
     */
    fun putEdge(edge: Edge) {
        checkState()
        val edgeValue = json.encodeToString(edge).toByteArray(Charsets.UTF_8)
        
        val outKey = "g:e:out:${edge.sourceId}:${edge.type}:${edge.targetId}".toByteArray(Charsets.UTF_8)
        val inKey  = "g:e:in:${edge.targetId}:${edge.type}:${edge.sourceId}".toByteArray(Charsets.UTF_8)
        
        batch.add(Pair(outKey, edgeValue))
        batch.add(Pair(inKey, edgeValue))
        
        for ((key, value) in edge.properties) {
             val propKey = "g:idx:e_prop:${edge.type}:$key:$value:${edge.sourceId}:${edge.targetId}".toByteArray(Charsets.UTF_8)
             batch.add(Pair(propKey, PRESENCE_MARKER))
        }
    }

    /**
     * Commits all buffered mutations atomically to the underlying storage.
     *
     * This operation writes the batch to the Write-Ahead Log (WAL) and the 
     * MemTable in a single step, guaranteeing atomicity and durability.
     *
     * @param urgent If true, forces a hardware-level fsync of the WAL.
     */
    suspend fun commit(urgent: Boolean = false) {
        checkState()
        if (batch.isEmpty()) return
        
        db.writeBatchRaw(batch, urgent)
        isCommitted = true
    }

    /**
     * Discards all pending mutations and prevents further operations on this transaction.
     */
    fun rollback() {
        checkState()
        batch.clear()
        isRolledBack = true
    }

    /**
     * Verifies that the transaction is still active.
     */
    private fun checkState() {
        if (isCommitted) throw IllegalStateException("Transaction already committed.")
        if (isRolledBack) throw IllegalStateException("Transaction already rolled back.")
    }
}
