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
import kotlinx.serialization.json.Json

/**
 * A specialized storage engine for property graphs, implemented on top of the KoreDB LSM-tree.
 *
 * [GraphStorage] manages the persistence of nodes and edges, maintaining multiple indices 
 * to support high-performance graph traversals and property-based lookups.
 *
 * ### Storage Schema:
 * - **Nodes**: `g:v:{nodeId}` -> JSON representation of [Node].
 * - **Node Index (Label)**: `g:idx:v:{label}:{nodeId}` -> Presence Marker.
 * - **Node Index (Property)**: `g:idx:v_prop:{label}:{key}:{value}:{nodeId}` -> Presence Marker.
 * - **Edges (Outgoing)**: `g:e:out:{sourceId}:{type}:{targetId}` -> JSON representation of [Edge].
 * - **Edges (Incoming)**: `g:e:in:{targetId}:{type}:{sourceId}` -> JSON representation of [Edge].
 * - **Edge Index (Property)**: `g:idx:e_prop:{type}:{key}:{value}:{sourceId}:{targetId}` -> Presence Marker.
 *
 * @property db The underlying [KoreDB] instance used for atomic key-value storage.
 */
class GraphStorage(val db: KoreDB) {

    private val json = Json { ignoreUnknownKeys = true }
    
    /**
     * A non-empty byte array used to indicate entry existence in secondary indices 
     * without using a zero-length array (which represents a tombstone/deletion).
     */
    private val PRESENCE_MARKER = ByteArray(1) { 1 }

    // --- NODE OPERATIONS ---

    /**
     * Persists a [Node] and atomically updates its associated label and property indices.
     *
     * @param node The node object to store.
     */
    suspend fun putNode(node: Node) {
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        
        val nodeKey = "g:v:${node.id}".toByteArray(Charsets.UTF_8)
        val nodeValue = json.encodeToString(node).toByteArray(Charsets.UTF_8)
        
        batch.add(Pair(nodeKey, nodeValue))
        
        for (label in node.labels) {
            // Index nodes by their labels for category-based scans.
            val labelKey = "g:idx:v:$label:${node.id}".toByteArray(Charsets.UTF_8)
            batch.add(Pair(labelKey, PRESENCE_MARKER))
            
            // Build indices for equality-based property lookups.
            for ((key, value) in node.properties) {
                val propKey = "g:idx:v_prop:$label:$key:$value:${node.id}".toByteArray(Charsets.UTF_8)
                batch.add(Pair(propKey, PRESENCE_MARKER))
            }
        }
        
        db.writeBatchRaw(batch)
    }

    /**
     * Retrieves a [Node] by its unique identifier.
     *
     * @param id The ID of the node to fetch.
     * @return The [Node] object, or null if it does not exist.
     */
    fun getNode(id: String): Node? {
        val bytes = db.getRaw("g:v:$id".toByteArray(Charsets.UTF_8)) ?: return null
        if (bytes.isEmpty()) return null
        return json.decodeFromString<Node>(String(bytes, Charsets.UTF_8))
    }

    /**
     * Efficiently retrieves nodes matching a specific label and property value.
     *
     * This operation performs an O(log N) seek on the property index followed by 
     * sequential reads for matching IDs.
     *
     * @param label The label category to filter by.
     * @param propertyKey The property name to match.
     * @param propertyValue The property value to match.
     * @return A list of matching [Node] objects.
     */
    fun getNodesByProperty(label: String, propertyKey: String, propertyValue: String): List<Node> {
        val prefix = "g:idx:v_prop:$label:$propertyKey:$propertyValue:".toByteArray(Charsets.UTF_8)
        
        val indexKeys = db.getKeysByPrefixRaw(prefix)
        return indexKeys.mapNotNull { keyBytes ->
            val keyString = String(keyBytes, Charsets.UTF_8)
            val nodeId = keyString.substringAfterLast(":")
            getNode(nodeId)
        }
    }

    // --- EDGE OPERATIONS ---

    /**
     * Persists an [Edge] using an atomic dual-write strategy.
     * 
     * This ensures that the relationship is indexed for both forward (outbound) 
     * and reverse (inbound) traversals with O(1) disk access.
     *
     * @param edge The edge object representing the relationship.
     */
    suspend fun putEdge(edge: Edge) {
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        val edgeValue = json.encodeToString(edge).toByteArray(Charsets.UTF_8)
        
        // Write the edge in both directions to support bidirectional traversals.
        val outKey = "g:e:out:${edge.sourceId}:${edge.type}:${edge.targetId}".toByteArray(Charsets.UTF_8)
        val inKey  = "g:e:in:${edge.targetId}:${edge.type}:${edge.sourceId}".toByteArray(Charsets.UTF_8)
        
        batch.add(Pair(outKey, edgeValue))
        batch.add(Pair(inKey, edgeValue))
        
        // Maintain property indices for edges to support weighted traversal queries.
        for ((key, value) in edge.properties) {
             val propKey = "g:idx:e_prop:${edge.type}:$key:$value:${edge.sourceId}:${edge.targetId}".toByteArray(Charsets.UTF_8)
             batch.add(Pair(propKey, PRESENCE_MARKER))
        }
        
        db.writeBatchRaw(batch)
    }

    /**
     * Atomically removes an edge and its bidirectional index entries.
     */
    suspend fun removeEdge(sourceId: String, type: String, targetId: String) {
        val outKey = "g:e:out:$sourceId:$type:$targetId".toByteArray(Charsets.UTF_8)
        val inKey  = "g:e:in:$targetId:$type:$sourceId".toByteArray(Charsets.UTF_8)
        
        db.writeBatchRaw(listOf(
            outKey to KoreDB.TOMBSTONE,
            inKey to KoreDB.TOMBSTONE
        ))
    }

    /**
     * Retrieves all outgoing relationships of a specific type from a node.
     */
    fun getOutboundEdges(sourceId: String, type: String): List<Edge> {
        val prefix = "g:e:out:$sourceId:$type:".toByteArray(Charsets.UTF_8)
        val rawValues = db.getByPrefixRaw(prefix)
        return rawValues.map { json.decodeFromString<Edge>(String(it, Charsets.UTF_8)) }
    }

    /**
     * Retrieves all incoming relationships of a specific type to a node.
     */
    fun getInboundEdges(targetId: String, type: String): List<Edge> {
        val prefix = "g:e:in:$targetId:$type:".toByteArray(Charsets.UTF_8)
        val rawValues = db.getByPrefixRaw(prefix)
        return rawValues.map { json.decodeFromString<Edge>(String(it, Charsets.UTF_8)) }
    }

    /**
     * Executes a series of graph operations within an atomic transaction.
     * 
     * Failures within the [block] will result in an automatic rollback of 
     * all pending changes.
     *
     * @param urgent If true, forces a hardware-level sync on commit.
     * @param block The transactional logic to execute.
     */
    suspend fun transaction(urgent: Boolean = false, block: GraphTransaction.() -> Unit) {
        val tx = GraphTransaction(this.db)
        try {
            tx.block()
            tx.commit(urgent)
        } catch (e: Exception) {
            tx.rollback()
            throw e
        }
    }

    /**
     * Retrieves target node IDs for outgoing relationships without deserializing edge data.
     *
     * This "fast path" method parses the LSM-tree keys directly, offering significant 
     * performance benefits for graph algorithms like PageRank or BFS.
     *
     * @return A list of Node IDs.
     */
    fun getOutboundTargetIds(sourceId: String, type: String): List<String> {
        val prefix = "g:e:out:$sourceId:$type:".toByteArray(Charsets.UTF_8)
        return db.getKeysByPrefixRaw(prefix).map {
            String(it, Charsets.UTF_8).substringAfterLast(":")
        }
    }

    /**
     * Retrieves source node IDs for incoming relationships without deserializing edge data.
     *
     * @return A list of Node IDs.
     */
    fun getInboundSourceIds(targetId: String, type: String): List<String> {
        val prefix = "g:e:in:$targetId:$type:".toByteArray(Charsets.UTF_8)
        return db.getKeysByPrefixRaw(prefix).map {
            String(it, Charsets.UTF_8).substringAfterLast(":")
        }
    }
}
