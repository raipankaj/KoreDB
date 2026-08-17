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
import kotlinx.serialization.decodeFromString
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
 * - **Reverse Node Index**: `g:rptr:v_prop:{label}:{key}:{nodeId}` -> Current Value.
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

    private fun escape(value: String): String {
        return value.replace("%", "%25").replace(":", "%3A")
    }

    private fun unescape(value: String): String {
        return value.replace("%3A", ":").replace("%25", "%")
    }

    // --- NODE OPERATIONS ---

    /**
     * Persists a [Node] and atomically updates its associated label and property indices.
     *
     * @param node The node object to store.
     */
    suspend fun putNode(node: Node) {
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        
        val nodeKey = "g:v:${escape(node.id)}".toByteArray(Charsets.UTF_8)
        val nodeValue = json.encodeToString(node).toByteArray(Charsets.UTF_8)
        
        batch.add(Pair(nodeKey, nodeValue))
        
        for (label in node.labels) {
            // Index nodes by their labels for category-based scans.
            val labelKey = "g:idx:v:${escape(label)}:${escape(node.id)}".toByteArray(Charsets.UTF_8)
            batch.add(Pair(labelKey, PRESENCE_MARKER))
            
            // Build indices for equality-based property lookups.
            for ((key, value) in node.properties) {
                // 1. Forward Index (sidx)
                val propKey = "g:idx:v_prop:${escape(label)}:${escape(key)}:${escape(value)}:${escape(node.id)}".toByteArray(Charsets.UTF_8)
                batch.add(Pair(propKey, PRESENCE_MARKER))

                // 2. Reverse Pointer (Truth Oracle)
                val rptrKey = "g:rptr:v_prop:${escape(label)}:${escape(key)}:${escape(node.id)}".toByteArray(Charsets.UTF_8)
                batch.add(Pair(rptrKey, value.toByteArray(Charsets.UTF_8)))
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
        val bytes = db.getRaw("g:v:${escape(id)}".toByteArray(Charsets.UTF_8)) ?: return null
        if (bytes.isEmpty()) return null
        return json.decodeFromString<Node>(String(bytes, Charsets.UTF_8))
    }

    /**
     * Retrieves multiple [Node]s in a batch.
     *
     * @param ids The IDs of the nodes to fetch.
     * @return A map of node ID to [Node] for all nodes that exist.
     */
    fun getNodes(ids: Collection<String>): Map<String, Node> {
        if (ids.isEmpty()) return emptyMap()
        val result = HashMap<String, Node>(ids.size)
        val uniqueIds = if (ids is Set) ids else ids.toSet()
        for (id in uniqueIds) {
            val node = getNode(id)
            if (node != null) {
                result[id] = node
            }
        }
        return result
    }

    /**
     * Efficiently retrieves nodes matching a specific label and property value.
     *
     * This operation performs an O(log N) seek on the property index followed by 
     * sequential reads for matching IDs. It also filters out stale index entries 
     * (e.g., if a node was updated and the old index entry persists).
     *
     * @param label The label category to filter by.
     * @param propertyKey The property name to match.
     * @param propertyValue The property value to match.
     * @return A list of matching [Node] objects.
     */
    fun getNodesByProperty(label: String, propertyKey: String, propertyValue: String): List<Node> {
        val prefix = "g:idx:v_prop:${escape(label)}:${escape(propertyKey)}:${escape(propertyValue)}:".toByteArray(Charsets.UTF_8)
        
        val indexKeys = db.getKeysByPrefixRaw(prefix)
        return indexKeys.mapNotNull { keyBytes ->
            val keyString = String(keyBytes, Charsets.UTF_8)
            val nodeId = unescape(keyString.substringAfterLast(":"))
            
            // 1. Validation using Reverse Pointer (The Truth Oracle)
            // Much faster than reading the full Node JSON.
            val rptrKey = "g:rptr:v_prop:${escape(label)}:${escape(propertyKey)}:${escape(nodeId)}".toByteArray(Charsets.UTF_8)
            val currentValueBytes = db.getRaw(rptrKey)
            
            if (currentValueBytes != null) {
                val currentValue = String(currentValueBytes, Charsets.UTF_8)
                if (currentValue == propertyValue) {
                    // 2. Proof positive: this index is fresh. Fetch node.
                    return@mapNotNull getNode(nodeId)
                }
            }
            null
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
        val outKey = "g:e:out:${escape(edge.sourceId)}:${escape(edge.type)}:${escape(edge.targetId)}".toByteArray(Charsets.UTF_8)
        val inKey  = "g:e:in:${escape(edge.targetId)}:${escape(edge.type)}:${escape(edge.sourceId)}".toByteArray(Charsets.UTF_8)
        
        batch.add(Pair(outKey, edgeValue))
        batch.add(Pair(inKey, edgeValue))
        
        // Maintain property indices for edges to support weighted traversal queries.
        for ((key, value) in edge.properties) {
             val propKey = "g:idx:e_prop:${escape(edge.type)}:${escape(key)}:${escape(value)}:${escape(edge.sourceId)}:${escape(edge.targetId)}".toByteArray(Charsets.UTF_8)
             batch.add(Pair(propKey, PRESENCE_MARKER))
        }
        
        db.writeBatchRaw(batch)
    }

    /**
     * Atomically removes an edge and its bidirectional index entries.
     */
    suspend fun removeEdge(sourceId: String, type: String, targetId: String) {
        val outKey = "g:e:out:${escape(sourceId)}:${escape(type)}:${escape(targetId)}".toByteArray(Charsets.UTF_8)
        val inKey  = "g:e:in:${escape(targetId)}:${escape(type)}:${escape(sourceId)}".toByteArray(Charsets.UTF_8)
        
        db.writeBatchRaw(listOf(
            outKey to KoreDB.TOMBSTONE,
            inKey to KoreDB.TOMBSTONE
        ))
    }

    /**
     * Retrieves all outgoing relationships of a specific type from a node.
     */
    fun getOutboundEdges(sourceId: String, type: String): List<Edge> {
        val prefix = "g:e:out:${escape(sourceId)}:${escape(type)}:".toByteArray(Charsets.UTF_8)
        val rawValues = db.getByPrefixRaw(prefix)
        return rawValues.map { json.decodeFromString<Edge>(String(it, Charsets.UTF_8)) }
    }

    /**
     * Retrieves all incoming relationships of a specific type to a node.
     */
    fun getInboundEdges(targetId: String, type: String): List<Edge> {
        val prefix = "g:e:in:${escape(targetId)}:${escape(type)}:".toByteArray(Charsets.UTF_8)
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
        val prefix = "g:e:out:${escape(sourceId)}:${escape(type)}:".toByteArray(Charsets.UTF_8)
        return db.getKeysByPrefixRaw(prefix).map {
            unescape(String(it, Charsets.UTF_8).substringAfterLast(":"))
        }
    }

    /**
     * Retrieves source node IDs for incoming relationships without deserializing edge data.
     *
     * @return A list of Node IDs.
     */
    fun getInboundSourceIds(targetId: String, type: String): List<String> {
        val prefix = "g:e:in:${escape(targetId)}:${escape(type)}:".toByteArray(Charsets.UTF_8)
        return db.getKeysByPrefixRaw(prefix).map {
            unescape(String(it, Charsets.UTF_8).substringAfterLast(":"))
        }
    }

    // --- NODE DELETION WITH CASCADING EDGE CLEANUP ---

    /**
     * Deletes a node and ALL of its connected edges, label indexes, and property indexes.
     *
     * This performs a cascading delete:
     * 1. Removes all outbound edges (both out-key and the corresponding in-key at the target)
     * 2. Removes all inbound edges (both in-key and the corresponding out-key at the source)
     * 3. Removes all label index entries
     * 4. Removes all property index entries and reverse pointers
     * 5. Removes the node itself
     *
     * @param nodeId The ID of the node to delete.
     * @return true if the node existed and was deleted.
     */
    suspend fun deleteNode(nodeId: String): Boolean {
        val node = getNode(nodeId) ?: return false
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()

        // 1. Remove all outbound edges
        val outPrefix = "g:e:out:${escape(nodeId)}:".toByteArray(Charsets.UTF_8)
        val outKeys = db.getKeysByPrefixRaw(outPrefix)
        for (keyBytes in outKeys) {
            val keyStr = String(keyBytes, Charsets.UTF_8)
            batch.add(keyBytes to KoreDB.TOMBSTONE)
            // Parse "g:e:out:sourceId:type:targetId" to build the corresponding in-key
            val parts = keyStr.split(":")
            if (parts.size >= 6) {
                val type = parts[4] // Already escaped
                val targetId = parts[5] // Already escaped
                val inKey = "g:e:in:$targetId:$type:${escape(nodeId)}".toByteArray(Charsets.UTF_8)
                batch.add(inKey to KoreDB.TOMBSTONE)
            }
        }

        // 2. Remove all inbound edges
        val inPrefix = "g:e:in:${escape(nodeId)}:".toByteArray(Charsets.UTF_8)
        val inKeys = db.getKeysByPrefixRaw(inPrefix)
        for (keyBytes in inKeys) {
            val keyStr = String(keyBytes, Charsets.UTF_8)
            batch.add(keyBytes to KoreDB.TOMBSTONE)
            val parts = keyStr.split(":")
            if (parts.size >= 6) {
                val type = parts[4] // Already escaped
                val sourceId = parts[5] // Already escaped
                val outKey = "g:e:out:$sourceId:$type:${escape(nodeId)}".toByteArray(Charsets.UTF_8)
                batch.add(outKey to KoreDB.TOMBSTONE)
            }
        }

        // 3. Remove label indexes and property indexes
        for (label in node.labels) {
            batch.add("g:idx:v:${escape(label)}:${escape(nodeId)}".toByteArray(Charsets.UTF_8) to KoreDB.TOMBSTONE)
            for ((key, value) in node.properties) {
                batch.add("g:idx:v_prop:${escape(label)}:${escape(key)}:${escape(value)}:${escape(nodeId)}".toByteArray(Charsets.UTF_8) to KoreDB.TOMBSTONE)
                batch.add("g:rptr:v_prop:${escape(label)}:${escape(key)}:${escape(nodeId)}".toByteArray(Charsets.UTF_8) to KoreDB.TOMBSTONE)
            }
        }

        // 4. Remove the node itself
        batch.add("g:v:${escape(nodeId)}".toByteArray(Charsets.UTF_8) to KoreDB.TOMBSTONE)

        db.writeBatchRaw(batch)
        return true
    }

    // --- BATCH OPERATIONS ---

    /**
     * Inserts multiple nodes in a single atomic batch write.
     *
     * @param nodes The list of nodes to insert.
     */
    suspend fun putNodes(nodes: List<Node>) {
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        for (node in nodes) {
            val nodeKey = "g:v:${escape(node.id)}".toByteArray(Charsets.UTF_8)
            val nodeValue = json.encodeToString(Node.serializer(), node).toByteArray(Charsets.UTF_8)
            batch.add(nodeKey to nodeValue)

            for (label in node.labels) {
                batch.add("g:idx:v:${escape(label)}:${escape(node.id)}".toByteArray(Charsets.UTF_8) to PRESENCE_MARKER)
                for ((key, value) in node.properties) {
                    batch.add("g:idx:v_prop:${escape(label)}:${escape(key)}:${escape(value)}:${escape(node.id)}".toByteArray(Charsets.UTF_8) to PRESENCE_MARKER)
                    batch.add("g:rptr:v_prop:${escape(label)}:${escape(key)}:${escape(node.id)}".toByteArray(Charsets.UTF_8) to value.toByteArray(Charsets.UTF_8))
                }
            }
        }
        db.writeBatchRaw(batch)
    }

    /**
     * Inserts multiple edges in a single atomic batch write.
     *
     * @param edges The list of edges to insert.
     */
    suspend fun putEdges(edges: List<Edge>) {
        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        for (edge in edges) {
            val edgeValue = json.encodeToString(Edge.serializer(), edge).toByteArray(Charsets.UTF_8)
            batch.add("g:e:out:${escape(edge.sourceId)}:${escape(edge.type)}:${escape(edge.targetId)}".toByteArray(Charsets.UTF_8) to edgeValue)
            batch.add("g:e:in:${escape(edge.targetId)}:${escape(edge.type)}:${escape(edge.sourceId)}".toByteArray(Charsets.UTF_8) to edgeValue)
            for ((key, value) in edge.properties) {
                batch.add("g:idx:e_prop:${escape(edge.type)}:${escape(key)}:${escape(value)}:${escape(edge.sourceId)}:${escape(edge.targetId)}".toByteArray(Charsets.UTF_8) to PRESENCE_MARKER)
            }
        }
        db.writeBatchRaw(batch)
    }

    // --- QUERY UTILITIES ---

    /**
     * Retrieves all nodes with a given label using the label index.
     */
    fun getNodesByLabel(label: String): List<Node> {
        val prefix = "g:idx:v:${escape(label)}:".toByteArray(Charsets.UTF_8)
        return db.getKeysByPrefixRaw(prefix).mapNotNull { keyBytes ->
            val nodeId = unescape(String(keyBytes, Charsets.UTF_8).substringAfterLast(":"))
            getNode(nodeId)
        }
    }

    /**
     * Returns all distinct outbound edge types from a given node.
     */
    fun getOutboundEdgeTypes(nodeId: String): List<String> {
        val prefix = "g:e:out:${escape(nodeId)}:".toByteArray(Charsets.UTF_8)
        return db.getKeysByPrefixRaw(prefix).map { keyBytes ->
            val keyStr = String(keyBytes, Charsets.UTF_8)
            unescape(keyStr.split(":").getOrElse(4) { "" })
        }.distinct()
    }

    /**
     * Returns all outbound edges of ANY type from a given node.
     */
    fun getAllOutboundEdges(nodeId: String): List<Edge> {
        val prefix = "g:e:out:${escape(nodeId)}:".toByteArray(Charsets.UTF_8)
        val rawValues = db.getByPrefixRaw(prefix)
        return rawValues.mapNotNull { bytes ->
            if (bytes.isEmpty()) null
            else json.decodeFromString<Edge>(String(bytes, Charsets.UTF_8))
        }
    }

    /**
     * Returns all inbound edges of ANY type to a given node.
     */
    fun getAllInboundEdges(nodeId: String): List<Edge> {
        val prefix = "g:e:in:${escape(nodeId)}:".toByteArray(Charsets.UTF_8)
        val rawValues = db.getByPrefixRaw(prefix)
        return rawValues.mapNotNull { bytes ->
            if (bytes.isEmpty()) null
            else json.decodeFromString<Edge>(String(bytes, Charsets.UTF_8))
        }
    }
}
