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

package com.pankaj.koredb.graph.query

import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.graph.Node

/**
 * A highly optimized, fluent API for traversing and querying graph data in KoreDB.
 *
 * Unlike traditional string-based query languages (e.g., Cypher or SQL), [GraphQuery] 
 * provides a type-safe, DSL-like interface designed specifically for Kotlin developers.
 *
 * ### Performance Architecture:
 * The query engine is designed for maximum throughput by strictly tracking lightweight 
 * Node identifiers (Strings) during graph traversals. By operating solely on these IDs, 
 * the engine completely bypasses heavy JSON deserialization and object allocation until 
 * the final terminal operation [toNodeList] is invoked.
 *
 * @property storage The underlying [GraphStorage] engine used to execute the query.
 */
class GraphQuery(private val storage: GraphStorage) {

    /**
     * The set of active Node IDs currently being tracked in the traversal state.
     */
    private var currentIds = setOf<String>()

    /**
     * Initializes the traversal by selecting nodes via a high-speed Property Index.
     *
     * This operation executes in O(log N) time by performing a prefix scan directly 
     * on the indexed keys in the LSM-tree.
     *
     * @param label The node label to filter by.
     * @param propertyKey The name of the indexed property.
     * @param propertyValue The value of the property to match.
     * @return The current [GraphQuery] instance for method chaining.
     */
    fun startingWith(label: String, propertyKey: String, propertyValue: String): GraphQuery {
        val prefix = "g:idx:v_prop:$label:$propertyKey:$propertyValue:".toByteArray(Charsets.UTF_8)
        currentIds = storage.db.getKeysByPrefixRaw(prefix)
            .map { String(it, Charsets.UTF_8).substringAfterLast(":") }
            .toSet()
        return this
    }

    /**
     * Initializes the traversal from a explicit set of known Node IDs.
     *
     * @param nodeIds One or more Node identifiers to start from.
     * @return The current [GraphQuery] instance for method chaining.
     */
    fun startingWith(vararg nodeIds: String): GraphQuery {
        currentIds = nodeIds.toSet()
        return this
    }

    /**
     * Traverses forward across outgoing edges of a specific type.
     *
     * This operation utilizes target-embedded keys in the storage engine to 
     * retrieve connected node IDs without reading or parsing edge property JSON.
     *
     * @param edgeType The type of relationship to follow.
     * @param hops The number of consecutive steps to take (default is 1).
     * @return The current [GraphQuery] instance for method chaining.
     */
    fun outbound(edgeType: String, hops: Int = 1): GraphQuery {
        require(hops > 0) { "Hops must be at least 1" }
        for (i in 0 until hops) {
            val nextIds = mutableSetOf<String>()
            for (id in currentIds) {
                // Efficiency: Retrieves target IDs directly from the LSM-tree key strings.
                nextIds.addAll(storage.getOutboundTargetIds(id, edgeType))
            }
            currentIds = nextIds
        }
        return this
    }

    /**
     * Traverses backward across incoming edges of a specific type.
     *
     * @param edgeType The type of relationship to follow in reverse.
     * @param hops The number of consecutive steps to take (default is 1).
     * @return The current [GraphQuery] instance for method chaining.
     */
    fun inbound(edgeType: String, hops: Int = 1): GraphQuery {
        require(hops > 0) { "Hops must be at least 1" }
        for (i in 0 until hops) {
            val nextIds = mutableSetOf<String>()
            for (id in currentIds) {
                // Efficiency: Retrieves source IDs directly from the LSM-tree key strings.
                nextIds.addAll(storage.getInboundSourceIds(id, edgeType))
            }
            currentIds = nextIds
        }
        return this
    }

    /**
     * Filters the current set of nodes using a native Kotlin predicate.
     *
     * Note: This operation requires inspecting node properties, which triggers 
     * JSON deserialization for the current intermediate result set. For maximum 
     * performance, prefer using [startingWith] to filter via indices.
     *
     * @param predicate A lambda that returns true for nodes that should be kept.
     * @return The current [GraphQuery] instance for method chaining.
     */
    fun filterNodes(predicate: (Node) -> Boolean): GraphQuery {
        currentIds = currentIds.filter { id ->
            val node = storage.getNode(id)
            node != null && predicate(node)
        }.toSet()
        return this
    }

    /**
     * Terminal Operation: Resolves the tracked IDs into full [Node] objects.
     *
     * This is typically the only stage where JSON deserialization occurs for 
     * the result set.
     *
     * @return A list of [Node] objects corresponding to the final state of the traversal.
     */
    fun toNodeList(): List<Node> {
        return currentIds.mapNotNull { storage.getNode(it) }
    }

    /**
     * Terminal Operation: Returns the raw Node IDs as strings.
     *
     * This is the highest-performance terminal operation, as it avoids all 
     * node object construction and property parsing.
     *
     * @return A list of Node identifiers.
     */
    fun toIdList(): List<String> {
        return currentIds.toList()
    }
}

/**
 * Entry point for creating a [GraphQuery] from a [GraphStorage] instance.
 */
fun GraphStorage.query(): GraphQuery = GraphQuery(this)

/**
 * A scope-oriented builder for constructing nested graph queries.
 *
 * Example:
 * ```
 * graph.query {
 *     startingWith("Person", "city", "Tokyo")
 *     outbound("KNOWS", hops = 2)
 * }.toNodeList()
 * ```
 */
inline fun GraphStorage.query(block: GraphQuery.() -> Unit): GraphQuery {
    return GraphQuery(this).apply(block)
}
