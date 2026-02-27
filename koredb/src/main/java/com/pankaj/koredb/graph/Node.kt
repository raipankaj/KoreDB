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

import kotlinx.serialization.Serializable

/**
 * Represents a fundamental entity (Node/Vertex) within the graph.
 *
 * Nodes are the primary data containers in the graph, representing discrete 
 * objects or concepts. They are uniquely identified and can be categorized 
 * using labels.
 *
 * @property id The unique identifier for this node, used as a primary key in storage.
 * @property labels a set of semantic categories or tags assigned to the node (e.g., "User", "Product").
 * @property properties A collection of key-value metadata attributes associated with the node.
 */
@Serializable
data class Node(
    val id: String,
    val labels: Set<String> = emptySet(),
    val properties: Map<String, String> = emptyMap()
)
