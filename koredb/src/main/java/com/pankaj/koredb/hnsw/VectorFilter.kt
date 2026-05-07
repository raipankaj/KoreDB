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

package com.pankaj.koredb.hnsw

/**
 * Metadata filter DSL for hybrid vector + metadata search.
 *
 * Usage:
 * ```kotlin
 * collection.search(queryVector, limit = 10) {
 *     where("category", eq("shoes"))
 *     where("price", lt(100.0))
 *     where("brand", inList("Nike", "Adidas"))
 * }
 * ```
 *
 * Filters are evaluated DURING HNSW traversal (pre-filtering), not after.
 * This ensures the result set always contains exactly `limit` matching results,
 * unlike post-filtering which can return fewer results than requested.
 */
class VectorFilterBuilder {
    internal val conditions = mutableListOf<FilterCondition>()

    fun where(field: String, predicate: FilterPredicate) {
        conditions.add(FilterCondition(field, predicate))
    }
    
    /**
     * Builds an immutable filter that can be evaluated against metadata maps.
     */
    fun build(): VectorFilter {
        return VectorFilter(conditions.toList())
    }
}

/**
 * An immutable, compiled filter for evaluating metadata during vector search.
 */
class VectorFilter(private val conditions: List<FilterCondition>) {
    
    companion object {
        val EMPTY = VectorFilter(emptyList())
    }
    
    fun isEmpty() = conditions.isEmpty()
    
    /**
     * Evaluates this filter against a metadata map.
     * Returns true if ALL conditions are satisfied (AND logic).
     */
    fun matches(metadata: Map<String, Any>?): Boolean {
        if (conditions.isEmpty()) return true
        if (metadata == null) return false
        
        return conditions.all { condition ->
            val value = metadata[condition.field]
            condition.predicate.evaluate(value)
        }
    }
}

data class FilterCondition(val field: String, val predicate: FilterPredicate)

/**
 * A predicate that evaluates a single metadata field value.
 */
sealed class FilterPredicate {
    abstract fun evaluate(value: Any?): Boolean
}

// --- Comparison Predicates ---

class EqPredicate(private val target: Any) : FilterPredicate() {
    override fun evaluate(value: Any?): Boolean = value == target
}

class NeqPredicate(private val target: Any) : FilterPredicate() {
    override fun evaluate(value: Any?): Boolean = value != null && value != target
}

class GtPredicate(private val target: Comparable<Any>) : FilterPredicate() {
    @Suppress("UNCHECKED_CAST")
    override fun evaluate(value: Any?): Boolean {
        if (value == null) return false
        return try { (value as Comparable<Any>) > target } catch (_: Exception) { false }
    }
}

class GtePredicate(private val target: Comparable<Any>) : FilterPredicate() {
    @Suppress("UNCHECKED_CAST")
    override fun evaluate(value: Any?): Boolean {
        if (value == null) return false
        return try { (value as Comparable<Any>) >= target } catch (_: Exception) { false }
    }
}

class LtPredicate(private val target: Comparable<Any>) : FilterPredicate() {
    @Suppress("UNCHECKED_CAST")
    override fun evaluate(value: Any?): Boolean {
        if (value == null) return false
        return try { (value as Comparable<Any>) < target } catch (_: Exception) { false }
    }
}

class LtePredicate(private val target: Comparable<Any>) : FilterPredicate() {
    @Suppress("UNCHECKED_CAST")
    override fun evaluate(value: Any?): Boolean {
        if (value == null) return false
        return try { (value as Comparable<Any>) <= target } catch (_: Exception) { false }
    }
}

class InListPredicate(private val values: Set<Any>) : FilterPredicate() {
    override fun evaluate(value: Any?): Boolean = value != null && value in values
}

class NotInListPredicate(private val values: Set<Any>) : FilterPredicate() {
    override fun evaluate(value: Any?): Boolean = value != null && value !in values
}

class ContainsPredicate(private val substring: String) : FilterPredicate() {
    override fun evaluate(value: Any?): Boolean {
        return value is String && value.contains(substring, ignoreCase = true)
    }
}

class ExistsPredicate : FilterPredicate() {
    override fun evaluate(value: Any?): Boolean = value != null
}

// --- Top-Level DSL Functions ---

fun eq(value: Any): FilterPredicate = EqPredicate(value)
fun neq(value: Any): FilterPredicate = NeqPredicate(value)

@Suppress("UNCHECKED_CAST")
fun gt(value: Comparable<*>): FilterPredicate = GtPredicate(value as Comparable<Any>)

@Suppress("UNCHECKED_CAST")
fun gte(value: Comparable<*>): FilterPredicate = GtePredicate(value as Comparable<Any>)

@Suppress("UNCHECKED_CAST")
fun lt(value: Comparable<*>): FilterPredicate = LtPredicate(value as Comparable<Any>)

@Suppress("UNCHECKED_CAST")
fun lte(value: Comparable<*>): FilterPredicate = LtePredicate(value as Comparable<Any>)

fun inList(vararg values: Any): FilterPredicate = InListPredicate(values.toSet())
fun notInList(vararg values: Any): FilterPredicate = NotInListPredicate(values.toSet())
fun contains(substring: String): FilterPredicate = ContainsPredicate(substring)
fun exists(): FilterPredicate = ExistsPredicate()
