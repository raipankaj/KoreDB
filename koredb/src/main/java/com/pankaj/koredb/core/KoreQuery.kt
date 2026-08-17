package com.pankaj.koredb.core

/**
 * A fluent query builder for KoreDB collections.
 *
 * Provides range queries, aggregation, sorting, and limiting — capabilities
 * that close the gap with Room/Realm without requiring SQL.
 *
 * Usage:
 * ```kotlin
 * // Range query
 * val results = collection.query()
 *     .where("price") { it.toDouble() > 10 && it.toDouble() < 100 }
 *     .sortBy("price") { it.toDouble() }
 *     .limit(20)
 *     .execute()
 *
 * // Aggregation
 * val stats = collection.query()
 *     .where("category") { it == "shoes" }
 *     .aggregate {
 *         count()
 *         sum("price") { it.toDouble() }
 *         avg("price") { it.toDouble() }
 *         min("price") { it.toDouble() }
 *         max("price") { it.toDouble() }
 *     }
 * ```
 *
 * @param T The document type.
 * @param collection The source collection.
 * @param propertyExtractors Map of property names to extraction functions.
 */
class KoreQuery<T>(
    private val collection: KoreCollection<T>,
    private val propertyExtractors: Map<String, (T) -> String>
) {
    private val filters = mutableListOf<(T) -> Boolean>()
    private var sortKey: ((T) -> Comparable<*>)? = null
    private var sortDescending = false
    private var limitCount: Int? = null
    private var offsetCount: Int = 0

    /**
     * Filters documents where the extracted property value matches the predicate.
     *
     * ```kotlin
     * .where("price") { it.toDoubleOrNull()?.let { p -> p > 10 } ?: false }
     * ```
     */
    fun where(propertyName: String, predicate: (String) -> Boolean): KoreQuery<T> {
        val extractor = propertyExtractors[propertyName]
        if (extractor != null) {
            filters.add { doc -> predicate(extractor(doc)) }
        }
        return this
    }

    /**
     * Filters documents using a raw predicate on the document object.
     */
    fun filter(predicate: (T) -> Boolean): KoreQuery<T> {
        filters.add(predicate)
        return this
    }

    /**
     * Sorts results by a property value.
     *
     * @param propertyName The property to sort by.
     * @param transform Converts the property string to a Comparable for ordering.
     * @param descending If true, sorts in descending order.
     */
    @Suppress("UNCHECKED_CAST")
    fun <R : Comparable<R>> sortBy(
        propertyName: String,
        descending: Boolean = false,
        transform: (String) -> R
    ): KoreQuery<T> {
        val extractor = propertyExtractors[propertyName]
        if (extractor != null) {
            sortKey = { doc -> transform(extractor(doc)) }
            sortDescending = descending
        }
        return this
    }

    /** Limits the result count. */
    fun limit(n: Int): KoreQuery<T> { limitCount = n; return this }

    /** Skips the first [n] results. */
    fun offset(n: Int): KoreQuery<T> { offsetCount = n; return this }

    /**
     * Executes the query and returns matching documents with early-termination streaming.
     */
    fun execute(): List<T> {
        var seq = collection.asSequence()

        // Apply filters lazily
        for (filter in filters) {
            seq = seq.filter(filter)
        }

        // Apply sort
        val key = sortKey
        if (key != null) {
            @Suppress("UNCHECKED_CAST")
            val list = if (sortDescending) {
                seq.toList().sortedByDescending { key(it) as Comparable<Any> }
            } else {
                seq.toList().sortedBy { key(it) as Comparable<Any> }
            }
            var resultList = list
            if (offsetCount > 0) resultList = resultList.drop(offsetCount)
            if (limitCount != null) resultList = resultList.take(limitCount!!)
            return resultList
        }

        // Apply offset + limit lazily without collecting entire dataset
        if (offsetCount > 0) seq = seq.drop(offsetCount)
        if (limitCount != null) seq = seq.take(limitCount!!)

        return seq.toList()
    }

    /**
     * Executes the query and returns aggregation results.
     */
    fun aggregate(block: AggregationBuilder<T>.() -> Unit): AggregationResult {
        val builder = AggregationBuilder<T>(propertyExtractors)
        builder.block()

        var seq = collection.asSequence()
        for (filter in filters) seq = seq.filter(filter)

        return builder.compute(seq.toList())
    }

    /** Returns the count of matching documents. */
    fun count(): Int {
        var seq = collection.asSequence()
        for (filter in filters) seq = seq.filter(filter)
        return seq.count()
    }
}

/**
 * Builder for aggregation operations (count, sum, avg, min, max).
 */
class AggregationBuilder<T>(
    private val extractors: Map<String, (T) -> String>
) {
    internal data class AggOp(val name: String, val field: String, val transform: (String) -> Double)

    internal val ops = mutableListOf<AggOp>()
    internal var doCount = false

    fun count() { doCount = true }

    fun sum(field: String, transform: (String) -> Double = { it.toDouble() }) {
        ops.add(AggOp("sum", field, transform))
    }

    fun avg(field: String, transform: (String) -> Double = { it.toDouble() }) {
        ops.add(AggOp("avg", field, transform))
    }

    fun min(field: String, transform: (String) -> Double = { it.toDouble() }) {
        ops.add(AggOp("min", field, transform))
    }

    fun max(field: String, transform: (String) -> Double = { it.toDouble() }) {
        ops.add(AggOp("max", field, transform))
    }

    fun compute(data: List<T>): AggregationResult {
        val results = mutableMapOf<String, Any>()

        if (doCount) results["count"] = data.size

        for (op in ops) {
            val extractor = extractors[op.field] ?: continue
            val values = data.mapNotNull { doc ->
                try { op.transform(extractor(doc)) } catch (_: Exception) { null }
            }

            if (values.isEmpty()) continue

            val key = "${op.name}(${op.field})"
            results[key] = when (op.name) {
                "sum" -> values.sum()
                "avg" -> values.average()
                "min" -> values.min()
                "max" -> values.max()
                else -> 0.0
            }
        }

        return AggregationResult(results)
    }
}

/**
 * Holds the results of an aggregation query.
 */
data class AggregationResult(private val values: Map<String, Any>) {
    fun getCount(): Int = values["count"] as? Int ?: 0
    fun getDouble(key: String): Double? = values[key] as? Double
    fun getSum(field: String): Double? = values["sum($field)"] as? Double
    fun getAvg(field: String): Double? = values["avg($field)"] as? Double
    fun getMin(field: String): Double? = values["min($field)"] as? Double
    fun getMax(field: String): Double? = values["max($field)"] as? Double
    fun toMap(): Map<String, Any> = values
    override fun toString(): String = values.entries.joinToString(", ") { "${it.key}=${it.value}" }
}
