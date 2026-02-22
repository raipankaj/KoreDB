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

package com.pankaj.koredb.core

import com.pankaj.koredb.engine.KoreDB

/**
 * Manages a collection of vectors for similarity search.
 *
 * This collection stores high-dimensional vectors (as FloatArrays) and provides
 * efficient k-nearest neighbor (k-NN) search using Cosine Similarity.
 *
 * @property name The name of the collection.
 * @property db The underlying database engine.
 */
class KoreVectorCollection(
    val name: String,
    private val db: KoreDB
) {
    private fun makeKey(id: String) = "vec:$name:$id".toByteArray(Charsets.UTF_8)

    /**
     * Inserts or updates a vector in the collection.
     *
     * @param id Unique identifier for the vector.
     * @param vector The vector data as a [FloatArray].
     */
    suspend fun insert(id: String, vector: FloatArray) {
        db.putRaw(makeKey(id), VectorSerializer.toByteArray(vector))
    }

    /**
     * Performs a batch insert of multiple vectors.
     *
     * @param vectors A map of IDs to their corresponding [FloatArray] vectors.
     */
    suspend fun insertBatch(vectors: Map<String, FloatArray>) {
        val batch = vectors.map { Pair(makeKey(it.key), VectorSerializer.toByteArray(it.value)) }
        db.writeBatchRaw(batch)
    }

    /**
     * Searches for the most similar vectors to the given query vector.
     *
     * Uses Cosine Similarity to rank results. The similarity score ranges from
     * -1.0 to 1.0 (where 1.0 is perfectly similar).
     *
     * @param query The query vector.
     * @param limit The maximum number of results to return.
     * @return A list of pairs containing the document ID and its similarity score,
     *         sorted by score in descending order.
     */
    fun search(query: FloatArray, limit: Int = 5): List<Pair<String, Float>> {
        val prefix = "vec:$name:".toByteArray(Charsets.UTF_8)
        val rawResults = db.searchVectorsRaw(prefix, query, limit)
        
        return rawResults.map { 
            val id = String(it.first, Charsets.UTF_8).removePrefix("vec:$name:")
            Pair(id, it.second)
        }
    }
}
