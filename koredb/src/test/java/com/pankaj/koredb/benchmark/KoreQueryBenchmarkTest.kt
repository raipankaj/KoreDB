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

package com.pankaj.koredb.benchmark

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

@Serializable
data class BenchmarkProduct(
    val id: String,
    val name: String,
    val category: String,
    val price: Double
)

class KoreQueryBenchmarkTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_query_bench_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun benchmarkQueryExecutionWithLimit() = runBlocking {
        val collection = db.collection<BenchmarkProduct>("products")
        collection.registerProperty("category") { it.category }
        collection.registerProperty("price") { it.price.toString() }

        // Populate 10,000 documents
        val docCount = 10000
        val batch = mutableMapOf<String, BenchmarkProduct>()
        val categories = listOf("shoes", "electronics", "clothing", "books", "home")

        for (i in 1..docCount) {
            val id = "prod_$i"
            val category = categories[i % categories.size]
            batch[id] = BenchmarkProduct(id, "Product #$i", category, (i % 500) + 9.99)
        }
        collection.insertBatch(batch)
        db.engine.flushMemTableInternal()

        println("=================================================================")
        println("📊 KORE QUERY BENCHMARK (10,000 Documents)")
        println("=================================================================")

        // 1. Query with filter and .limit(10)
        val start = System.nanoTime()
        val results = collection.query()
            .where("category") { it == "shoes" }
            .limit(10)
            .execute()
        val durationMs = (System.nanoTime() - start) / 1_000_000.0

        println("⏱️  Filtered Query with .limit(10) across $docCount docs: ${String.format("%.3f", durationMs)} ms (Returned: ${results.size} items)")
        println("=================================================================\n")

        assertEquals(10, results.size)
    }
}
