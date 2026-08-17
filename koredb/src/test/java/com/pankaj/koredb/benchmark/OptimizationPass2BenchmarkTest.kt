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
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.HNSWIndex
import com.pankaj.koredb.hnsw.ScalarQuantizer
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.Random
import java.util.UUID

class OptimizationPass2BenchmarkTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_opt2_bench_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun benchmarkQuantizedVectorSearchThroughput() {
        val dims = 128
        val count = 2000
        val random = Random(42)

        val quantizer = ScalarQuantizer(dims)
        val trainingVectors = (0 until 150).map {
            FloatArray(dims) { (random.nextFloat() - 0.5f) * 2f }
        }
        quantizer.train(trainingVectors)

        val index = HNSWIndex(
            maxNeighbors = 16,
            efConstruction = 100,
            efSearch = 50,
            metric = DistanceMetric.COSINE,
            quantizer = quantizer
        )

        // Populate quantized index
        for (i in 0 until count) {
            val vec = FloatArray(dims) { (random.nextFloat() - 0.5f) * 2f }
            val norm = kotlin.math.sqrt(vec.map { it * it }.sum())
            index.insert("vec_$i", vec, norm)
        }

        // Benchmark 1,000 vector search queries
        val queryVec = FloatArray(dims) { (random.nextFloat() - 0.5f) * 2f }
        val queryNorm = kotlin.math.sqrt(queryVec.map { it * it }.sum())

        val start = System.nanoTime()
        val queryIterations = 1000
        var totalResults = 0
        for (i in 0 until queryIterations) {
            val results = index.search(queryVec, limit = 10)
            totalResults += results.size
        }
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        val qps = (queryIterations / (durationMs / 1000.0)).toInt()

        println("=================================================================")
        println("🤖 QUANTIZED VECTOR SEARCH BENCHMARK (SQ8 128-d)")
        println("   Index Size: $count Vectors | Query Iterations: $queryIterations")
        println("   Total Search Time: ${String.format("%.2f", durationMs)} ms ($qps queries/sec)")
        println("=================================================================\n")

        assertTrue(totalResults > 0)
    }

    @Test
    fun benchmarkEventStreamHistoryRetrieval() = runBlocking {
        val stream = db.eventStream("audit_logs")
        val eventCount = 5000

        for (i in 1..eventCount) {
            stream.publish("Audit event payload #$i for transaction".toByteArray(Charsets.UTF_8))
        }
        db.engine.flushMemTableInternal()

        val start = System.nanoTime()
        val history = stream.getHistory()
        val durationMs = (System.nanoTime() - start) / 1_000_000.0

        println("=================================================================")
        println("⚡ EVENT STREAM HISTORY BENCHMARK")
        println("   Retrieved $eventCount Chronological Events: ${String.format("%.2f", durationMs)} ms")
        println("=================================================================\n")

        assertEquals(eventCount, history.size)
    }
}
