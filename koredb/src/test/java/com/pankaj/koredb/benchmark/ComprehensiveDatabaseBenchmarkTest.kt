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
import com.pankaj.koredb.foundation.BlockCache
import com.pankaj.koredb.fts.FtsIndex
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.GraphStorage
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.HNSWIndex
import com.pankaj.koredb.hnsw.MmapHNSWIndex
import com.pankaj.koredb.hnsw.ScalarQuantizer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.Random
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Comprehensive Reproducible Programmatic Benchmark Suite for KoreDB.
 *
 * Measures performance and latency baselines across all core subsystems:
 * 1. Storage Engine: LSM write throughput (ops/sec) and point lookup latency (μs)
 * 2. Foundation: Multi-threaded BlockCache hit throughput (hits/sec)
 * 3. Vector Engine: Unquantized Float vs SQ8 Quantized vs MmapHNSW query throughput (QPS)
 * 4. Full-Text Search: Inverted term query latency (μs) and BM25 scoring throughput (QPS)
 * 5. Property Graph: Multi-hop BFS traversal (ms) and PageRank power iteration latency (ms)
 * 6. Concurrency: Multi-threaded MVCC commit throughput (tx/sec) and commit latency (μs)
 * 7. Engine Lifecycle: Cold-start hydration time (ms) and JVM heap footprint delta (MB)
 * 8. Query Engine: Field filtering and Top-K bounded sorting latency (ms)
 */
class ComprehensiveDatabaseBenchmarkTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    data class BenchmarkDoc(
        val id: String,
        val name: String,
        val category: String,
        val score: Double
    )

    @Before
    fun setup() {
        testDir = File("build/tmp/test_comprehensive_bench_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        try {
            db.close()
        } catch (_: Exception) {}
        testDir.deleteRecursively()
    }

    /**
     * Benchmark 1: Measures write throughput (ops/sec) and point lookup latency (micros) on KoreDB LSM engine.
     */
    @Test
    fun benchmarkEngineWritesAndPointReads() = runBlocking {
        val writeCount = 10_000
        val payload = ByteArray(128) { (it % 100).toByte() }

        // 1. Measure raw LSM-tree write throughput
        val writeStart = System.nanoTime()
        for (i in 1..writeCount) {
            val key = "bench_key_${String.format("%06d", i)}".toByteArray(Charsets.UTF_8)
            db.engine.putRaw(key, payload)
        }
        val writeDurationMs = (System.nanoTime() - writeStart) / 1_000_000.0
        val writeThroughput = (writeCount / (writeDurationMs / 1000.0)).toInt()

        // Flush to SSTables to exercise SSTable point lookup path
        db.engine.flushMemTableInternal()

        // 2. Measure point lookup latency & read throughput
        val readCount = 10_000
        val readStart = System.nanoTime()
        var foundCount = 0
        for (i in 1..readCount) {
            val key = "bench_key_${String.format("%06d", i)}".toByteArray(Charsets.UTF_8)
            val res = db.engine.getRaw(key)
            if (res != null) foundCount++
        }
        val readDurationMs = (System.nanoTime() - readStart) / 1_000_000.0
        val avgLookupMicros = (readDurationMs * 1000.0) / readCount
        val readThroughput = (readCount / (readDurationMs / 1000.0)).toInt()

        println("=================================================================")
        println("🔥 BENCHMARK: Engine Writes & Point Reads (10,000 Records)")
        println("=================================================================")
        println("   Write Throughput       : $writeThroughput ops/sec (${String.format("%.2f", writeDurationMs)} ms)")
        println("   Point Lookup Latency   : ${String.format("%.2f", avgLookupMicros)} μs/op")
        println("   Read Throughput        : $readThroughput ops/sec (${String.format("%.2f", readDurationMs)} ms)")
        println("   Verified Matches       : $foundCount / $readCount")
        println("=================================================================\n")

        assertEquals(writeCount, foundCount)
        assertTrue(writeThroughput > 0)
        assertTrue(readThroughput > 0)
    }

    /**
     * Benchmark 2: Measures multi-threaded cache hit throughput.
     */
    @Test
    fun benchmarkBlockCacheHits() {
        val cache = BlockCache(maxSizeBytes = 32L * 1024L * 1024L)
        val entryCount = 1_000
        val blockData = ByteArray(1024) { (it % 127).toByte() }

        for (i in 0 until entryCount) {
            cache.put("block_$i", blockData)
        }

        val threadCount = 8
        val opsPerThread = 25_000
        val executor = Executors.newFixedThreadPool(threadCount)
        val latch = CountDownLatch(threadCount)

        val start = System.nanoTime()
        for (t in 0 until threadCount) {
            executor.submit {
                try {
                    val rng = Random(t.toLong() * 31L + 42L)
                    for (op in 0 until opsPerThread) {
                        val key = "block_${rng.nextInt(entryCount)}"
                        val hit = cache.get(key)
                        if (hit == null) error("Cache hit failed for $key")
                    }
                } finally {
                    latch.countDown()
                }
            }
        }
        latch.await(30, TimeUnit.SECONDS)
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        executor.shutdown()

        val totalOps = threadCount.toLong() * opsPerThread
        val throughput = (totalOps / (durationMs / 1000.0)).toLong()
        val hitRatio = cache.hits.get().toDouble() / (cache.hits.get() + cache.misses.get()) * 100.0

        println("=================================================================")
        println("⚡ BENCHMARK: Multi-Threaded Block Cache Hits (8 Threads, 200,000 Ops)")
        println("=================================================================")
        println("   Total Lookups          : $totalOps")
        println("   Duration               : ${String.format("%.2f", durationMs)} ms")
        println("   Hit Throughput         : $throughput hits/sec")
        println("   Hit Ratio              : ${String.format("%.2f", hitRatio)}% (${cache.hits.get()} hits, ${cache.misses.get()} misses)")
        println("=================================================================\n")

        assertEquals(totalOps, cache.hits.get())
        assertEquals(0L, cache.misses.get())
        assertTrue(throughput > 0)
    }

    /**
     * Benchmark 3: Measures QPS for Unquantized Float vs SQ8 Quantized vs MmapHNSW.
     */
    @Test
    fun benchmarkVectorSearchThroughput() {
        val dims = 128
        val count = 2000
        val random = Random(42)

        // Generate normalized dataset vectors
        val dataset = (0 until count).map { i ->
            val v = FloatArray(dims) { (random.nextFloat() - 0.5f) * 2f }
            val norm = kotlin.math.sqrt(v.map { it * it }.sum())
            val normalized = FloatArray(dims) { v[it] / norm }
            "vec_$i" to normalized
        }

        // 1. Build Unquantized Float Index
        val unquantIndex = HNSWIndex(
            maxNeighbors = 16,
            efConstruction = 100,
            efSearch = 50,
            metric = DistanceMetric.COSINE
        )
        for ((id, vec) in dataset) {
            unquantIndex.insert(id, vec, 1.0f)
        }
        val snapshotFile = File(testDir, "bench_unquant_snapshot.bin")
        unquantIndex.saveToDisk(snapshotFile)

        // 2. Build SQ8 Quantized Index
        val quantizer = ScalarQuantizer(dims)
        val training = (0 until 150).map { dataset[it].second }
        quantizer.train(training)

        val sq8Index = HNSWIndex(
            maxNeighbors = 16,
            efConstruction = 100,
            efSearch = 50,
            metric = DistanceMetric.COSINE,
            quantizer = quantizer
        )
        for ((id, vec) in dataset) {
            sq8Index.insert(id, vec, 1.0f)
        }

        // 3. Open Mmap HNSW Index
        val mmapIndex = MmapHNSWIndex(snapshotFile)

        // Benchmark queries
        val queryIterations = 1000
        val queries = (0 until queryIterations).map {
            val q = FloatArray(dims) { (random.nextFloat() - 0.5f) * 2f }
            val norm = kotlin.math.sqrt(q.map { it * it }.sum())
            FloatArray(dims) { q[it] / norm }
        }

        // Measure Unquantized Float
        val unquantStart = System.nanoTime()
        var unquantMatches = 0
        for (q in queries) {
            unquantMatches += unquantIndex.search(q, limit = 10).size
        }
        val unquantDurationMs = (System.nanoTime() - unquantStart) / 1_000_000.0
        val unquantQps = (queryIterations / (unquantDurationMs / 1000.0)).toInt()

        // Measure SQ8 Quantized
        val sq8Start = System.nanoTime()
        var sq8Matches = 0
        for (q in queries) {
            sq8Matches += sq8Index.search(q, limit = 10).size
        }
        val sq8DurationMs = (System.nanoTime() - sq8Start) / 1_000_000.0
        val sq8Qps = (queryIterations / (sq8DurationMs / 1000.0)).toInt()

        // Measure Mmap HNSW
        val mmapStart = System.nanoTime()
        var mmapMatches = 0
        for (q in queries) {
            mmapMatches += mmapIndex.search(q, limit = 10).size
        }
        val mmapDurationMs = (System.nanoTime() - mmapStart) / 1_000_000.0
        val mmapQps = (queryIterations / (mmapDurationMs / 1000.0)).toInt()

        mmapIndex.close()

        println("=================================================================")
        println("🤖 BENCHMARK: Vector Search QPS (128-d, 2,000 Vectors, 1,000 Queries)")
        println("=================================================================")
        println("   Unquantized Float HNSW : $unquantQps QPS (${String.format("%.2f", unquantDurationMs)} ms)")
        println("   SQ8 Quantized HNSW     : $sq8Qps QPS (${String.format("%.2f", sq8DurationMs)} ms)")
        println("   MmapHNSW (Off-Heap)    : $mmapQps QPS (${String.format("%.2f", mmapDurationMs)} ms)")
        println("=================================================================\n")

        assertTrue(unquantMatches > 0)
        assertTrue(sq8Matches > 0)
        assertTrue(mmapMatches > 0)
    }

    /**
     * Benchmark 4: Measures FTS term query latency and BM25 scoring throughput.
     */
    @Test
    fun benchmarkFtsSearchLatency() = runBlocking {
        val ftsIndex = FtsIndex("comp_bench_fts", db.engine)
        val corpusWords = listOf(
            "cardiology", "neurology", "orthopedic", "pediatric", "dermatology",
            "appointment", "prescription", "consultation", "diagnosis", "therapy",
            "invoice", "receipt", "insurance", "statement", "payment",
            "algorithm", "database", "vector", "graph", "optimization"
        )
        val docCount = 2000
        val rng = Random(42)

        val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
        for (i in 1..docCount) {
            val w1 = corpusWords[rng.nextInt(corpusWords.size)]
            val w2 = corpusWords[rng.nextInt(corpusWords.size)]
            val w3 = corpusWords[rng.nextInt(corpusWords.size)]
            val text = "Document $i discussing $w1 and $w2 clinical protocol with $w3 system."
            ftsIndex.indexDocument("doc_$i", text, batch)
        }
        db.engine.writeBatchRaw(batch)
        db.engine.flushMemTableInternal()

        val queryTerms = listOf(
            "cardiology", "neurology", "diagnosis", "database", "vector",
            "optimization", "prescription", "consultation", "insurance", "algorithm"
        )
        val queryCount = 1000
        val start = System.nanoTime()
        var totalHits = 0
        for (i in 0 until queryCount) {
            val term = queryTerms[i % queryTerms.size]
            val hits = ftsIndex.search(term, limit = 10)
            totalHits += hits.size
        }
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        val avgLatencyMicros = (durationMs * 1000.0) / queryCount
        val qps = (queryCount / (durationMs / 1000.0)).toInt()

        println("=================================================================")
        println("🔍 BENCHMARK: FTS Term Query Latency & BM25 Scoring Throughput")
        println("=================================================================")
        println("   Indexed Documents      : $docCount")
        println("   Query Iterations       : $queryCount")
        println("   Average Query Latency  : ${String.format("%.2f", avgLatencyMicros)} μs/query")
        println("   BM25 Scoring QPS       : $qps queries/sec (${String.format("%.2f", durationMs)} ms)")
        println("   Total Matching Hits    : $totalHits")
        println("=================================================================\n")

        assertTrue(totalHits > 0)
        assertTrue(qps > 0)
    }

    /**
     * Benchmark 5: Measures multi-hop traversal time and PageRank iteration latency (ms).
     */
    @Test
    fun benchmarkGraphTraversalAndPageRank() = runBlocking {
        val graph = GraphStorage(db.engine)
        val nodeCount = 500

        // Populate nodes
        for (i in 1..nodeCount) {
            graph.putNode(Node("node_$i", setOf("Entity"), mapOf("title" to "Node #$i")))
        }

        // Populate directed edges
        for (i in 1..nodeCount) {
            val t1 = (i % nodeCount) + 1
            val t2 = ((i * 3) % nodeCount) + 1
            val t3 = ((i * 7) % nodeCount) + 1
            graph.putEdge(Edge("node_$i", "node_$t1", "LINKS"))
            if (t2 != t1) graph.putEdge(Edge("node_$i", "node_$t2", "LINKS"))
            if (t3 != t1 && t3 != t2) graph.putEdge(Edge("node_$i", "node_$t3", "LINKS"))
        }
        db.engine.flushMemTableInternal()

        // 1. Multi-hop traversal benchmark
        val traversalRuns = 50
        val tStart = System.nanoTime()
        var totalTraversed = 0
        for (r in 1..traversalRuns) {
            val seed = "node_$r"
            val visited = GraphAlgorithms.bfs(graph, seed, "LINKS").take(50).toList()
            totalTraversed += visited.size
        }
        val tDurationMs = (System.nanoTime() - tStart) / 1_000_000.0
        val avgTraversalMs = tDurationMs / traversalRuns

        // 2. PageRank iteration benchmark
        val seedNodes = (1..nodeCount).map { "node_$it" }
        val iterations = 10
        val prStart = System.nanoTime()
        val ranks = GraphAlgorithms.pageRank(graph, seedNodes, "LINKS", iterations = iterations)
        val prDurationMs = (System.nanoTime() - prStart) / 1_000_000.0
        val msPerIteration = prDurationMs / iterations

        println("=================================================================")
        println("🕸️ BENCHMARK: Graph Traversal & PageRank ($nodeCount Nodes)")
        println("=================================================================")
        println("   Multi-Hop BFS Traversal : ${String.format("%.2f", avgTraversalMs)} ms/run ($traversalRuns runs, $totalTraversed nodes visited)")
        println("   PageRank Total Time     : ${String.format("%.2f", prDurationMs)} ms ($iterations iterations)")
        println("   PageRank Latency/Iter   : ${String.format("%.2f", msPerIteration)} ms/iteration")
        println("   Centrality Top Node     : ${ranks.maxByOrNull { it.value }?.key}")
        println("=================================================================\n")

        assertEquals(nodeCount, ranks.size)
        assertTrue(totalTraversed > 0)
    }

    /**
     * Benchmark 6: Measures multi-threaded MVCC commit throughput and latency.
     */
    @Test
    fun benchmarkMvccTransactionCommit() {
        val threadCount = 4
        val txPerThread = 500
        val totalTx = threadCount * txPerThread

        val executor = Executors.newFixedThreadPool(threadCount)
        val latch = CountDownLatch(threadCount)
        val start = System.nanoTime()

        for (t in 0 until threadCount) {
            executor.submit {
                try {
                    for (i in 0 until txPerThread) {
                        val key = "tx_${t}_$i".toByteArray(Charsets.UTF_8)
                        val value = "val_${t}_$i".toByteArray(Charsets.UTF_8)
                        db.transaction { tx ->
                            tx.putRaw(key, value)
                        }
                    }
                } finally {
                    latch.countDown()
                }
            }
        }

        latch.await(30, TimeUnit.SECONDS)
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        executor.shutdown()

        val throughput = (totalTx / (durationMs / 1000.0)).toInt()
        val avgLatencyMicros = (durationMs * 1000.0) / totalTx

        println("=================================================================")
        println("🔒 BENCHMARK: Multi-Threaded MVCC Transaction Commit")
        println("=================================================================")
        println("   Concurrent Threads     : $threadCount")
        println("   Committed Transactions : $totalTx transactions")
        println("   Execution Duration     : ${String.format("%.2f", durationMs)} ms")
        println("   Commit Throughput      : $throughput tx/sec")
        println("   Average Commit Latency : ${String.format("%.2f", avgLatencyMicros)} μs/tx")
        println("=================================================================\n")

        assertNotNull(db.engine.getRaw("tx_0_0".toByteArray(Charsets.UTF_8)))
        assertTrue(throughput > 0)
    }

    /**
     * Benchmark 7: Measures cold-start initialization latency and JVM heap footprint delta.
     */
    @Test
    fun benchmarkColdStartAndMemoryFootprint() = runBlocking {
        // Pre-populate database with documents, SSTables, and vectors
        val collection = db.collection<BenchmarkDoc>("cold_catalog")
        val batch = (1..2000).associate { i ->
            "item_$i" to BenchmarkDoc("item_$i", "Doc #$i", if (i % 2 == 0) "catA" else "catB", i * 1.5)
        }
        collection.insertBatch(batch)
        db.engine.flushMemTableInternal()

        // Create vector index
        val dims = 64
        val vectorCount = 1000
        val dummyVec = FloatArray(dims) { 0.1f }
        val vCol = db.vectorCollection("cold_vectors") {
            this.dimensions = dims
            this.quantization = false
        }
        val vBatch = (1..vectorCount).associate { "v_$it" to dummyVec }
        vCol.insertBatch(vBatch)
        vCol.waitForIndexing()

        db.close()

        // Settle GC
        System.gc()
        Thread.sleep(500)

        // Measure Cold-Start Reopen
        val heapBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        val start = System.nanoTime()
        val freshDb = KoreDatabase(testDir)
        val freshCol = freshDb.collection<BenchmarkDoc>("cold_catalog")
        val count = freshCol.count()
        val coldStartMs = (System.nanoTime() - start) / 1_000_000.0
        val heapAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        val heapRetainedMb = maxOf(0.0, (heapAfter - heapBefore) / (1024.0 * 1024.0))

        println("=================================================================")
        println("❄️ BENCHMARK: Cold-Start Hydration & Memory Footprint")
        println("=================================================================")
        println("   Cold Start Reopen Time : ${String.format("%.2f", coldStartMs)} ms")
        println("   Hydrated Documents     : $count")
        println("   JVM Heap Delta         : ${String.format("%.2f", heapRetainedMb)} MB")
        println("=================================================================\n")

        freshDb.close()
        // Reopen for tearDown
        db = KoreDatabase(testDir)
        assertEquals(2000, count)
        assertTrue(coldStartMs > 0.0)
    }

    /**
     * Benchmark 8: Measures field filtering and Top-K sorting latency with limit.
     */
    @Test
    fun benchmarkQueryFilteringAndSorting() = runBlocking {
        val col = db.collection<BenchmarkDoc>("query_bench")
        col.registerProperty("category") { it.category }
        col.registerProperty("score") { it.score.toString() }

        val docCount = 5000
        val batch = (1..docCount).associate { i ->
            "item_$i" to BenchmarkDoc(
                id = "item_$i",
                name = "Item $i",
                category = if (i % 5 == 0) "premium" else "standard",
                score = (i % 1000).toDouble()
            )
        }
        col.insertBatch(batch)
        db.engine.flushMemTableInternal()

        // 1. Filtered Query
        val filterStart = System.nanoTime()
        val filtered = col.query().where("category") { it == "premium" }.execute()
        val filterDurationMs = (System.nanoTime() - filterStart) / 1_000_000.0

        // 2. Full scan sort vs Top-K sort with limit(10)
        val sortLimitStart = System.nanoTime()
        val topK = col.query()
            .where("category") { it == "premium" }
            .sortBy("score", descending = true) { it.toDouble() }
            .limit(10)
            .execute()
        val sortLimitDurationMs = (System.nanoTime() - sortLimitStart) / 1_000_000.0

        println("=================================================================")
        println("📊 BENCHMARK: Query Filtering & Top-K Sorting ($docCount Documents)")
        println("=================================================================")
        println("   Filter Execution Time  : ${String.format("%.2f", filterDurationMs)} ms (Matches: ${filtered.size})")
        println("   Sort + Limit(10) Time  : ${String.format("%.2f", sortLimitDurationMs)} ms (Top: ${topK.firstOrNull()?.score})")
        println("=================================================================\n")

        assertEquals(1000, filtered.size)
        assertEquals(10, topK.size)
    }
}
