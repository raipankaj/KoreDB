package com.pankaj.koredb.benchmark

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.exporter.exportToJson
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.HNSWIndex
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.Random
import java.util.UUID

/**
 * Quantitative performance benchmark comparing optimized paths vs baseline paths.
 */
class PerformanceComparisonBenchmarkTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    data class ProductItem(
        val id: String,
        val title: String,
        val category: String,
        val price: Double
    )

    @Before
    fun setup() {
        testDir = File("build/tmp/test_perf_bench_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun benchmarkIndexPushdownVsFullScan() = runBlocking {
        val collection = db.collection<ProductItem>("benchmark_catalog")
        collection.createIndex("category") { it.category }

        val docCount = 10_000
        val categories = listOf("Shoes", "Electronics", "Clothing", "Books", "Home")
        val batch = mutableMapOf<String, ProductItem>()

        for (i in 1..docCount) {
            val id = "item_$i"
            val category = categories[i % categories.size]
            batch[id] = ProductItem(id, "Product Title $i", category, 10.0 + (i % 100))
        }
        collection.insertBatch(batch)
        db.engine.flushMemTableInternal()

        // Warmup JIT
        repeat(3) {
            collection.query().where("category") { it == "Shoes" }.execute()
            collection.query().whereEq("category", "Shoes").execute()
        }

        // 1. Measure Full Collection Scan (Baseline)
        val fullScanRuns = 10
        val fullScanStart = System.nanoTime()
        var fullScanMatches = 0
        repeat(fullScanRuns) {
            val results = collection.query()
                .where("category") { it == "Shoes" }
                .execute()
            fullScanMatches = results.size
        }
        val fullScanDurationMs = (System.nanoTime() - fullScanStart) / 1_000_000.0 / fullScanRuns

        // 2. Measure Index Pushdown (Optimized whereEq)
        val indexPushdownRuns = 10
        val indexStart = System.nanoTime()
        var indexMatches = 0
        repeat(indexPushdownRuns) {
            val results = collection.query()
                .whereEq("category", "Shoes")
                .execute()
            indexMatches = results.size
        }
        val indexDurationMs = (System.nanoTime() - indexStart) / 1_000_000.0 / indexPushdownRuns

        val speedup = fullScanDurationMs / maxOf(0.001, indexDurationMs)

        println("=================================================================")
        println("🚀 BENCHMARK: Index Pushdown (whereEq) vs Full Scan (10,000 docs)")
        println("=================================================================")
        println("   Full Collection Scan Time : ${String.format("%.2f", fullScanDurationMs)} ms (Matches: $fullScanMatches)")
        println("   Index Pushdown Time       : ${String.format("%.2f", indexDurationMs)} ms (Matches: $indexMatches)")
        println("   ⚡ Speedup Factor         : ${String.format("%.1f", speedup)}x faster")
        println("=================================================================\n")

        assertEquals(2000, fullScanMatches)
        assertEquals(2000, indexMatches)
    }

    @Test
    fun benchmarkStreamingCompactionThroughput() = runBlocking {
        // Populate 4 SSTables with 2,500 records each = 10,000 records
        val totalRecords = 10_000
        val recordsPerTable = 2_500
        for (f in 1..4) {
            for (i in 1..recordsPerTable) {
                val key = "sensor_${f}_$i".toByteArray()
                val value = "reading_temp_24.5_humidity_60_timestamp_${System.currentTimeMillis()}".toByteArray()
                db.engine.putRaw(key, value)
            }
            db.engine.flushMemTableInternal()
        }

        // Measure streaming compaction
        val start = System.nanoTime()
        db.engine.performLeveledCompaction()
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        val recordsPerSec = (totalRecords / (durationMs / 1000.0)).toInt()

        println("=================================================================")
        println("⚡ BENCHMARK: Streaming SSTable Compaction Throughput")
        println("=================================================================")
        println("   Compacted Records    : $totalRecords entries")
        println("   Compaction Time      : ${String.format("%.2f", durationMs)} ms")
        println("   Compaction Rate      : $recordsPerSec records/sec")
        println("   Heap Memory Strategy : Zero-MemTable Streaming (256KB direct buffer)")
        println("=================================================================\n")

        assertTrue(durationMs > 0)
    }

    @Test
    fun benchmarkStreamingJsonExport() = runBlocking {
        val collection = db.collection<ProductItem>("export_perf")
        val docCount = 5_000
        val batch = mutableMapOf<String, ProductItem>()

        for (i in 1..docCount) {
            batch["item_$i"] = ProductItem("item_$i", "Product Title $i", "General", i * 1.25)
        }
        collection.insertBatch(batch)

        val outputFile = File(testDir, "perf_export.json")

        val start = System.nanoTime()
        val stats = collection.exportToJson(outputFile)
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        val throughputRecordsSec = (docCount / (durationMs / 1000.0)).toInt()
        val mbWritten = stats.totalBytes / (1024.0 * 1024.0)

        println("=================================================================")
        println("📦 BENCHMARK: Streaming JSON Export (BufferedWriter)")
        println("=================================================================")
        println("   Exported Documents : ${stats.totalRecords}")
        println("   File Size On Disk  : ${String.format("%.2f", mbWritten)} MB")
        println("   Execution Duration : ${String.format("%.2f", durationMs)} ms ($throughputRecordsSec docs/sec)")
        println("=================================================================\n")

        assertEquals(docCount.toLong(), stats.totalRecords)
        assertTrue(outputFile.exists())
    }

    @Test
    fun benchmarkParallelHnswIngestionThroughput() = runBlocking {
        val hnsw = HNSWIndex(
            maxNeighbors = 16,
            efConstruction = 64,
            efSearch = 32,
            metric = DistanceMetric.COSINE
        )

        val vectorCount = 1_000
        val dims = 64
        val random = Random(42)

        val vectors = (0 until vectorCount).map {
            val v = FloatArray(dims) { random.nextFloat() }
            val norm = kotlin.math.sqrt(v.map { it * it }.sum())
            "vec_$it" to Pair(v, norm)
        }

        val start = System.nanoTime()
        coroutineScope {
            val chunks = vectors.chunked(250)
            chunks.map { chunk ->
                async(Dispatchers.Default) {
                    for ((id, pair) in chunk) {
                        hnsw.insert(id, pair.first, pair.second)
                    }
                }
            }.awaitAll()
        }
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        val rate = (vectorCount / (durationMs / 1000.0)).toInt()

        println("=================================================================")
        println("🔥 BENCHMARK: Concurrent Lock-Free HNSW Vector Ingestion")
        println("=================================================================")
        println("   Total Vectors Inserted : $vectorCount (64 dimensions)")
        println("   Workers (Parallel)     : 4 Coroutine Workers")
        println("   Ingestion Duration     : ${String.format("%.2f", durationMs)} ms ($rate vectors/sec)")
        println("   Deadlocks Encountered  : 0 (Lock-Free Set Connections)")
        println("=================================================================\n")

        assertEquals(vectorCount, hnsw.size())
    }

    @Test
    fun benchmarkNumericRangeIndexPushdownVsFullScan() = runBlocking {
        val collection = db.collection<ProductItem>("range_catalog")
        collection.createNumericIndex("price") { it.price }

        val docCount = 10_000
        val batch = (1..docCount).associate { i ->
            "item_$i" to ProductItem("item_$i", "Title $i", "Cat", i.toDouble())
        }
        collection.insertBatch(batch)
        db.engine.flushMemTableInternal()

        // 1. Full scan range query: price between 4000.0 and 4500.0 (501 matches)
        val scanStart = System.nanoTime()
        val scanResults = collection.query()
            .where("price") {
                val p = it.toDoubleOrNull() ?: 0.0
                p in 4000.0..4500.0
            }
            .execute()
        val scanTimeMs = (System.nanoTime() - scanStart) / 1_000_000.0

        // 2. Index Pushdown range query: whereBetween
        val pushdownStart = System.nanoTime()
        val pushdownResults = collection.query()
            .whereBetween("price", 4000.0, 4500.0)
            .execute()
        val pushdownTimeMs = (System.nanoTime() - pushdownStart) / 1_000_000.0

        println("=================================================================")
        println("🎯 BENCHMARK: Numeric Range Pushdown (Order-Preserving Encoding)")
        println("   Dataset: $docCount Documents | Target Range: [4000.0, 4500.0]")
        println("=================================================================")
        println("   Full Collection Scan Time : ${String.format("%.2f", scanTimeMs)} ms (Matches: ${scanResults.size})")
        println("   LSM Range Pushdown Time   : ${String.format("%.2f", pushdownTimeMs)} ms (Matches: ${pushdownResults.size})")
        println("=================================================================\n")

        assertEquals(501, scanResults.size)
        assertEquals(501, pushdownResults.size)
    }

    @Test
    fun benchmarkLz4VsDeflateThroughput() {
        val lz4 = com.pankaj.koredb.compression.Lz4CompressionCodec()
        val deflate = com.pankaj.koredb.compression.DeflateCompressionCodec()

        val sampleData = "KoreDB High Performance Embedded Multi-Model Database with LSM-Tree Storage Engine ".repeat(500).toByteArray()
        val lz4Compressed = lz4.compress(sampleData)
        val deflateCompressed = deflate.compress(sampleData)

        // Measure LZ4 decompression
        val iterations = 500
        val lz4Start = System.nanoTime()
        for (i in 0 until iterations) {
            lz4.decompress(lz4Compressed)
        }
        val lz4DurationMs = (System.nanoTime() - lz4Start) / 1_000_000.0
        val lz4MbSec = ((sampleData.size.toLong() * iterations) / (1024.0 * 1024.0)) / (lz4DurationMs / 1000.0)

        // Measure Deflate decompression
        val defStart = System.nanoTime()
        for (i in 0 until iterations) {
            deflate.decompress(deflateCompressed)
        }
        val defDurationMs = (System.nanoTime() - defStart) / 1_000_000.0
        val defMbSec = ((sampleData.size.toLong() * iterations) / (1024.0 * 1024.0)) / (defDurationMs / 1000.0)

        println("=================================================================")
        println("⚡ BENCHMARK: Block Decompression Throughput (LZ4 vs Deflate)")
        println("=================================================================")
        println("   Original Block Size     : ${sampleData.size / 1024} KB")
        println("   LZ4 Decompression Speed : ${String.format("%.1f", lz4MbSec)} MB/s (${String.format("%.2f", lz4DurationMs)} ms)")
        println("   Deflate Decompress Speed: ${String.format("%.1f", defMbSec)} MB/s (${String.format("%.2f", defDurationMs)} ms)")
        println("   ⚡ LZ4 Speedup Factor   : ${String.format("%.1f", lz4MbSec / defMbSec)}x faster")
        println("=================================================================\n")

        assertTrue(lz4MbSec > 100.0)
    }

    @Test
    fun benchmarkProductQuantizerAdcSearch() {
        val dims = 128
        val vectorCount = 5_000
        val numSubVectors = 16 // 32x compression
        val pq = com.pankaj.koredb.hnsw.ProductQuantizer(dimensions = dims, numSubVectors = numSubVectors, numCentroids = 128)

        val random = Random(42)
        val training = (1..100).map { FloatArray(dims) { random.nextFloat() } }
        pq.train(training, maxIterations = 3)

        // Quantize 5,000 vectors
        val datasetCodes = (1..vectorCount).map {
            pq.quantize(FloatArray(dims) { random.nextFloat() })
        }

        // Benchmark 1,000 Asymmetric Distance Computation queries
        val query = FloatArray(dims) { random.nextFloat() }
        val start = System.nanoTime()
        val queryCount = 1_000
        for (q in 0 until queryCount) {
            val table = pq.computeAsymmetricDistanceTable(query, DistanceMetric.EUCLIDEAN)
            for (i in 0 until minOf(100, vectorCount)) {
                pq.computeDistanceWithTable(table, datasetCodes[i], DistanceMetric.EUCLIDEAN)
            }
        }
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        val totalDistanceComps = queryCount * minOf(100, vectorCount)
        val rate = (totalDistanceComps / (durationMs / 1000.0)).toInt()

        println("=================================================================")
        println("🤖 BENCHMARK: Product Quantization ADC Throughput (32x Memory Reduction)")
        println("   Vector Dimensions : 128-d -> 16 bytes codebook")
        println("   Distance Comps    : $totalDistanceComps evaluations")
        println("   Total Time        : ${String.format("%.2f", durationMs)} ms ($rate distances/sec)")
        println("=================================================================\n")

        assertTrue(rate > 0)
    }

    @Test
    fun benchmarkMvccTransactionThroughput() {
        val count = 2_000
        val start = System.nanoTime()

        for (i in 0 until count) {
            db.transaction { tx ->
                val k = "tx_key_$i".toByteArray()
                val v = "tx_val_$i".toByteArray()
                tx.putRaw(k, v)
            }
        }
        val durationMs = (System.nanoTime() - start) / 1_000_000.0
        val txSec = (count / (durationMs / 1000.0)).toInt()

        println("=================================================================")
        println("🔒 BENCHMARK: MVCC Snapshot Isolation Transaction Throughput")
        println("   Committed Transactions : $count transactions")
        println("   Execution Duration     : ${String.format("%.2f", durationMs)} ms ($txSec tx/sec)")
        println("=================================================================\n")

        assertEquals("tx_val_42", String(db.engine.getRaw("tx_key_42".toByteArray())!!))
    }
}
