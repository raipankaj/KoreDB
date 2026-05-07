package com.pankaj.koredb

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.core.VectorSerializer
import com.pankaj.koredb.hnsw.*
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.random.Random
import kotlin.system.measureTimeMillis

/**
 * Comprehensive benchmark comparing KoreDB's vector engine against Room (SQLite)
 * for all new vector features: insert, search, delete, update, hybrid search,
 * multi-distance metrics, quantization, and metadata filtering.
 */
@RunWith(AndroidJUnit4::class)
class VectorBenchmark {

    private lateinit var app: MyApplication
    private val DIM = 128
    private val VECTOR_COUNT = 5_000
    private val SEARCH_RUNS = 50
    private val CATEGORIES = listOf("electronics", "clothing", "shoes", "books", "food")

    @Before
    fun setup() = runBlocking {
        app = ApplicationProvider.getApplicationContext<MyApplication>()
        app.roomDatabase.vectorDao().deleteAll()
    }

    // ====================================================================
    // HELPERS
    // ====================================================================

    private fun randomVector() = FloatArray(DIM) { Random.nextFloat() }

    private fun generateData(count: Int): Map<String, FloatArray> {
        return (1..count).associate { "vec_$it" to randomVector() }
    }

    private fun generateMetadata(id: String, index: Int): Map<String, Any> = mapOf(
        "category" to CATEGORIES[index % CATEGORIES.size],
        "price" to (10.0 + (index % 500)),
        "label" to "item_$index"
    )

    private fun printResult(label: String, koreMs: Long, roomMs: Long) {
        val winner = if (koreMs <= roomMs) "✅ KoreDB" else "⚠️ Room"
        val ratio = if (roomMs > 0) "%.1fx".format(roomMs.toFloat() / koreMs) else "N/A"
        println("  $label → KoreDB: ${koreMs}ms | Room: ${roomMs}ms | $winner ($ratio)")
    }

    // ====================================================================
    // 1. BATCH INSERT BENCHMARK
    // ====================================================================

    @Test
    fun benchmark_01_BatchInsert() = runBlocking {
        println("\n\n🚀 ═══════════════════════════════════════════════════")
        println("   BATCH INSERT ($VECTOR_COUNT vectors, ${DIM}d)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val koreVec = app.database.vectorCollection("bench_insert") {
            dimensions = DIM
            metric = DistanceMetric.COSINE
        }

        val koreTime = measureTimeMillis {
            koreVec.insertBatch(data)
        }

        val roomTime = measureTimeMillis {
            val entities = data.map { (id, vec) ->
                VectorEntity(id, VectorSerializer.toByteArray(vec))
            }
            app.roomDatabase.vectorDao().insertAll(entities)
        }

        printResult("INSERT", koreTime, roomTime)

        // Cleanup
        koreVec.waitForIndexing()
        println("   HNSW Index Stats: ${koreVec.stats()}")
    }

    // ====================================================================
    // 2. SEARCH (HNSW vs Brute-Force SQLite)
    // ====================================================================

    @Test
    fun benchmark_02_Search() = runBlocking {
        println("\n\n🔍 ═══════════════════════════════════════════════════")
        println("   SEARCH ($VECTOR_COUNT vectors, $SEARCH_RUNS queries)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val query = randomVector()

        val koreVec = app.database.vectorCollection("bench_search") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data)
        koreVec.waitForIndexing()

        val dao = app.roomDatabase.vectorDao()
        dao.deleteAll()
        dao.insertAll(data.map { VectorEntity(it.key, VectorSerializer.toByteArray(it.value)) })

        // KoreDB HNSW search
        val koreTime = measureTimeMillis {
            repeat(SEARCH_RUNS) { koreVec.search(query, limit = 10) }
        }

        // Room brute-force search
        val roomTime = measureTimeMillis {
            val qMag = VectorMath.getMagnitude(query)
            repeat(SEARCH_RUNS) {
                val all = dao.getAll()
                all.map { entity ->
                    val buf = java.nio.ByteBuffer.wrap(entity.blob).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    entity.id to VectorMath.cosineSimilarity(query, qMag, buf, 0, DIM)
                }.sortedByDescending { it.second }.take(10)
            }
        }

        printResult("SEARCH (top-10)", koreTime, roomTime)
    }

    // ====================================================================
    // 3. HYBRID SEARCH (Vector + Metadata Filtering)
    // ====================================================================

    @Test
    fun benchmark_03_HybridSearch() = runBlocking {
        println("\n\n🧬 ═══════════════════════════════════════════════════")
        println("   HYBRID SEARCH (vector + metadata filter)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val metadataMap = data.entries.mapIndexed { i, (id, _) ->
            id to generateMetadata(id, i)
        }.toMap()

        val koreVec = app.database.vectorCollection("bench_hybrid") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data, metadataMap)
        koreVec.waitForIndexing()

        val dao = app.roomDatabase.vectorDao()
        dao.deleteAll()
        dao.insertAll(data.entries.mapIndexed { i, (id, vec) ->
            VectorEntity(id, VectorSerializer.toByteArray(vec),
                category = CATEGORIES[i % CATEGORIES.size],
                price = 10.0 + (i % 500),
                label = "item_$i")
        })

        val query = randomVector()
        val targetCategory = "shoes"

        // KoreDB: HNSW with pre-filter
        val koreTime = measureTimeMillis {
            repeat(SEARCH_RUNS) {
                koreVec.search(query, limit = 10) {
                    where("category", eq(targetCategory))
                }
            }
        }

        // Room: Query category first, then brute-force similarity
        val roomTime = measureTimeMillis {
            val qMag = VectorMath.getMagnitude(query)
            repeat(SEARCH_RUNS) {
                val filtered = dao.getByCategory(targetCategory)
                filtered.map { entity ->
                    val buf = java.nio.ByteBuffer.wrap(entity.blob).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    entity.id to VectorMath.cosineSimilarity(query, qMag, buf, 0, DIM)
                }.sortedByDescending { it.second }.take(10)
            }
        }

        printResult("HYBRID SEARCH", koreTime, roomTime)
    }

    // ====================================================================
    // 4. DELETE BENCHMARK
    // ====================================================================

    @Test
    fun benchmark_04_Delete() = runBlocking {
        println("\n\n🗑️ ═══════════════════════════════════════════════════")
        println("   DELETE (1000 vectors from $VECTOR_COUNT)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val deleteIds = data.keys.take(1000)

        val koreVec = app.database.vectorCollection("bench_delete") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data)
        koreVec.waitForIndexing()

        val dao = app.roomDatabase.vectorDao()
        dao.deleteAll()
        dao.insertAll(data.map { VectorEntity(it.key, VectorSerializer.toByteArray(it.value)) })

        val koreTime = measureTimeMillis {
            koreVec.deleteBatch(deleteIds.toList())
        }

        val roomTime = measureTimeMillis {
            deleteIds.forEach { dao.deleteById(it) }
        }

        printResult("DELETE (x1000)", koreTime, roomTime)
    }

    // ====================================================================
    // 5. UPDATE BENCHMARK
    // ====================================================================

    @Test
    fun benchmark_05_Update() = runBlocking {
        println("\n\n✏️ ═══════════════════════════════════════════════════")
        println("   UPDATE (500 vectors)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val updateIds = data.keys.take(500)

        val koreVec = app.database.vectorCollection("bench_update") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data)
        koreVec.waitForIndexing()

        val dao = app.roomDatabase.vectorDao()
        dao.deleteAll()
        dao.insertAll(data.map { VectorEntity(it.key, VectorSerializer.toByteArray(it.value)) })

        val koreTime = measureTimeMillis {
            for (id in updateIds) {
                koreVec.update(id, randomVector(), mapOf("updated" to true))
            }
        }

        val roomTime = measureTimeMillis {
            for (id in updateIds) {
                dao.update(VectorEntity(id, VectorSerializer.toByteArray(randomVector())))
            }
        }

        printResult("UPDATE (x500)", koreTime, roomTime)
    }

    // ====================================================================
    // 6. DISTANCE METRICS BENCHMARK
    // ====================================================================

    @Test
    fun benchmark_06_DistanceMetrics() = runBlocking {
        println("\n\n📐 ═══════════════════════════════════════════════════")
        println("   DISTANCE METRICS COMPARISON ($VECTOR_COUNT vectors)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val query = randomVector()
        val metrics = listOf(
            DistanceMetric.COSINE,
            DistanceMetric.EUCLIDEAN,
            DistanceMetric.INNER_PRODUCT,
            DistanceMetric.MANHATTAN
        )

        for (metric in metrics) {
            val collName = "bench_metric_${metric.name.lowercase()}"
            val koreVec = app.database.vectorCollection(collName) {
                dimensions = DIM; this.metric = metric
            }
            koreVec.insertBatch(data)
            koreVec.waitForIndexing()

            val searchTime = measureTimeMillis {
                repeat(SEARCH_RUNS) { koreVec.search(query, limit = 10) }
            }

            println("  ${metric.name.padEnd(15)} → Search ($SEARCH_RUNS queries): ${searchTime}ms")
        }
    }

    // ====================================================================
    // 7. SCALAR QUANTIZATION BENCHMARK
    // ====================================================================

    @Test
    fun benchmark_07_Quantization() = runBlocking {
        println("\n\n📦 ═══════════════════════════════════════════════════")
        println("   QUANTIZATION: FP32 vs SQ8 ($VECTOR_COUNT vectors)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val query = randomVector()

        // FP32 (no quantization)
        val fp32Vec = app.database.vectorCollection("bench_fp32") {
            dimensions = DIM; metric = DistanceMetric.COSINE; quantization = false
        }
        fp32Vec.insertBatch(data)
        fp32Vec.waitForIndexing()

        // SQ8 (quantized)
        val sq8Vec = app.database.vectorCollection("bench_sq8") {
            dimensions = DIM; metric = DistanceMetric.COSINE; quantization = true
        }
        sq8Vec.insertBatch(data)
        sq8Vec.waitForIndexing()

        val fp32Time = measureTimeMillis {
            repeat(SEARCH_RUNS) { fp32Vec.search(query, limit = 10) }
        }

        val sq8Time = measureTimeMillis {
            repeat(SEARCH_RUNS) { sq8Vec.search(query, limit = 10) }
        }

        // Compare results for recall
        val fp32Results = fp32Vec.search(query, limit = 10).map { it.first }.toSet()
        val sq8Results = sq8Vec.search(query, limit = 10).map { it.first }.toSet()
        val recall = fp32Results.intersect(sq8Results).size.toFloat() / fp32Results.size

        println("  FP32  → Search ($SEARCH_RUNS queries): ${fp32Time}ms")
        println("  SQ8   → Search ($SEARCH_RUNS queries): ${sq8Time}ms")
        println("  Memory: FP32=${VECTOR_COUNT * DIM * 4 / 1024}KB | SQ8=${VECTOR_COUNT * DIM / 1024}KB")
        println("  Recall@10: ${(recall * 100).toInt()}%")
    }

    // ====================================================================
    // 8. METADATA-ONLY UPDATE
    // ====================================================================

    @Test
    fun benchmark_08_MetadataUpdate() = runBlocking {
        println("\n\n🏷️ ═══════════════════════════════════════════════════")
        println("   METADATA-ONLY UPDATE (1000 ops)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val metadataMap = data.entries.mapIndexed { i, (id, _) ->
            id to generateMetadata(id, i)
        }.toMap()

        val koreVec = app.database.vectorCollection("bench_meta_update") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data, metadataMap)
        koreVec.waitForIndexing()

        val dao = app.roomDatabase.vectorDao()
        dao.deleteAll()
        dao.insertAll(data.entries.mapIndexed { i, (id, vec) ->
            VectorEntity(id, VectorSerializer.toByteArray(vec),
                category = CATEGORIES[i % CATEGORIES.size],
                price = 10.0 + (i % 500))
        })

        val updateIds = data.keys.take(1000)

        // KoreDB: metadata-only update (no re-indexing)
        val koreTime = measureTimeMillis {
            for (id in updateIds) {
                koreVec.updateMetadata(id, mapOf("category" to "updated", "price" to 999.0))
            }
        }

        // Room: column update
        val roomTime = measureTimeMillis {
            for (id in updateIds) {
                val existing = dao.getById(id) ?: return@measureTimeMillis
                dao.update(existing.copy(category = "updated", price = 999.0))
            }
        }

        printResult("META UPDATE (x1000)", koreTime, roomTime)
    }

    // ====================================================================
    // 9. INDEX COMPACTION
    // ====================================================================

    @Test
    fun benchmark_09_Compaction() = runBlocking {
        println("\n\n🧹 ═══════════════════════════════════════════════════")
        println("   INDEX COMPACTION (delete 2000 then compact)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val koreVec = app.database.vectorCollection("bench_compact") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data)
        koreVec.waitForIndexing()

        val statsBefore = koreVec.stats()
        println("  Before: ${statsBefore.totalNodes} nodes, ${statsBefore.totalEdges} edges")

        // Delete 2000 vectors
        koreVec.deleteBatch(data.keys.take(2000).toList())

        val statsAfterDelete = koreVec.stats()
        println("  After delete: ${statsAfterDelete.totalNodes} active, ${statsAfterDelete.deletedNodes} tombstoned")

        val compactTime = measureTimeMillis {
            koreVec.compactIndex()
        }

        val statsAfterCompact = koreVec.stats()
        println("  After compact: ${statsAfterCompact.totalNodes} nodes, ${statsAfterCompact.totalEdges} edges")
        println("  Compaction time: ${compactTime}ms")

        // Verify search still works
        val results = koreVec.search(randomVector(), limit = 5)
        println("  Post-compact search returns ${results.size} results ✅")
    }

    // ====================================================================
    // 10. NAMESPACE ISOLATION
    // ====================================================================

    @Test
    fun benchmark_10_NamespaceIsolation() = runBlocking {
        println("\n\n🏗️ ═══════════════════════════════════════════════════")
        println("   NAMESPACE ISOLATION TEST")
        println("═══════════════════════════════════════════════════════")

        val nsA = app.database.vectorCollection("products") {
            dimensions = DIM; namespace = "tenant_A"
        }
        val nsB = app.database.vectorCollection("products") {
            dimensions = DIM; namespace = "tenant_B"
        }

        val dataA = (1..100).associate { "a_$it" to randomVector() }
        val dataB = (1..200).associate { "b_$it" to randomVector() }

        nsA.insertBatch(dataA)
        nsB.insertBatch(dataB)
        nsA.waitForIndexing()
        nsB.waitForIndexing()

        val query = randomVector()
        val resultsA = nsA.search(query, limit = 5)
        val resultsB = nsB.search(query, limit = 5)

        // Verify isolation
        val aIds = resultsA.map { it.first }.toSet()
        val bIds = resultsB.map { it.first }.toSet()
        val isolated = aIds.all { it.startsWith("a_") } && bIds.all { it.startsWith("b_") }

        println("  Namespace A: ${nsA.size()} vectors, search returned ${resultsA.size}")
        println("  Namespace B: ${nsB.size()} vectors, search returned ${resultsB.size}")
        println("  Isolation: ${if (isolated) "✅ PASS" else "❌ FAIL"}")
    }

    // ====================================================================
    // 11. FULL REPORT
    // ====================================================================

    @Test
    fun benchmark_11_FullReport() = runBlocking {
        println("\n\n" + "═".repeat(60))
        println("  KOREDB VECTOR ENGINE — FULL BENCHMARK REPORT")
        println("  Vectors: $VECTOR_COUNT | Dimensions: $DIM | Queries: $SEARCH_RUNS")
        println("═".repeat(60))

        val data = generateData(VECTOR_COUNT)
        val query = randomVector()
        val metadataMap = data.entries.mapIndexed { i, (id, _) ->
            id to generateMetadata(id, i)
        }.toMap()

        // --- Setup KoreDB ---
        val koreVec = app.database.vectorCollection("bench_full") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }

        // --- Setup Room ---
        val dao = app.roomDatabase.vectorDao()
        dao.deleteAll()

        // 1. INSERT
        val koreInsert = measureTimeMillis {
            koreVec.insertBatch(data, metadataMap)
        }
        koreVec.waitForIndexing()

        val roomInsert = measureTimeMillis {
            dao.insertAll(data.entries.mapIndexed { i, (id, vec) ->
                VectorEntity(id, VectorSerializer.toByteArray(vec),
                    category = CATEGORIES[i % CATEGORIES.size],
                    price = 10.0 + (i % 500))
            })
        }

        // 2. SEARCH
        val koreSearch = measureTimeMillis {
            repeat(SEARCH_RUNS) { koreVec.search(query, limit = 10) }
        }

        val roomSearch = measureTimeMillis {
            val qMag = VectorMath.getMagnitude(query)
            repeat(SEARCH_RUNS) {
                dao.getAll().map { e ->
                    val buf = java.nio.ByteBuffer.wrap(e.blob).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    e.id to VectorMath.cosineSimilarity(query, qMag, buf, 0, DIM)
                }.sortedByDescending { it.second }.take(10)
            }
        }

        // 3. HYBRID SEARCH
        val koreHybrid = measureTimeMillis {
            repeat(SEARCH_RUNS) {
                koreVec.search(query, limit = 10) {
                    where("category", eq("shoes"))
                }
            }
        }

        val roomHybrid = measureTimeMillis {
            val qMag = VectorMath.getMagnitude(query)
            repeat(SEARCH_RUNS) {
                dao.getByCategory("shoes").map { e ->
                    val buf = java.nio.ByteBuffer.wrap(e.blob).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    e.id to VectorMath.cosineSimilarity(query, qMag, buf, 0, DIM)
                }.sortedByDescending { it.second }.take(10)
            }
        }

        // 4. DELETE
        val deleteIds = data.keys.take(500).toList()
        val koreDelete = measureTimeMillis { koreVec.deleteBatch(deleteIds) }
        val roomDelete = measureTimeMillis { deleteIds.forEach { dao.deleteById(it) } }

        // 5. UPDATE
        val updateIds = data.keys.drop(500).take(500).toList()
        val koreUpdate = measureTimeMillis {
            for (id in updateIds) koreVec.update(id, randomVector())
        }
        val roomUpdate = measureTimeMillis {
            for (id in updateIds) dao.update(VectorEntity(id, VectorSerializer.toByteArray(randomVector())))
        }

        // --- PRINT REPORT ---
        println("\n  ┌─────────────────────────┬────────────┬────────────┐")
        println("  │ Operation               │ KoreDB     │ Room       │")
        println("  ├─────────────────────────┼────────────┼────────────┤")
        println("  │ INSERT (${VECTOR_COUNT} vectors)   │ ${koreInsert.toString().padStart(7)}ms  │ ${roomInsert.toString().padStart(7)}ms  │")
        println("  │ SEARCH (top-10 × $SEARCH_RUNS)   │ ${koreSearch.toString().padStart(7)}ms  │ ${roomSearch.toString().padStart(7)}ms  │")
        println("  │ HYBRID SEARCH (× $SEARCH_RUNS)   │ ${koreHybrid.toString().padStart(7)}ms  │ ${roomHybrid.toString().padStart(7)}ms  │")
        println("  │ DELETE (× 500)          │ ${koreDelete.toString().padStart(7)}ms  │ ${roomDelete.toString().padStart(7)}ms  │")
        println("  │ UPDATE (× 500)          │ ${koreUpdate.toString().padStart(7)}ms  │ ${roomUpdate.toString().padStart(7)}ms  │")
        println("  └─────────────────────────┴────────────┴────────────┘")
        println("\n  Index Stats: ${koreVec.stats()}")
        println("═".repeat(60) + "\n")
    }
}
