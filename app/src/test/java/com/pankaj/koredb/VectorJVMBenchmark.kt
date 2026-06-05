package com.pankaj.koredb

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.core.VectorSerializer
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.hnsw.*
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.graph.query.query
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.UUID
import kotlin.random.Random
import kotlin.system.measureTimeMillis

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [33])
class VectorJVMBenchmark {

    private lateinit var context: android.content.Context
    private lateinit var koreDb: KoreDatabase
    private lateinit var roomDb: AppDatabase
    private lateinit var testDir: File

    private val DIM = 128
    private val VECTOR_COUNT = 1000
    private val SEARCH_RUNS = 20
    private val CATEGORIES = listOf("electronics", "clothing", "shoes", "books", "food")

    @Before
    fun setup() = runBlocking {
        context = ApplicationProvider.getApplicationContext()
        testDir = File("build/tmp/kore_jvm_bench_${UUID.randomUUID()}")
        testDir.mkdirs()
        koreDb = KoreDatabase(testDir)
        roomDb = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries()
            .build()
    }

    @After
    fun tearDown() {
        koreDb.close()
        roomDb.close()
        testDir.deleteRecursively()
    }

    private fun randomVector() = FloatArray(DIM) { Random.nextFloat() }

    private fun generateData(count: Int): Map<String, FloatArray> {
        return (1..count).associate { "vec_$it" to randomVector() }
    }

    private fun generateMetadata(index: Int): Map<String, Any> = mapOf(
        "category" to CATEGORIES[index % CATEGORIES.size],
        "price" to (10.0 + (index % 500)),
        "label" to "item_$index"
    )

    private fun printResult(label: String, koreMs: Long, roomMs: Long) {
        val winner = if (koreMs <= roomMs) "✅ KoreDB" else "⚠️ Room"
        val ratio = if (roomMs > 0) "%.1fx".format(roomMs.toFloat() / koreMs) else "N/A"
        println("  $label → KoreDB: ${koreMs}ms | Room: ${roomMs}ms | $winner ($ratio)")
    }

    @Test
    fun benchmark_01_PointOperations() = runBlocking {
        println("\n\n🎯 ═══════════════════════════════════════════════════")
        println("   POINT OPERATIONS (1-by-1 Read/Write)")
        println("═══════════════════════════════════════════════════════")

        val collection = koreDb.collection("point_notes", Note.serializer())
        val dao = roomDb.noteDao()
        val OPS = 500

        val writeKore = measureTimeMillis {
            repeat(OPS) { i -> collection.insertBatch(mapOf(i.toString() to Note(i.toString(), "T", "B"))) }
        }
        val writeRoom = measureTimeMillis {
            repeat(OPS) { i -> dao.insert(Note(i.toString(), "T", "B")) }
        }
        printResult("Single Writes (x$OPS)", writeKore, writeRoom)

        val readKore = measureTimeMillis {
            repeat(OPS) { i -> collection.getById(i.toString()) }
        }
        val readRoom = measureTimeMillis {
            repeat(OPS) { i -> dao.getById(i.toString()) }
        }
        printResult("Single Reads (x$OPS)", readKore, readRoom)
    }

    @Test
    fun benchmark_02_BulkOperations() = runBlocking {
        println("\n\n🚀 ═══════════════════════════════════════════════════")
        println("   BULK OPERATIONS (Bulk Insert & Scan)")
        println("═══════════════════════════════════════════════════════")

        val collection = koreDb.collection("bulk_notes", Note.serializer())
        val dao = roomDb.noteDao()
        val SIZE = 5000

        val notes = (1..SIZE).map { Note(it.toString(), "Title", "Content") }
        val batchMap = notes.associateBy { it.id }

        val insertKore = measureTimeMillis { collection.insertBatch(batchMap) }
        val insertRoom = measureTimeMillis { dao.insertAll(notes) }
        printResult("Bulk Insert (x$SIZE)", insertKore, insertRoom)

        val scanKore = measureTimeMillis { collection.getAll() }
        val scanRoom = measureTimeMillis { dao.getAll() }
        printResult("Full Table Scan", scanKore, scanRoom)
    }

    @Test
    fun benchmark_03_PrefixAndRangeQueries() = runBlocking {
        println("\n\n📖 ═══════════════════════════════════════════════════")
        println("   PREFIX & RANGE QUERIES")
        println("═══════════════════════════════════════════════════════")

        val collection = koreDb.collection("range_notes", Note.serializer())
        val dao = roomDb.noteDao()
        val SIZE = 2000

        val data = (1..SIZE).map {
            val prefix = if (it % 2 == 0) "userA" else "userB"
            Note("$prefix:$it", "Title", "Body")
        }
        collection.insertBatch(data.associateBy { it.id })
        dao.insertAll(data)

        val prefixKore = measureTimeMillis { repeat(20) { collection.getByIdPrefix("userA:") } }
        val prefixRoom = measureTimeMillis { repeat(20) { dao.getByPrefix("userA:") } }
        printResult("Prefix Scan (userA:, 20 runs)", prefixKore, prefixRoom)

        val startId = "userA:${200}"
        val endId = "userA:${800}"
        val rangeKore = measureTimeMillis { repeat(20) { collection.getByIdRange(startId, endId) } }
        val rangeRoom = measureTimeMillis { repeat(20) { dao.getByIdRange(startId, endId) } }
        printResult("Range Scan (20 runs)", rangeKore, rangeRoom)
    }

    @Test
    fun benchmark_04_Concurrency() = runBlocking {
        println("\n\n🧵 ═══════════════════════════════════════════════════")
        println("   CONCURRENCY (Parallel threads)")
        println("═══════════════════════════════════════════════════════")

        val collection = koreDb.collection("concurrent_notes", Note.serializer())
        val dao = roomDb.noteDao()
        val SIZE = 2000
        val notes = (1..SIZE).map { Note(it.toString(), "Title", "Body") }
        collection.insertBatch(notes.associateBy { it.id })
        dao.insertAll(notes)

        val idsToRead = (1..1000).map { Random.nextInt(1, SIZE).toString() }

        val readKore = measureTimeMillis {
            coroutineScope {
                repeat(4) { launch(Dispatchers.Default) { idsToRead.forEach { collection.getById(it) } } }
            }
        }
        val readRoom = measureTimeMillis {
            coroutineScope {
                repeat(4) { launch(Dispatchers.Default) { idsToRead.forEach { dao.getById(it) } } }
            }
        }
        printResult("Parallel Reads (4 threads)", readKore, readRoom)
    }

    @Test
    fun benchmark_05_VectorOperations() = runBlocking {
        println("\n\n🤖 ═══════════════════════════════════════════════════")
        println("   VECTOR OPERATIONS & SEARCH (HNSW vs SQLite Flat)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val query = randomVector()

        val koreVec = koreDb.vectorCollection("vec_collection") {
            dimensions = DIM
            metric = DistanceMetric.COSINE
        }

        val insertKore = measureTimeMillis {
            koreVec.insertBatch(data)
        }
        koreVec.waitForIndexing()

        val dao = roomDb.vectorDao()
        val insertRoom = measureTimeMillis {
            val entities = data.map { (id, vec) ->
                VectorEntity(id, VectorSerializer.toByteArray(vec))
            }
            dao.insertAll(entities)
        }

        printResult("Vector Insert (x$VECTOR_COUNT)", insertKore, insertRoom)

        val searchKore = measureTimeMillis {
            repeat(SEARCH_RUNS) { koreVec.search(query, limit = 10) }
        }

        val searchRoom = measureTimeMillis {
            val qMag = VectorMath.getMagnitude(query)
            repeat(SEARCH_RUNS) {
                dao.getAll().map { entity ->
                    val buf = ByteBuffer.wrap(entity.blob).order(ByteOrder.LITTLE_ENDIAN)
                    entity.id to VectorMath.cosineSimilarity(query, qMag, buf, 0, DIM)
                }.sortedByDescending { it.second }.take(10)
            }
        }

        printResult("Vector Search (top-10, $SEARCH_RUNS runs)", searchKore, searchRoom)
    }

    @Test
    fun benchmark_06_HybridSearch() = runBlocking {
        println("\n\n🧬 ═══════════════════════════════════════════════════")
        println("   HYBRID SEARCH (Vector + Metadata Filter)")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val metadataMap = data.entries.mapIndexed { i, (id, _) ->
            id to generateMetadata(i)
        }.toMap()

        val koreVec = koreDb.vectorCollection("hybrid_collection") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data, metadataMap)
        koreVec.waitForIndexing()

        val dao = roomDb.vectorDao()
        dao.insertAll(data.entries.mapIndexed { i, (id, vec) ->
            VectorEntity(id, VectorSerializer.toByteArray(vec),
                category = CATEGORIES[i % CATEGORIES.size],
                price = 10.0 + (i % 500),
                label = "item_$i")
        })

        val query = randomVector()
        val targetCategory = "shoes"

        val searchKore = measureTimeMillis {
            repeat(SEARCH_RUNS) {
                koreVec.search(query, limit = 10) {
                    where("category", eq(targetCategory))
                }
            }
        }

        val searchRoom = measureTimeMillis {
            val qMag = VectorMath.getMagnitude(query)
            repeat(SEARCH_RUNS) {
                dao.getByCategory(targetCategory).map { entity ->
                    val buf = ByteBuffer.wrap(entity.blob).order(ByteOrder.LITTLE_ENDIAN)
                    entity.id to VectorMath.cosineSimilarity(query, qMag, buf, 0, DIM)
                }.sortedByDescending { it.second }.take(10)
            }
        }

        printResult("Hybrid Search (shoes, 10 results)", searchKore, searchRoom)
    }

    @Test
    fun benchmark_07_DeleteAndUpdate() = runBlocking {
        println("\n\n🗑️ ═══════════════════════════════════════════════════")
        println("   DELETE & UPDATE OPERATIONS")
        println("═══════════════════════════════════════════════════════")

        val data = generateData(VECTOR_COUNT)
        val deleteIds = data.keys.take(200).toList()
        val updateIds = data.keys.drop(200).take(200).toList()

        val koreVec = koreDb.vectorCollection("del_up_collection") {
            dimensions = DIM; metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(data)
        koreVec.waitForIndexing()

        val dao = roomDb.vectorDao()
        dao.insertAll(data.map { VectorEntity(it.key, VectorSerializer.toByteArray(it.value)) })

        val deleteKore = measureTimeMillis { koreVec.deleteBatch(deleteIds) }
        val deleteRoom = measureTimeMillis { deleteIds.forEach { dao.deleteById(it) } }
        printResult("Delete Batch (x200)", deleteKore, deleteRoom)

        val updateKore = measureTimeMillis {
            for (id in updateIds) {
                koreVec.update(id, randomVector())
            }
        }
        val updateRoom = measureTimeMillis {
            for (id in updateIds) {
                dao.update(VectorEntity(id, VectorSerializer.toByteArray(randomVector())))
            }
        }
        printResult("Update Batch (x200)", updateKore, updateRoom)
    }

    @Test
    fun benchmark_08_GraphTraversal() = runBlocking {
        println("\n\n🕸️ ═══════════════════════════════════════════════════")
        println("   GRAPH & RELATIONAL TRAVERSAL (DSL vs SQL JOIN)")
        println("═══════════════════════════════════════════════════════")

        val graph = koreDb.graph()
        val roomDao = roomDb.noteDao()
        val NODE_COUNT = 500
        val EDGES_PER_NODE = 4

        val buildKore = measureTimeMillis {
            graph.transaction {
                for (i in 1..NODE_COUNT) putNode(Node("u$i", labels = setOf("Person"), properties = mapOf("city" to "City_${i % 10}")))
                for (i in 1..NODE_COUNT) {
                    for (j in 1..EDGES_PER_NODE) {
                        putEdge(Edge("u$i", "u${(i + j) % NODE_COUNT + 1}", "FOLLOWS"))
                    }
                }
            }
        }
        val buildRoom = measureTimeMillis {
            for (i in 1..NODE_COUNT) {
                for (j in 1..EDGES_PER_NODE) {
                    roomDao.insertEdge(EdgeEntity("u$i", "u${(i + j) % NODE_COUNT + 1}", "FOLLOWS"))
                }
            }
        }
        printResult("Graph Build (500 Nodes, 2000 Edges)", buildKore, buildRoom)

        val dslKore = measureTimeMillis {
            repeat(50) {
                graph.query().startingWith("Person", "city", "City_1").outbound("FOLLOWS", hops = 2).toIdList()
            }
        }
        val sqlRoom = measureTimeMillis {
            repeat(50) {
                roomDao.getTwoHopNodes("u10", "FOLLOWS")
            }
        }
        printResult("2-Hop Traversal (50 runs)", dslKore, sqlRoom)

        val prTime = measureTimeMillis {
            GraphAlgorithms.pageRank(graph, (1..200).map { "u$it" }, "FOLLOWS", 5)
        }
        println(String.format("  %-30s | KoreDB: %4d ms | Room: N/A", "PageRank (200 nodes)", prTime))
    }

    @Test
    fun benchmark_09_SQLiteBTreeSplits() = runBlocking {
        println("\n\n⚖️ ═══════════════════════════════════════════════════")
        println("   B-TREE SPLIT OVERHEAD: SEQUENTIAL VS RANDOM KEYS")
        println("═══════════════════════════════════════════════════════")

        val count = 10000
        val sequentialNotes = (1..count).map { Note("seq_${it.toString().padStart(6, '0')}", "Title", "Body") }
        val randomNotes = (1..count).map { Note(UUID.randomUUID().toString(), "Title", "Body") }

        val roomDao = roomDb.noteDao()
        val collection = koreDb.collection("split_notes", Note.serializer())

        // --- JVM Warmup / JIT compilation trigger ---
        repeat(3) {
            roomDao.insertAll((1..1000).map { Note("warm_${it}_$it", "T", "B") })
            roomDao.deleteAll()
            collection.insertBatch((1..1000).associate { "warm_${it}_$it" to Note("warm_${it}_$it", "T", "B") })
            collection.deleteAll()
        }

        // --- Room (SQLite B-Tree) ---
        val roomSeqTimes = mutableListOf<Long>()
        val roomRandTimes = mutableListOf<Long>()

        repeat(5) {
            roomDao.deleteAll()
            roomSeqTimes.add(measureTimeMillis { roomDao.insertAll(sequentialNotes) })

            roomDao.deleteAll()
            roomRandTimes.add(measureTimeMillis { roomDao.insertAll(randomNotes) })
        }

        // Average the last 3 runs (steady state)
        val roomSeqAvg = roomSeqTimes.takeLast(3).average()
        val roomRandAvg = roomRandTimes.takeLast(3).average()

        // --- KoreDB (LSM Engine) ---
        val koreSeqTimes = mutableListOf<Long>()
        val koreRandTimes = mutableListOf<Long>()

        repeat(5) {
            collection.deleteAll()
            koreSeqTimes.add(measureTimeMillis { collection.insertBatch(sequentialNotes.associateBy { it.id }) })

            collection.deleteAll()
            koreRandTimes.add(measureTimeMillis { collection.insertBatch(randomNotes.associateBy { it.id }) })
        }

        val koreSeqAvg = koreSeqTimes.takeLast(3).average()
        val koreRandAvg = koreRandTimes.takeLast(3).average()

        println("  Room (B-Tree) Sequential Inserts (Avg): %.1f ms".format(roomSeqAvg))
        println("  Room (B-Tree) Random UUID Inserts (Avg): %.1f ms (B-Tree Page Split Overhead)".format(roomRandAvg))
        println("  Room Split Overhead Ratio (Random / Seq): %.2fx".format(roomRandAvg / roomSeqAvg))
        println("")
        println("  KoreDB (LSM) Sequential Inserts (Avg): %.1f ms".format(koreSeqAvg))
        println("  KoreDB (LSM) Random UUID Inserts (Avg): %.1f ms".format(koreRandAvg))
        println("  KoreDB LSM Overhead Ratio (Random / Seq): %.2fx".format(koreRandAvg / koreSeqAvg))
    }
}

