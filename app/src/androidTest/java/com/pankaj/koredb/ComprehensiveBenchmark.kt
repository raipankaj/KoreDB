package com.pankaj.koredb

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.core.VectorSerializer
import com.pankaj.koredb.db.KoreAndroid
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.graph.query.query
import kotlinx.coroutines.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.random.Random
import kotlin.system.measureTimeMillis
import java.nio.ByteBuffer
import java.nio.ByteOrder

@RunWith(AndroidJUnit4::class)
class ComprehensiveBenchmark {
    private lateinit var app: MyApplication

    @Before
    fun setup() {
        app = ApplicationProvider.getApplicationContext()
        runBlocking<Unit> {
            app.database.collection("notes", Note.serializer()).deleteAll()
            app.roomDatabase.noteDao().deleteAll()
            app.database.deleteAllRaw()
            app.roomDatabase.noteDao().clearEdges()
            app.roomDatabase.vectorDao().deleteAll()
        }
    }

    private fun generateLargeString(sizeKb: Int): String {
        val sb = StringBuilder(sizeKb * 1024)
        val charPool = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        repeat(sizeKb * 1024) { sb.append(charPool[it % charPool.length]) }
        return sb.toString()
    }

    private fun printResult(name: String, koreTime: Long, roomTime: Long) {
        val speedup = if (koreTime == 0L) 0f else roomTime.toFloat() / koreTime.toFloat()
        val winner = if (koreTime < roomTime) "🏆 KoreDB" else "🏆 Room"
        println(String.format("%-30s | KoreDB: %4d ms | Room: %4d ms | %s (%.2fx)", name, koreTime, roomTime, winner, speedup))
    }

    @Test
    fun benchmark01_PointOperations() = runBlocking {
        println("\n=======================================================")
        println(" 🎯 BENCHMARK 1: POINT OPERATIONS (Read/Write 1 by 1)")
        println("=======================================================")

        val collection = app.database.collection("point_ops", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        val OPS = 5_000

        // Single Writes
        val writeKore = measureTimeMillis {
            repeat(OPS) { i -> collection.insertBatch(mapOf(i.toString() to Note(i.toString(), "T", "B"))) }
        }
        val writeRoom = measureTimeMillis {
            repeat(OPS) { i -> dao.insert(Note(i.toString(), "T", "B")) }
        }
        printResult("Single Writes ($OPS)", writeKore, writeRoom)

        // Single Reads (Warm)
        val readKore = measureTimeMillis {
            repeat(OPS) { i -> collection.getById(i.toString()) }
        }
        val readRoom = measureTimeMillis {
            repeat(OPS) { i -> dao.getById(i.toString()) }
        }
        printResult("Single Reads ($OPS)", readKore, readRoom)

        // Negative Lookups
        val negKore = measureTimeMillis {
            repeat(OPS) { collection.getById("missing_$it") }
        }
        val negRoom = measureTimeMillis {
            repeat(OPS) { dao.getById("missing_$it") }
        }
        printResult("Negative Lookups ($OPS)", negKore, negRoom)
    }

    @Test
    fun benchmark02_BulkOperations() = runBlocking {
        println("\n=======================================================")
        println(" 🚀 BENCHMARK 2: MASSIVE BULK OPERATIONS")
        println("=======================================================")

        val collection = app.database.collection("bulk_ops", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        val SIZE = 50_000

        val initialData = (1..SIZE).map { Note(it.toString(), "Title", "Content") }
        val batchMap = initialData.associateBy { it.id }

        // Bulk Insert
        val insertKore = measureTimeMillis { collection.insertBatch(batchMap) }
        val insertRoom = measureTimeMillis { dao.insertAll(initialData) }
        printResult("Bulk Insert ($SIZE)", insertKore, insertRoom)

        // Sequential Read (Full Scan)
        val scanKore = measureTimeMillis { collection.getAll() }
        val scanRoom = measureTimeMillis { dao.getAll() }
        printResult("Full Sequential Scan", scanKore, scanRoom)

        // Random Updates
        val updates = (1..10_000).map { Note(Random.nextInt(1, SIZE).toString(), "Updated", "New") }
        val updateKore = measureTimeMillis { collection.insertBatch(updates.associateBy { it.id }) }
        val updateRoom = measureTimeMillis { dao.insertAll(updates) }
        printResult("Random Updates (10K)", updateKore, updateRoom)
    }

    @Test
    fun benchmark03_PrefixAndRange() = runBlocking {
        println("\n=======================================================")
        println(" 📖 BENCHMARK 3: PREFIX & RANGE QUERIES")
        println("=======================================================")

        val collection = app.database.collection("prefix_range", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        val SIZE = 50_000

        val data = (1..SIZE).map {
            val prefix = if (it % 2 == 0) "userA" else "userB"
            Note("$prefix:$it", "Title", "Body")
        }
        collection.insertBatch(data.associateBy { it.id })
        dao.insertAll(data)

        // Warm up
        collection.getByIdPrefix("userA:")
        dao.getByPrefix("userA:")

        // Prefix Scan
        val prefixKore = measureTimeMillis { repeat(50) { collection.getByIdPrefix("userA:") } }
        val prefixRoom = measureTimeMillis { repeat(50) { dao.getByPrefix("userA:") } }
        printResult("Prefix Scan 'userA' (50x)", prefixKore, prefixRoom)

        // Range Query (Small Data)
        val startId = "userA:${20000}"
        val endId = "userA:${21000}"
        collection.getByIdRange(startId, endId) // Warmup
        val rangeKore = measureTimeMillis { repeat(50) { collection.getByIdRange(startId, endId) } }
        val rangeRoom = measureTimeMillis { repeat(50) { dao.getByIdRange(startId, endId) } }
        printResult("Range Query (500 items, 50x)", rangeKore, rangeRoom)

        // Range Query (Large Data 50KB/record)
        val largeCollection = app.database.collection("large_range", Note.serializer())
        val largeContent = generateLargeString(50) // 50 KB
        val largeData = (1..2000).map { Note("lg:$it", "Title", largeContent) }
        largeCollection.insertBatch(largeData.associateBy { it.id })
        dao.insertAll(largeData)

        largeCollection.getByIdRange("lg:1000", "lg:1500") // Warmup
        val lgRangeKore = measureTimeMillis { repeat(5) { largeCollection.getByIdRange("lg:1000", "lg:1500") } }
        val lgRangeRoom = measureTimeMillis { repeat(5) { dao.getByIdRange("lg:1000", "lg:1500") } }
        printResult("Large Range (50KB/rec) (5x)", lgRangeKore, lgRangeRoom)
    }

    @Test
    fun benchmark04_Concurrency() = runBlocking {
        println("\n=======================================================")
        println(" 🧵 BENCHMARK 4: CONCURRENCY (Parallel Reads/Writes)")
        println("=======================================================")

        val collection = app.database.collection("concurrency", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        val SIZE = 50_000
        val initialData = (1..SIZE).map { Note(it.toString(), "Title", "Body") }
        collection.insertBatch(initialData.associateBy { it.id })
        dao.insertAll(initialData)

        val idsToRead = (1..5_000).map { Random.nextInt(1, SIZE).toString() }

        // Parallel Reads
        val readKore = measureTimeMillis {
            coroutineScope {
                repeat(8) { launch(Dispatchers.Default) { idsToRead.forEach { collection.getById(it) } } }
            }
        }
        val readRoom = measureTimeMillis {
            coroutineScope {
                repeat(8) { launch(Dispatchers.Default) { idsToRead.forEach { dao.getById(it) } } }
            }
        }
        printResult("Parallel Reads (8 threads)", readKore, readRoom)

        // Concurrent Writes
        val writeKore = measureTimeMillis {
            coroutineScope {
                repeat(8) { threadId ->
                    launch(Dispatchers.Default) {
                        val batch = (1..1000).associate { "$threadId-$it" to Note("$threadId-$it", "T", "B") }
                        collection.insertBatch(batch)
                    }
                }
            }
        }
        val writeRoom = measureTimeMillis {
            coroutineScope {
                repeat(8) { threadId ->
                    launch(Dispatchers.Default) {
                        val batch = (1..1000).map { Note("r-$threadId-$it", "T", "B") }
                        dao.insertAll(batch)
                    }
                }
            }
        }
        printResult("Concurrent Writes (8 threads)", writeKore, writeRoom)
    }

    @Test
    fun benchmark05_VectorSimilarity() = runBlocking {
        println("\n=======================================================")
        println(" 🤖 BENCHMARK 5: VECTOR SIMILARITY (HNSW vs Flat)")
        println("=======================================================")

        val VECTOR_COUNT = 15_000
        val DIM = 384
        val query = FloatArray(DIM) { Random.nextFloat() }
        
        val koreVec = app.database.vectorCollection("bench_vectors")
        val roomVecDao = app.roomDatabase.vectorDao()

        val data = (1..VECTOR_COUNT).associate { it.toString() to FloatArray(DIM) { Random.nextFloat() } }

        // Vector Insert
        val insertKore = measureTimeMillis { koreVec.insertBatch(data) }
        val insertRoom = measureTimeMillis { 
            roomVecDao.insertAll(data.map { VectorEntity(it.key, VectorSerializer.toByteArray(it.value)) }) 
        }
        printResult("Vector Insert ($VECTOR_COUNT)", insertKore, insertRoom)

        // Wait for HNSW background indexing
        koreVec.waitForIndexing()

        // Vector Search
        val qMag = VectorMath.getMagnitude(query)
        val searchKore = measureTimeMillis { repeat(50) { koreVec.search(query, 5) } }
        val searchRoom = measureTimeMillis {
            repeat(50) {
                roomVecDao.getAll().map {
                    val buf = ByteBuffer.wrap(it.blob).order(ByteOrder.LITTLE_ENDIAN)
                    it.id to VectorMath.cosineSimilarity(query, qMag, buf, 0, DIM)
                }.sortedByDescending { it.second }.take(5)
            }
        }
        printResult("Vector Search (50 queries)", searchKore, searchRoom)

        // Hydration / Cold Start (KoreDB only simulation)
        KoreAndroid.delete(app, "cold_hnsw.db")
        val coldDb = KoreAndroid.create(app, "cold_hnsw.db")
        val coldVec = coldDb.vectorCollection("bench_vectors")
        coldVec.insertBatch(data)
        coldVec.waitForIndexing() // Builds and saves HNSW to disk
        coldDb.close()

        val hydrateStart = System.currentTimeMillis()
        val restartedDb = KoreAndroid.create(app, "cold_hnsw.db")
        val restartedVec = restartedDb.vectorCollection("bench_vectors")
        restartedVec.waitForIndexing() // Loads from binary disk file
        val hydrateTime = System.currentTimeMillis() - hydrateStart
        restartedDb.close()

        println(String.format("%-30s | KoreDB: %4d ms | (Graph loaded instantly from disk)", "HNSW Binary Hydration", hydrateTime))
    }

    @Test
    fun benchmark06_GraphTraversal() = runBlocking {
        println("\n=======================================================")
        println(" 🕸️ BENCHMARK 6: GRAPH & RELATIONAL TRAVERSAL")
        println("=======================================================")

        val graph = app.database.graph()
        val roomDao = app.roomDatabase.noteDao()
        val NODE_COUNT = 2_000
        val EDGES_PER_NODE = 5

        // Build Graph
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
                for (j in 1..EDGES_PER_NODE) roomDao.insertEdge(EdgeEntity("u$i", "u${(i + j) % NODE_COUNT + 1}", "FOLLOWS"))
            }
        }
        printResult("Graph Build (2K Nodes, 10K Edges)", buildKore, buildRoom)

        // 2-Hop Traversal (DSL vs SQL JOIN)
        val dslKore = measureTimeMillis {
            repeat(100) {
                graph.query().startingWith("Person", "city", "City_1").outbound("FOLLOWS", hops = 2).toIdList()
            }
        }
        val sqlRoom = measureTimeMillis {
            repeat(100) {
                roomDao.getTwoHopNodes("u10", "FOLLOWS") // Approximation of the SQL equivalent
            }
        }
        printResult("2-Hop Traversal (100x)", dslKore, sqlRoom)

        // Graph Algorithms (KoreDB Native)
        val prTime = measureTimeMillis {
            GraphAlgorithms.pageRank(graph, (1..500).map { "u$it" }, "FOLLOWS", 5)
        }
        println(String.format("%-30s | KoreDB: %4d ms | (Room: N/A - No Graph Engine)", "PageRank (500 nodes, 5 iter)", prTime))

        val dijTime = measureTimeMillis {
            GraphAlgorithms.shortestPathDijkstra(graph, "u1", "u1000", "FOLLOWS")
        }
        println(String.format("%-30s | KoreDB: %4d ms | (Room: N/A - No Graph Engine)", "Dijkstra Shortest Path", dijTime))
    }
}