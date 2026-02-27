package com.pankaj.koredb

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.core.VectorSerializer
import com.pankaj.koredb.db.KoreAndroid
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.io.RandomAccessFile
import kotlin.random.Random
import kotlin.system.measureTimeMillis
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.algo.GraphAlgorithms
import com.pankaj.koredb.graph.query.query
import com.pankaj.koredb.graph.GraphStorage

@RunWith(AndroidJUnit4::class)
class KoreFurtherBenchmark {
    private lateinit var app: MyApplication

    @Before
    fun setup() {
        app = ApplicationProvider.getApplicationContext<MyApplication>()
        // Reset both databases to ensure a clean state for each benchmark
        runBlocking {
            app.database.collection("notes", Note.serializer()).deleteAll()
            app.roomDatabase.noteDao().deleteAll()
        }
    }

    @Test
    fun benchmarkSingleWriteLatency() = runBlocking {

        val collection = app.database.collection("single_write", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.deleteAll()
        dao.deleteAll()

        println("\n⚡ --- SINGLE WRITE LATENCY (1000 ops) ---")

        val koreTime = measureTimeMillis {
            repeat(1000) { i ->
                collection.insertBatch(
                    mapOf(i.toString() to Note(i.toString(), "T", "B"))
                )
            }
        }

        val roomTime = measureTimeMillis {
            repeat(1000) { i ->
                dao.insert(Note(i.toString(), "T", "B"))
            }
        }

        println("KoreDB: ${koreTime}ms")
        println("Room  : ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkSingleReadLatency() = runBlocking {

        val SIZE = 50_000
        val collection = app.database.collection("single_read", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.deleteAll()
        dao.deleteAll()

        val notes = (1..SIZE).map {
            Note(it.toString(), "Title", "Body")
        }

        collection.insertBatch(notes.associateBy { it.id })
        dao.insertAll(notes)

        println("\n⚡ --- SINGLE READ LATENCY (10,000 ops) ---")

        val koreTime = measureTimeMillis {
            repeat(10_000) {
                collection.getById("25000")
            }
        }

        val roomTime = measureTimeMillis {
            repeat(10_000) {
                dao.getById("25000")
            }
        }

        println("KoreDB: ${koreTime}ms")
        println("Room  : ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkSequentialReadThroughput() = runBlocking {

        val SIZE = 100_000
        val collection = app.database.collection("sequential", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.deleteAll()
        dao.deleteAll()

        val notes = (1..SIZE).map {
            Note(it.toString(), "Title", "Body")
        }

        collection.insertBatch(notes.associateBy { it.id })
        dao.insertAll(notes)

        println("\n📚 --- SEQUENTIAL READ (Full Scan) ---")

        val koreTime = measureTimeMillis {
            collection.getAll()
        }

        val roomTime = measureTimeMillis {
            dao.getAll()
        }

        println("KoreDB: ${koreTime}ms")
        println("Room  : ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkMassiveBulkInsert() = runBlocking {
        val SIZES = listOf(1_000, 10_000, 50_000)

        val notesCollection = app.database.collection("notes", Note.serializer())
        val noteDao = app.roomDatabase.noteDao()

        println("\n\n🚀 --- MASSIVE BULK INSERT BENCHMARK ---")

        for (size in SIZES) {

            notesCollection.deleteAll()
            noteDao.deleteAll()

            val data = (1..size).map {
                Note(it.toString(), "Title $it", "Body $it")
            }

            val batchMap = data.associateBy { it.id }

            val koreTime = measureTimeMillis {
                notesCollection.insertBatch(batchMap)
            }

            val roomTime = measureTimeMillis {
                noteDao.insertAll(data)
            }

            println("N=$size → KoreDB: ${koreTime}ms | Room: ${roomTime}ms")
        }

        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkNegativeLookup() = runBlocking {
        val SIZE = 50_000

        val notes = (1..SIZE).map {
            Note(it.toString(), "Title $it", "Body")
        }

        val batchMap = notes.associateBy { it.id }

        val notesCollection = app.database.collection("notes", Note.serializer())
        val noteDao = app.roomDatabase.noteDao()

        notesCollection.deleteAll()
        noteDao.deleteAll()

        notesCollection.insertBatch(batchMap)
        noteDao.insertAll(notes)

        val nonExistingId = "does_not_exist"

        val koreTime = measureTimeMillis {
            repeat(1_000) {
                notesCollection.getById(nonExistingId)
            }
        }

        val roomTime = measureTimeMillis {
            repeat(1_000) {
                noteDao.getById(nonExistingId)
            }
        }

        println("\n🛡️ NEGATIVE LOOKUP (1000x)")
        println("KoreDB: ${koreTime}ms | Room: ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkRandomUpdates() = runBlocking {
        val DATASET = 50_000
        val UPDATES = 10_000

        val initial = (1..DATASET).map {
            Note(it.toString(), "Initial", "Content")
        }

        val updates = (1..UPDATES).map {
            val id = Random.nextInt(1, DATASET).toString()
            Note(id, "Updated", "New Content")
        }

        val collection = app.database.collection("notes", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.deleteAll()
        dao.deleteAll()

        collection.insertBatch(initial.associateBy { it.id })
        dao.insertAll(initial)

        val koreTime = measureTimeMillis {
            collection.insertBatch(updates.associateBy { it.id })
        }

        val roomTime = measureTimeMillis {
            dao.insertAll(updates)
        }

        println("\n🔥 RANDOM UPDATE STRESS")
        println("KoreDB: ${koreTime}ms | Room: ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkPrefixScan() = runBlocking {
        val SIZE = 100_000

        val notes = (1..SIZE).map {
            val prefix = if (it % 2 == 0) "groupA" else "groupB"
            Note("$prefix-$it", "Title", "Body")
        }

        val collection = app.database.collection("notes", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.deleteAll()
        dao.deleteAll()

        collection.insertBatch(notes.associateBy { it.id })
        dao.insertAll(notes)

        val koreTime = measureTimeMillis {
            collection.getAll().filter { it.id.startsWith("groupA") }
        }

        val roomTime = measureTimeMillis {
            dao.getAll().filter { it.id.startsWith("groupA") }
        }

        println("\n📖 PREFIX SCAN")
        println("KoreDB: ${koreTime}ms | Room: ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkPrefixScanFairComparison() = runBlocking {
        val SIZE = 100_000

        val notes = (1..SIZE).map {
            val prefix = if (it % 2 == 0) "groupA" else "groupB"
            Note("$prefix-$it", "Title", "Body")
        }

        val collection = app.database.collection("notes", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.deleteAll()
        dao.deleteAll()

        println("\n📖 --- FAIR PREFIX SCAN BENCHMARK ---")

        // Insert
        collection.insertBatch(notes.associateBy { it.id })
        dao.insertAll(notes)

        // Warm up (important to avoid first-call disk bias)
        collection.getByIdPrefix("groupA")
        dao.getByPrefix("groupA")

        val koreTime = measureTimeMillis {
            repeat(50) {
                collection.getByIdPrefix("groupA")
            }
        }

        val roomTime = measureTimeMillis {
            repeat(50) {
                dao.getByPrefix("groupA")
            }
        }

        println("PREFIX='groupA' (50x)")
        println("KoreDB: ${koreTime}ms")
        println("Room  : ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun stressConcurrentWrites() = runBlocking {
        val collection = app.database.collection("concurrent", Note.serializer())
        collection.deleteAll()

        coroutineScope {
            repeat(8) {
                launch {
                    repeat(5000) { i ->
                        collection.insertBatch(
                            mapOf("$it-$i" to Note("$it-$i", "T", "B"))
                        )
                    }
                }
            }
        }

        val sample = collection.getById("1-100")
        assert(sample != null)
    }

    @Test
    fun testManifestConsistency() = runBlocking {

        val dbName = "manifest_test.db"

        val db = KoreAndroid.create(app, dbName)
        val collection = db.collection("manifest", Note.serializer())

        collection.insertBatch(
            (1..50_000).associate {
                it.toString() to Note(it.toString(), "T", "B")
            }
        )

        db.close()

        val reopened = KoreAndroid.create(app, dbName)
        val item = reopened.collection("manifest", Note.serializer()).getById("100")

        assert(item != null)

        reopened.close()
    }

    @Test
    fun benchmarkParallelReads() = runBlocking {
        val SIZE = 50_000

        val notes = (1..SIZE).map {
            Note(it.toString(), "Title", "Body")
        }

        val collection = app.database.collection("notes", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.deleteAll()
        dao.deleteAll()

        collection.insertBatch(notes.associateBy { it.id })
        dao.insertAll(notes)

        val ids = (1..10_000).map { Random.nextInt(1, SIZE).toString() }

        val koreTime = measureTimeMillis {
            coroutineScope {
                repeat(8) {
                    launch {
                        ids.forEach { collection.getById(it) }
                    }
                }
            }
        }

        val roomTime = measureTimeMillis {
            coroutineScope {
                repeat(8) {
                    launch {
                        ids.forEach { dao.getById(it) }
                    }
                }
            }
        }

        println("\n🧵 PARALLEL READS (8 coroutines)")
        println("KoreDB: ${koreTime}ms | Room: ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkLargeVectorSearch() = runBlocking {
        val VECTOR_COUNT = 25_000
        val DIM = 384

        val query = FloatArray(DIM) { Random.nextFloat() }
        val data = (1..VECTOR_COUNT).associate {
            it.toString() to FloatArray(DIM) { Random.nextFloat() }
        }

        val kore = app.database.vectorCollection("embeddings")
        val dao = app.roomDatabase.vectorDao()

        val koreInsert = measureTimeMillis {
            kore.insertBatch(data)
        }

        val roomInsert = measureTimeMillis {
            val entities = data.map {
                VectorEntity(it.key, VectorSerializer.toByteArray(it.value))
            }
            dao.insertAll(entities)
        }

        val koreSearch = measureTimeMillis {
            repeat(50) {
                kore.search(query, limit = 5)
            }
        }

        val roomSearch = measureTimeMillis {
            repeat(50) {
                val all = dao.getAll()
                val qMag = VectorMath.getMagnitude(query)
                all.map {
                    val score = VectorMath.cosineSimilarity(
                        query,
                        qMag,
                        java.nio.ByteBuffer.wrap(it.blob),
                        0
                    )
                    it.id to score
                }.sortedByDescending { it.second }.take(5)
            }
        }

        println("\n🤖 VECTOR SEARCH (N=$VECTOR_COUNT, 50 queries)")
        println("INSERT → KoreDB: ${koreInsert}ms | Room: ${roomInsert}ms")
        println("SEARCH → KoreDB: ${koreSearch}ms | Room: ${roomSearch}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun benchmarkColdStartLargeDataset() = runBlocking {
        val SIZE = 100_000

        val data = (1..SIZE).map {
            Note(it.toString(), "Title", "Body")
        }

        val collection = app.database.collection("notes", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        collection.insertBatch(data.associateBy { it.id })
        dao.insertAll(data)

        app.database.close()
        app.roomDatabase.close()

        System.gc()
        delay(1000)

        val koreTime = measureTimeMillis {
            val fresh = KoreAndroid.create(app, "cold_kore.db")
            fresh.collection("notes", Note.serializer()).getById("50000")
            fresh.close()
        }

        val roomTime = measureTimeMillis {
            val fresh = Room.databaseBuilder(app, AppDatabase::class.java, "cold_room.db").build()
            fresh.noteDao().getById("50000")
            fresh.close()
        }

        println("\n🚀 COLD START (N=$SIZE)")
        println("KoreDB: ${koreTime}ms | Room: ${roomTime}ms")
        println("----------------------------------------------------\n")
    }

    @Test
    fun testBatchAtomicity() = runBlocking {
        val db = app.database
        val collection = db.collection("atomic", Note.serializer())

        collection.deleteAll()

        val batch = mapOf(
            "1" to Note("1", "A", "A"),
            "2" to Note("2", "B", "B"),
            "3" to Note("3", "C", "C")
        )

        collection.insertBatch(batch)

        assert(collection.getById("1") != null)
        assert(collection.getById("2") != null)
        assert(collection.getById("3") != null)
    }

    @Test
    fun testWalRecoveryAfterClose() = runBlocking {
        val dbName = "recovery_test.db"
        val db = KoreAndroid.create(app, dbName)
        val collection = db.collection("recovery", Note.serializer())

        collection.insertBatch(
            (1..1000).associate {
                it.toString() to Note(it.toString(), "Title", "Body")
            }
        )

        db.close() // Simulate app kill

        val reopened = KoreAndroid.create(app, dbName)
        val reopenedCollection = reopened.collection("recovery", Note.serializer())

        val item = reopenedCollection.getById("500")
        assert(item != null)

        reopened.close()
    }

    @Test
    fun testWalCorruptionDetection() = runBlocking {
        val dbName = "crc_test.db"
        val db = KoreAndroid.create(app, dbName)
        val collection = db.collection("crc", Note.serializer())

        collection.insertBatch(
            (1..100).associate {
                it.toString() to Note(it.toString(), "T", "B")
            }
        )

        db.close()

        val walFile = app.getDatabasePath(dbName).parentFile!!
            .resolve("kore.wal")

        RandomAccessFile(walFile, "rw").use {
            it.seek(it.length() - 5)
            it.write(byteArrayOf(0, 0, 0, 0, 0)) // Corrupt tail
        }

        val reopened = KoreAndroid.create(app, dbName)
        val reopenedCollection = reopened.collection("crc", Note.serializer())

        val item = reopenedCollection.getById("50")
        assert(item != null) // Data before corruption survives

        reopened.close()
    }

    @Test
    fun benchmarkGraphEnginePerformance() = runBlocking {
        val graph = app.database.graph()
        val roomDao = app.roomDatabase.noteDao()

        app.database.deleteAllRaw()
        roomDao.clearEdges()

        val report = StringBuilder()
        report.append("\n\n🕸️ --- KORE DB vs ROOM: IDIOMATIC GRAPH BENCHMARK ---\n")

        val NODE_COUNT = 1000
        val EDGES_PER_NODE = 5

        // 1. 🏗️ High-Volume Bulk Build
        val buildTimeKore = measureTimeMillis {
            graph.transaction {
                for (i in 1..NODE_COUNT) {
                    putNode(Node("u$i", labels = setOf("Person"), properties = mapOf("age" to (i % 80).toString(), "city" to "City_${i % 10}")))
                }
                for (i in 1..NODE_COUNT) {
                    for (j in 1..EDGES_PER_NODE) {
                        val target = (i + j) % NODE_COUNT + 1
                        putEdge(Edge("u$i", "u$target", "FOLLOWS", properties = mapOf("weight" to Random.nextDouble(1.0, 10.0).toString())))
                    }
                }
            }
        }

        val buildTimeRoom = measureTimeMillis {
            for (i in 1..NODE_COUNT) {
                for (j in 1..EDGES_PER_NODE) {
                    val target = (i + j) % NODE_COUNT + 1
                    roomDao.insertEdge(EdgeEntity("u$i", "u$target", "FOLLOWS"))
                }
            }
        }
        report.append("📝 GRAPH BUILD (1K Nodes, 5K Edges) -> KoreDB: ${buildTimeKore}ms | Room: ${buildTimeRoom}ms\n")

        // 2. 🔍 Property Index Lookup
        var cityNodes: List<Node> = emptyList()
        val indexTime = measureTimeMillis {
            cityNodes = graph.getNodesByProperty("Person", "city", "City_5")
        }
        report.append("🔍 PROPERTY INDEX (People in City_5) -> KoreDB: ${indexTime}ms (Found ${cityNodes.size})\n")

        // 3. 🚀 DSL Query Performance (The New Optimized Path)

        // A. Full Object Retrieval (Includes JSON Deserialization)
        var twoHopNodesKore: List<Node> = emptyList()
        val nodeDslTime = measureTimeMillis {
            twoHopNodesKore = graph.query()
                .startingWith("Person", "city", "City_1")
                .outbound("FOLLOWS", hops = 2)
                .toNodeList() // Only parses JSON for the final result
        }

        // B. Pure Structural Retrieval (Bypasses JSON entirely)
        var twoHopIdsKore: List<String> = emptyList()
        val structuralDslTime = measureTimeMillis {
            twoHopIdsKore = graph.query()
                .startingWith("Person", "city", "City_1")
                .outbound("FOLLOWS", hops = 2)
                .toIdList() // ULTRA FAST
        }

        // Room's single SQL JOIN (starts from only 1 node for comparison)
        var twoHopResultsRoom: List<String> = emptyList()
        val sqlJoinTime = measureTimeMillis {
            twoHopResultsRoom = roomDao.getTwoHopNodes("u10", "FOLLOWS")
        }

        report.append("🚀 2-HOP TRAVERSAL (toNodeList) -> KoreDB: ${nodeDslTime}ms\n")
        report.append("🚀 2-HOP TRAVERSAL (toIdList)   -> KoreDB: ${structuralDslTime}ms\n")
        report.append("🚀 2-HOP SQL JOIN (Room)        -> Room  : ${sqlJoinTime}ms\n")

        // 4. 🛤️ Graph Algorithm: Dijkstra Shortest Path
        val pathTime = measureTimeMillis {
            val path = GraphAlgorithms.shortestPathDijkstra(graph, "u1", "u500", "FOLLOWS")
            report.append("🛤️ DIJKSTRA (u1 -> u500): Found path of length ${path?.size ?: 0}\n")
        }
        report.append("⏱️ DIJKSTRA EXECUTION -> KoreDB: ${pathTime}ms\n")

        // 5. 🏆 Graph Algorithm: PageRank Centrality
        val seed = (1..500).map { "u$it" }
        val prTime = measureTimeMillis {
            val ranks = GraphAlgorithms.pageRank(graph, seed, "FOLLOWS", iterations = 5)
            val topNode = ranks.maxByOrNull { it.value }
            report.append("🏆 PAGERANK TOP NODE: ${topNode?.key} (Score: ${String.format("%.4f", topNode?.value)})\n")
        }
        report.append("⏱️ PAGERANK (500 nodes, 5 iter) -> KoreDB: ${prTime}ms\n")

        report.append("----------------------------------------------------\n\n")
        println(report.toString())
    }
}