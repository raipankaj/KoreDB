package com.pankaj.koredb

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import com.pankaj.koredb.core.VectorSerializer
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.Edge
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.io.File
import java.util.UUID
import kotlin.random.Random

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [33])
class MemoryBenchmark {

    private lateinit var context: android.content.Context
    private lateinit var koreDb: KoreDatabase
    private lateinit var roomDb: AppDatabase
    private lateinit var koreDir: File
    private lateinit var roomDbFile: File

    private val KEY_VALUE_COUNT = 20_000
    private val VECTOR_COUNT = 3_000
    private val DIM = 128

    @Before
    fun setup() {
        context = ApplicationProvider.getApplicationContext()
        val randomUuid = UUID.randomUUID()
        koreDir = File("build/tmp/kore_memory_bench_$randomUuid")
        koreDir.mkdirs()

        roomDbFile = File("build/tmp/room_memory_bench_$randomUuid.db")
    }

    @After
    fun tearDown() {
        if (::koreDb.isInitialized) {
            koreDb.close()
        }
        if (::roomDb.isInitialized) {
            roomDb.close()
        }
        koreDir.deleteRecursively()
        roomDbFile.delete()
        val walFile = File(roomDbFile.absolutePath + "-wal")
        val shmFile = File(roomDbFile.absolutePath + "-shm")
        if (walFile.exists()) walFile.delete()
        if (shmFile.exists()) shmFile.delete()
    }

    private fun getUsedMemory(): Long {
        repeat(3) {
            System.gc()
            Thread.sleep(50)
        }
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
    }

    private fun formatMem(bytes: Long): String {
        return "%.2f MB".format(bytes.toDouble() / (1024.0 * 1024.0))
    }

    private fun formatDisk(bytes: Long): String {
        if (bytes < 1024) return "$bytes B"
        if (bytes < 1024 * 1024) return "%.2f KB".format(bytes.toDouble() / 1024.0)
        return "%.2f MB".format(bytes.toDouble() / (1024.0 * 1024.0))
    }

    private fun getKoreDiskSize(): Long {
        if (!koreDir.exists()) return 0L
        return koreDir.walkTopDown().filter { it.isFile }.sumOf { it.length() }
    }

    private fun getRoomDiskSize(): Long {
        val wal = File(roomDbFile.absolutePath + "-wal")
        val shm = File(roomDbFile.absolutePath + "-shm")
        var total = 0L
        if (roomDbFile.exists()) total += roomDbFile.length()
        if (wal.exists()) total += wal.length()
        if (shm.exists()) total += shm.length()
        return total
    }

    @Test
    fun benchmarkMemoryFootprint() = runBlocking {
        println("\n\n📊 ═══════════════════════════════════════════════════")
        println("   STORAGE & MEMORY CONSUMPTION BENCHMARK")
        println("═══════════════════════════════════════════════════════")

        // 1. Measure System Baseline
        val baseline = getUsedMemory()
        println("System JVM Baseline Memory: ${formatMem(baseline)}")

        // --- ROOM / SQLITE BENCHMARK ---
        println("\n--- Initializing on-disk Room/SQLite Database ---")
        roomDb = Room.databaseBuilder(context, AppDatabase::class.java, roomDbFile.absolutePath)
            .allowMainThreadQueries()
            .build()
        
        val roomInitMem = getUsedMemory() - baseline
        val roomInitDisk = getRoomDiskSize()

        // Insert KV records in Room
        val notes = (1..KEY_VALUE_COUNT).map { Note(it.toString(), "Title-$it", "Body-Content-For-Note-$it") }
        roomDb.noteDao().insertAll(notes)
        val roomAfterKVWriteMem = getUsedMemory() - baseline
        val roomAfterKVWriteDisk = getRoomDiskSize()

        // Scan in Room
        val roomReadData = roomDb.noteDao().getAll()
        val roomAfterKVReadMem = getUsedMemory() - baseline

        // Vector insert in Room (Flat store)
        val vectors = (1..VECTOR_COUNT).associate { "vec_$it" to FloatArray(DIM) { Random.nextFloat() } }
        roomDb.vectorDao().deleteAll()
        roomDb.vectorDao().insertAll(vectors.map { (id, vec) ->
            VectorEntity(id, VectorSerializer.toByteArray(vec))
        })
        val roomAfterVecWriteMem = getUsedMemory() - baseline
        val roomAfterVecWriteDisk = getRoomDiskSize()

        // Graph build in Room (Relational edge table)
        val NODE_COUNT = 500
        val EDGES_PER_NODE = 4
        for (i in 1..NODE_COUNT) {
            for (j in 1..EDGES_PER_NODE) {
                roomDb.noteDao().insertEdge(EdgeEntity("u$i", "u${(i + j) % NODE_COUNT + 1}", "FOLLOWS"))
            }
        }
        val roomAfterGraphMem = getUsedMemory() - baseline
        val roomAfterGraphDisk = getRoomDiskSize()
        
        // Close Room to flush everything to disk for true size
        roomDb.close()
        val roomFinalDisk = getRoomDiskSize()
        
        // Trigger GC to clear Room residue
        repeat(5) { System.gc() }
        Thread.sleep(200)

        // --- KOREDB BENCHMARK ---
        val baselineKore = getUsedMemory()
        println("\n--- Initializing on-disk KoreDB Database ---")
        koreDb = KoreDatabase(koreDir)
        val koreInitMem = getUsedMemory() - baselineKore
        val koreInitDisk = getKoreDiskSize()

        // Insert KV in KoreDB
        val collection = koreDb.collection("memory_notes", Note.serializer())
        collection.insertBatch(notes.associateBy { it.id })
        
        val koreAfterKVWriteMem = getUsedMemory() - baselineKore
        val koreAfterKVWriteDisk = getKoreDiskSize()

        // Scan in KoreDB
        val koreReadData = collection.getAll()
        val koreAfterKVReadMem = getUsedMemory() - baselineKore

        // Vector insert in KoreDB (HNSW Engine)
        val koreVec = koreDb.vectorCollection("memory_vectors") {
            dimensions = DIM
            metric = DistanceMetric.COSINE
        }
        koreVec.insertBatch(vectors)
        koreVec.waitForIndexing()
        
        val koreAfterVecWriteMem = getUsedMemory() - baselineKore
        val koreAfterVecWriteDisk = getKoreDiskSize()

        // Graph build in KoreDB
        val graph = koreDb.graph()
        graph.transaction {
            for (i in 1..NODE_COUNT) putNode(Node("u$i", labels = setOf("Person"), properties = mapOf("city" to "City_${i % 10}")))
            for (i in 1..NODE_COUNT) {
                for (j in 1..EDGES_PER_NODE) {
                    putEdge(Edge("u$i", "u${(i + j) % NODE_COUNT + 1}", "FOLLOWS"))
                }
            }
        }
        
        val koreAfterGraphMem = getUsedMemory() - baselineKore
        val koreAfterGraphDisk = getKoreDiskSize()

        // Close KoreDB
        koreDb.close()
        val koreFinalDisk = getKoreDiskSize()

        println("\n=======================================================")
        println(" 📈 MEMORY USAGE SUMMARY (Relative to GC Baseline)")
        println("=======================================================")
        println("Task                          | Room / SQLite | KoreDB (LSM)")
        println("------------------------------+---------------+--------------")
        println("Database Initialization       | %-13s | %s".format(formatMem(roomInitMem), formatMem(koreInitMem)))
        println("KV Write (%d items)          | %-13s | %s".format(KEY_VALUE_COUNT, formatMem(roomAfterKVWriteMem), formatMem(koreAfterKVWriteMem)))
        println("KV Full Scan                  | %-13s | %s".format(formatMem(roomAfterKVReadMem), formatMem(koreAfterKVReadMem)))
        println("Vector Indexing (%d vectors) | %-13s | %s".format(VECTOR_COUNT, formatMem(roomAfterVecWriteMem), formatMem(koreAfterVecWriteMem)))
        println("Graph Traversal Setup         | %-13s | %s".format(formatMem(roomAfterGraphMem), formatMem(koreAfterGraphMem)))
        println("=======================================================")

        println("\n=======================================================")
        println(" 💾 DISK STORAGE SIZE COMPARISON")
        println("=======================================================")
        println("Phase / Collection Type       | Room / SQLite | KoreDB (LSM)")
        println("------------------------------+---------------+--------------")
        println("Empty Database                | %-13s | %s".format(formatDisk(roomInitDisk), formatDisk(koreInitDisk)))
        println("KV Store only (%d items)      | %-13s | %s".format(KEY_VALUE_COUNT, formatDisk(roomAfterKVWriteDisk), formatDisk(koreAfterKVWriteDisk)))
        println("Vector Store (%d vectors)     | %-13s | %s".format(VECTOR_COUNT, formatDisk(roomAfterVecWriteDisk), formatDisk(koreAfterVecWriteDisk)))
        println("Graph Store added             | %-13s | %s".format(formatDisk(roomAfterGraphDisk), formatDisk(koreAfterGraphDisk)))
        println("Closed/Final Storage State    | %-13s | %s".format(formatDisk(roomFinalDisk), formatDisk(koreFinalDisk)))
        println("=======================================================")
    }
}
