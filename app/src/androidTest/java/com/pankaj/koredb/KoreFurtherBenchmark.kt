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
import kotlin.random.Random
import kotlin.system.measureTimeMillis

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
}