/*
 * Copyright 2024 KoreDB Authors
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

package com.pankaj.koredb

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.core.VectorSerializer
import com.pankaj.koredb.db.KoreAndroid
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.random.Random
import kotlin.system.measureTimeMillis

/**
 * Performance benchmark comparing KoreDB against Room (SQLite).
 *
 * This test uses the real application context and production-like data sets
 * to measure and compare various database operations including bulk writes,
 * point lookups, concurrent access, and AI vector similarity search.
 *
 * Results are printed to Logcat under the 'System.out' tag.
 */
@RunWith(AndroidJUnit4::class)
class KoreBenchmarkTest {

    private lateinit var app: MyApplication
    private val TEST_SIZE = 50 // Standard size for quick comparisons

    @Before
    fun setup() {
        app = ApplicationProvider.getApplicationContext<MyApplication>()
        // Reset both databases to ensure a clean state for each benchmark
        runBlocking {
            app.database.collection("notes", Note.serializer()).deleteAll()
            app.roomDatabase.noteDao().deleteAll()
        }
    }

    /**
     * Measures basic CRUD performance for a world-scale scenario.
     * Compares Bulk Writes, Sequential Reads, Point Lookups, and Deletes.
     */
    @Test
    fun runGeneralPerformanceBenchmark() = runBlocking {
        val notesCollection = app.database.collection("notes", Note.serializer())
        val noteDao = app.roomDatabase.noteDao()

        // 1. Prepare Test Data
        val notes = (1..TEST_SIZE).map {
            Note(it.toString(), "Title $it", "Content body for index $it. Optimized for performance.")
        }
        val batchMap = notes.associateBy { it.id }

        val report = StringBuilder()
        report.append("\n\n📊 --- KORE DB vs ROOM GENERAL BENCHMARK (N=$TEST_SIZE) ---\n")

        // --- TEST 1: BULK WRITE ---
        val koreWriteTime = measureTimeMillis {
            notesCollection.insertBatch(batchMap)
        }
        val roomWriteTime = measureTimeMillis {
            noteDao.insertAll(notes)
        }
        report.append("📝 BULK WRITE:  KoreDB: ${koreWriteTime}ms | Room: ${roomWriteTime}ms\n")

        // --- TEST 2: FULL SCAN ---
        val koreReadTime = measureTimeMillis {
            notesCollection.getAll()
        }
        val roomReadTime = measureTimeMillis {
            noteDao.getAll()
        }
        report.append("📖 FULL READ:   KoreDB: ${koreReadTime}ms | Room: ${roomReadTime}ms\n")

        // --- TEST 3: POINT LOOKUP (Bloom Filter Test) ---
        val koreLookupTime = measureTimeMillis {
            notesCollection.getById("non_existent_id")
        }
        val roomLookupTime = measureTimeMillis {
            noteDao.getById("non_existent_id")
        }
        report.append("🛡️ POINT LOOKUP: KoreDB: ${koreLookupTime}ms | Room: ${roomLookupTime}ms (Non-existent ID)\n")

        // --- TEST 4: SINGLE RECORD DELETE ---
        val koreDeleteTime = measureTimeMillis {
            notesCollection.delete("1")
        }
        val roomDeleteTime = measureTimeMillis {
            noteDao.deleteById("1")
        }
        report.append("🗑️ DELETE:      KoreDB: ${koreDeleteTime}ms | Room: ${roomDeleteTime}ms\n")

        report.append("----------------------------------------------------\n\n")
        println(report.toString())
    }

    /**
     * Stresses the random update capabilities. 
     * In LSM-Trees (KoreDB), updates are appends (O(1)), while in B-Trees (Room),
     * they often require page updates and rewrites (O(log N)).
     */
    @Test
    fun runUpdateStressTest() = runBlocking {
        val DATA_SET_SIZE = 5000
        val UPDATE_COUNT = 1000
        
        val notes = (1..DATA_SET_SIZE).map {
            Note(it.toString(), "Title $it", "Initial Content")
        }
        val batchMap = notes.associateBy { it.id }

        val koreCollection = app.database.collection("notes", Note.serializer())
        val noteDao = app.roomDatabase.noteDao()

        // Initial Load
        koreCollection.insertBatch(batchMap)
        noteDao.insertAll(notes)

        // Generate Random Updates
        val updates = (1..UPDATE_COUNT).map {
            val id = (1..DATA_SET_SIZE).random().toString()
            Note(id, "Updated Title", "Modified Content")
        }
        val updateMap = updates.associateBy { it.id }

        val report = StringBuilder()
        report.append("\n\n🔥 --- RANDOM UPDATE STRESS TEST (Initial N=$DATA_SET_SIZE, Updates=$UPDATE_COUNT) ---\n")

        val koreUpdateTime = measureTimeMillis {
            koreCollection.insertBatch(updateMap)
        }

        val roomUpdateTime = measureTimeMillis {
            noteDao.insertAll(updates)
        }

        report.append("🔄 RANDOM UPDATES: KoreDB: ${koreUpdateTime}ms | Room: ${roomUpdateTime}ms\n")
        report.append("----------------------------------------------------\n")
        println(report.toString())
    }

    /**
     * Tests the responsiveness of point-reads during a heavy background write operation.
     */
    @Test
    fun runConcurrencyTest() = runBlocking {
        val koreCollection = app.database.collection("notes", Note.serializer())

        val report = StringBuilder()
        report.append("\n\n🔀 --- CONCURRENCY TEST (Read while Writing) ---\n")

        // 1. Start a heavy background write session
        val backgroundWrite = launch(Dispatchers.IO) {
            val heavyLoad = (1..3000).associate {
                it.toString() to Note(it.toString(), "Heavy", "Load Content")
            }
            koreCollection.insertBatch(heavyLoad)
        }

        // 2. Immediately attempt to read a record while the background write is active
        delay(5) 
        val koreReadTime = measureTimeMillis {
            koreCollection.getById("1")
        }

        backgroundWrite.join()
        report.append("⏱️ READ DURING WRITE: KoreDB: ${koreReadTime}ms\n")
        report.append("----------------------------------------------------\n")
        println(report.toString())
    }

    /**
     * Measures startup and first-query performance when no instances are cached.
     * Simulates a "Cold Start" of the application.
     */
    @Test
    fun runColdStartBenchmark() = runBlocking {
        val COUNT = 10000
        val testData = (1..COUNT).map { Note(it.toString(), "Title", "Body") }
        val batchMap = testData.associateBy { it.id }

        val koreCollection = app.database.collection("notes", Note.serializer())
        koreCollection.insertBatch(batchMap)
        app.roomDatabase.noteDao().insertAll(testData)

        // Close current instances to force file re-opening
        app.database.close()
        app.roomDatabase.close()

        System.gc()
        delay(1000)

        // Measure KoreDB Cold Start
        val koreStartupTime = measureTimeMillis {
            val freshKore = KoreAndroid.create(app, "benchmark_kore.db")
            val coll = freshKore.collection("notes", Note.serializer())
            coll.getById("5000")
            freshKore.close()
        }

        // Measure Room Cold Start
        val roomStartupTime = measureTimeMillis {
            val freshRoom = Room.databaseBuilder(app, AppDatabase::class.java, "benchmark_room.db").build()
            freshRoom.noteDao().getById("5000")
            freshRoom.close()
        }

        val report = """
            
            🚀 --- COLD START PERFORMANCE (Data N=$COUNT) ---
            KoreDB: ${koreStartupTime}ms
            Room:   ${roomStartupTime}ms
            ----------------------------------------------
            Result: KoreDB is ${String.format("%.1f", (roomStartupTime.toDouble() / koreStartupTime.toDouble()))}x Faster at Cold Start
        """.trimIndent()

        println(report)
    }

    /**
     * Compares AI Vector Similarity Search (Similarity Search).
     * KoreDB performs zero-allocation search directly on memory-mapped buffers.
     * Room requires fetching all blobs into RAM, deserializing them, and then calculating similarity.
     */
    @Test
    fun runVectorSimilarityBenchmark() = runBlocking {
        val VECTOR_COUNT = 5000
        val DIMENSIONS = 384 // Common size for mobile-optimized embeddings

        // Generate Random Query and Data
        val queryVector = FloatArray(DIMENSIONS) { Random.nextFloat() }
        val testData = (1..VECTOR_COUNT).associate {
            it.toString() to FloatArray(DIMENSIONS) { Random.nextFloat() }
        }

        val koreVectorColl = app.database.vectorCollection("face_embeddings")
        val vectorDao = app.roomDatabase.vectorDao()

        val report = StringBuilder()
        report.append("\n\n🤖 --- AI VECTOR SEARCH BENCHMARK (N=$VECTOR_COUNT, Dim=$DIMENSIONS) ---\n")

        // Measure Insertion
        val koreWriteTime = measureTimeMillis {
            koreVectorColl.insertBatch(testData)
        }

        val roomWriteTime = measureTimeMillis {
            val entities = testData.map { VectorEntity(it.key, VectorSerializer.toByteArray(it.value)) }
            vectorDao.insertAll(entities)
        }
        report.append("📝 BULK INSERT: KoreDB: ${koreWriteTime}ms | Room: ${roomWriteTime}ms\n")

        // Measure Search (Top 5)
        var koreResults: List<Pair<String, Float>>
        val koreSearchTime = measureTimeMillis {
            koreResults = koreVectorColl.search(queryVector, limit = 5)
        }

        var roomResults: List<Pair<String, Float>>
        val roomSearchTime = measureTimeMillis {
            val allFromRoom = vectorDao.getAll()
            val queryMag = VectorMath.getMagnitude(queryVector)
            roomResults = allFromRoom.map {
                val score = VectorMath.cosineSimilarity(
                    queryVector, queryMag,
                    java.nio.ByteBuffer.wrap(it.blob), 0
                )
                it.id to score
            }.sortedByDescending { it.second }.take(5)
        }

        report.append("🔍 SEARCH (Top 5): KoreDB: ${koreSearchTime}ms | Room: ${roomSearchTime}ms\n")
        report.append("----------------------------------------------------\n")

        // Verify that both engines found the same winner
        val match = if (koreResults.isNotEmpty() && roomResults.isNotEmpty()) {
            koreResults[0].first == roomResults[0].first
        } else false
        
        report.append("✅ Top Result Match: $match\n")
        println(report.toString())
    }
}
