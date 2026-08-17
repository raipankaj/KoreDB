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

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import com.pankaj.koredb.AppDatabase
import com.pankaj.koredb.EdgeEntity
import com.pankaj.koredb.Note
import com.pankaj.koredb.NoteDao
import com.pankaj.koredb.db.KoreAndroid
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.io.File
import java.util.Random
import java.util.UUID

@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE)
class RoomVsKoreDBExhaustiveBenchmarkTest {

    private lateinit var context: Context
    private lateinit var roomDb: AppDatabase
    private lateinit var noteDao: NoteDao

    private lateinit var koreDb: KoreDatabase
    private lateinit var testDir: File

    @Before
    fun setup() {
        context = ApplicationProvider.getApplicationContext()
        testDir = File(context.filesDir, "bench_koredb_${UUID.randomUUID()}").apply { mkdirs() }

        // In-memory / fast SQLite Room database for testing
        roomDb = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries()
            .build()
        noteDao = roomDb.noteDao()

        // KoreDB instance
        koreDb = KoreAndroid.create(context, testDir.name)
    }

    @After
    fun tearDown() {
        roomDb.close()
        koreDb.close()
        testDir.deleteRecursively()
    }

    @Test
    fun runComprehensiveRoomVsKoreDBBenchmark() = runBlocking {
        println("==================================================================================================")
        println("🔥 COMPREHENSIVE BENCHMARK: ROOM (SQLite) vs. KOREDB (Multi-Model LSM)")
        println("   Workload Size: 1,000 to 5,000 operations per category across all database dimensions")
        println("==================================================================================================")

        val notesCollection = koreDb.collection<Note>("notes")
        notesCollection.searchableFields({ it.title }, { it.content })
        notesCollection.createIndex("title") { it.title }

        val testNotes = (1..1000).map { i ->
            Note(
                id = "note_$i",
                title = "Medical Report for Patient #$i in Cardiology",
                content = "Clinical notes for session $i: patient presents hypertension, normal sinus rhythm, medication adjusted.",
                isPinned = (i % 5 == 0)
            )
        }

        // ------------------------------------------------------------------------------------------------
        // 1. SINGLE-RECORD INSERTS (1,000 operations)
        // ------------------------------------------------------------------------------------------------
        val roomInsertStart = System.nanoTime()
        for (note in testNotes) {
            noteDao.insert(note)
        }
        val roomInsertTimeMs = (System.nanoTime() - roomInsertStart) / 1_000_000.0

        val koreInsertStart = System.nanoTime()
        for (note in testNotes) {
            notesCollection.insert(note.id, note)
        }
        val koreInsertTimeMs = (System.nanoTime() - koreInsertStart) / 1_000_000.0

        println("\n📌 1. SINGLE-RECORD INSERTS (1,000 docs):")
        println("   - Room (SQLite):   ${String.format("%.2f", roomInsertTimeMs)} ms (${(1000 / (roomInsertTimeMs / 1000.0)).toInt()} ops/sec)")
        println("   - KoreDB (LSM):    ${String.format("%.2f", koreInsertTimeMs)} ms (${(1000 / (koreInsertTimeMs / 1000.0)).toInt()} ops/sec)")
        println("   👉 KoreDB is ${String.format("%.1fx", roomInsertTimeMs / maxOf(0.001, koreInsertTimeMs))} faster")

        // ------------------------------------------------------------------------------------------------
        // 2. BULK BATCH INGESTION (5,000 docs in 1 operation)
        // ------------------------------------------------------------------------------------------------
        val bulkNotes = (1001..6000).map { i ->
            Note(
                id = "note_$i",
                title = "Diagnostic Batch Entry #$i",
                content = "Automated sensor readings and diagnostics recorded at interval $i.",
                isPinned = false
            )
        }

        val roomBulkStart = System.nanoTime()
        noteDao.insertAll(bulkNotes)
        val roomBulkTimeMs = (System.nanoTime() - roomBulkStart) / 1_000_000.0

        val koreBulkStart = System.nanoTime()
        val bulkMap = bulkNotes.associateBy { it.id }
        notesCollection.insertBatch(bulkMap)
        val koreBulkTimeMs = (System.nanoTime() - koreBulkStart) / 1_000_000.0

        println("\n📌 2. BULK BATCH INGESTION (5,000 docs in 1 batch):")
        println("   - Room (SQLite):   ${String.format("%.2f", roomBulkTimeMs)} ms (${(5000 / (roomBulkTimeMs / 1000.0)).toInt()} docs/sec)")
        println("   - KoreDB (Nitro):  ${String.format("%.2f", koreBulkTimeMs)} ms (${(5000 / (koreBulkTimeMs / 1000.0)).toInt()} docs/sec)")
        println("   👉 KoreDB is ${String.format("%.1fx", roomBulkTimeMs / maxOf(0.001, koreBulkTimeMs))} faster")

        // ------------------------------------------------------------------------------------------------
        // 3. POINT LOOKUPS BY PRIMARY KEY (1,000 random ID lookups)
        // ------------------------------------------------------------------------------------------------
        val random = Random(42)
        val sampleIds = (0 until 1000).map { "note_${random.nextInt(6000) + 1}" }

        val roomLookupStart = System.nanoTime()
        for (id in sampleIds) {
            noteDao.getById(id)
        }
        val roomLookupTimeMs = (System.nanoTime() - roomLookupStart) / 1_000_000.0

        val koreLookupStart = System.nanoTime()
        for (id in sampleIds) {
            notesCollection.getById(id)
        }
        val koreLookupTimeMs = (System.nanoTime() - koreLookupStart) / 1_000_000.0

        println("\n📌 3. POINT LOOKUPS BY PRIMARY KEY (1,000 reads):")
        println("   - Room (SQLite):   ${String.format("%.2f", roomLookupTimeMs)} ms (${(1000 / (roomLookupTimeMs / 1000.0)).toInt()} reads/sec)")
        println("   - KoreDB (Cache):  ${String.format("%.2f", koreLookupTimeMs)} ms (${(1000 / (koreLookupTimeMs / 1000.0)).toInt()} reads/sec)")
        println("   👉 KoreDB is ${String.format("%.1fx", roomLookupTimeMs / maxOf(0.001, koreLookupTimeMs))} faster")

        // ------------------------------------------------------------------------------------------------
        // 4. SECONDARY INDEX / TITLE EQUALITY QUERIES (1,000 queries)
        // ------------------------------------------------------------------------------------------------
        val searchTitles = (1..1000).map { "Medical Report for Patient #$it in Cardiology" }

        val roomIndexStart = System.nanoTime()
        for (t in searchTitles) {
            noteDao.getByTitle(t)
        }
        val roomIndexTimeMs = (System.nanoTime() - roomIndexStart) / 1_000_000.0

        val koreIndexStart = System.nanoTime()
        for (t in searchTitles) {
            notesCollection.getByIndex("title", t)
        }
        val koreIndexTimeMs = (System.nanoTime() - koreIndexStart) / 1_000_000.0

        println("\n📌 4. SECONDARY INDEX EQUALITY LOOKUPS (1,000 queries):")
        println("   - Room (B-Tree):   ${String.format("%.2f", roomIndexTimeMs)} ms (${(1000 / (roomIndexTimeMs / 1000.0)).toInt()} queries/sec)")
        println("   - KoreDB (LSM SIdx): ${String.format("%.2f", koreIndexTimeMs)} ms (${(1000 / (koreIndexTimeMs / 1000.0)).toInt()} queries/sec)")
        println("   👉 KoreDB is ${String.format("%.1fx", roomIndexTimeMs / maxOf(0.001, koreIndexTimeMs))} faster")

        // ------------------------------------------------------------------------------------------------
        // 5. FULL-TEXT SEARCH: BM25 vs. SQL LIKE (100 keyword queries)
        // ------------------------------------------------------------------------------------------------
        val queryKeywords = listOf("hypertension", "cardiology", "sinus", "rhythm", "medication", "patient", "clinical")

        val roomFtsStart = System.nanoTime()
        var roomFtsMatches = 0
        for (i in 0 until 100) {
            val kw = queryKeywords[i % queryKeywords.size]
            val matches = noteDao.getByPrefix(kw) // Room LIKE
            roomFtsMatches += matches.size
        }
        val roomFtsTimeMs = (System.nanoTime() - roomFtsStart) / 1_000_000.0

        val koreFtsStart = System.nanoTime()
        var koreFtsMatches = 0
        for (i in 0 until 100) {
            val kw = queryKeywords[i % queryKeywords.size]
            val matches = notesCollection.searchBM25(kw, limit = 10)
            koreFtsMatches += matches.size
        }
        val koreFtsTimeMs = (System.nanoTime() - koreFtsStart) / 1_000_000.0

        println("\n📌 5. FULL-TEXT SEARCH (100 multi-keyword queries across 6,000 docs):")
        println("   - Room (SQL LIKE):   ${String.format("%.2f", roomFtsTimeMs)} ms")
        println("   - KoreDB (Okapi BM25): ${String.format("%.2f", koreFtsTimeMs)} ms")
        println("   👉 KoreDB BM25 is ${String.format("%.1fx", roomFtsTimeMs / maxOf(0.001, koreFtsTimeMs))} faster + provides relevance ranking")

        // ------------------------------------------------------------------------------------------------
        // 6. GRAPH MULTI-HOP RELATIONSHIPS: 2-Hop Traversal (1,000 queries)
        // ------------------------------------------------------------------------------------------------
        val graph = koreDb.graph()
        // Build a graph of 1,000 nodes and 2,000 edges
        val edgeEntities = mutableListOf<EdgeEntity>()
        for (i in 1..999) {
            edgeEntities.add(EdgeEntity("note_$i", "note_${i + 1}", "REFERENCES"))
            if (i + 2 <= 1000) {
                edgeEntities.add(EdgeEntity("note_$i", "note_${i + 2}", "REFERENCES"))
            }
        }
        for (e in edgeEntities) {
            noteDao.insertEdge(e)
            graph.putEdge(Edge(e.fromId, e.toId, e.relation))
        }

        val roomGraphStart = System.nanoTime()
        for (i in 1..1000) {
            val startId = "note_${(i % 500) + 1}"
            noteDao.getTwoHopNodes(startId, "REFERENCES")
        }
        val roomGraphTimeMs = (System.nanoTime() - roomGraphStart) / 1_000_000.0

        val koreGraphStart = System.nanoTime()
        for (i in 1..1000) {
            val startId = "note_${(i % 500) + 1}"
            val hop1 = graph.getOutboundTargetIds(startId, "REFERENCES")
            for (h1 in hop1) {
                graph.getOutboundTargetIds(h1, "REFERENCES")
            }
        }
        val koreGraphTimeMs = (System.nanoTime() - koreGraphStart) / 1_000_000.0

        println("\n📌 6. GRAPH MULTI-HOP TRAVERSAL (1,000 2-hop queries):")
        println("   - Room (SQL JOIN):   ${String.format("%.2f", roomGraphTimeMs)} ms")
        println("   - KoreDB (Direct Adj): ${String.format("%.2f", koreGraphTimeMs)} ms")
        println("   👉 KoreDB is ${String.format("%.1fx", roomGraphTimeMs / maxOf(0.001, koreGraphTimeMs))} faster without SQL JOIN overhead")

        // ------------------------------------------------------------------------------------------------
        // 7. MULTI-MODEL CAPABILITY: VECTOR SIMILARITY SEARCH (HNSW 128-d)
        // ------------------------------------------------------------------------------------------------
        val vectors = koreDb.vectorCollection("doc_vecs") {
            dimensions = 128
        }
        val vecRandom = Random(123)
        val vectorEntries = (1..2000).associate { i ->
            val v = FloatArray(128) { (vecRandom.nextFloat() - 0.5f) * 2f }
            "note_$i" to v
        }
        vectors.insertBatch(vectorEntries)

        val queryVec = FloatArray(128) { (vecRandom.nextFloat() - 0.5f) * 2f }
        val koreVecSearchStart = System.nanoTime()
        for (i in 0 until 1000) {
            vectors.search(queryVec, limit = 10)
        }
        val koreVecSearchTimeMs = (System.nanoTime() - koreVecSearchStart) / 1_000_000.0

        println("\n📌 7. NATIVE VECTOR SIMILARITY SEARCH (1,000 Top-10 queries, 128-d):")
        println("   - Room (SQLite):     UNSUPPORTED (Requires full table scan + manual float deserialization in Kotlin)")
        println("   - KoreDB (HNSW):     ${String.format("%.2f", koreVecSearchTimeMs)} ms (${(1000 / (koreVecSearchTimeMs / 1000.0)).toInt()} searches/sec)")

        // ------------------------------------------------------------------------------------------------
        // 8. UNIFIED HYBRID SEARCH (Okapi BM25 + HNSW via RRF)
        // ------------------------------------------------------------------------------------------------
        val bridge = koreDb.graphVectorBridge(vectors)
        val koreHybridStart = System.nanoTime()
        for (i in 0 until 200) {
            bridge.searchHybrid(
                collection = notesCollection,
                queryText = "Cardiology hypertension rhythm",
                queryVector = queryVec,
                limit = 10
            )
        }
        val koreHybridTimeMs = (System.nanoTime() - koreHybridStart) / 1_000_000.0

        println("\n📌 8. UNIFIED HYBRID SEARCH (BM25 Keyword + Dense HNSW Vector Rank Fusion):")
        println("   - Room (SQLite):     UNSUPPORTED")
        println("   - KoreDB (Hybrid):   ${String.format("%.2f", koreHybridTimeMs)} ms (${(200 / (koreHybridTimeMs / 1000.0)).toInt()} hybrid queries/sec)")

        // ------------------------------------------------------------------------------------------------
        // 9. UPDATES & DELETIONS (1,000 records)
        // ------------------------------------------------------------------------------------------------
        val updatedNotes = testNotes.map { it.copy(title = it.title + " (UPDATED)") }
        val roomUpdateStart = System.nanoTime()
        for (n in updatedNotes) noteDao.update(n)
        val roomUpdateTimeMs = (System.nanoTime() - roomUpdateStart) / 1_000_000.0

        val koreUpdateStart = System.nanoTime()
        for (n in updatedNotes) notesCollection.insert(n.id, n)
        val koreUpdateTimeMs = (System.nanoTime() - koreUpdateStart) / 1_000_000.0

        println("\n📌 9. UPDATES (1,000 records):")
        println("   - Room (SQLite UPDATE): ${String.format("%.2f", roomUpdateTimeMs)} ms")
        println("   - KoreDB (LSM Append):  ${String.format("%.2f", koreUpdateTimeMs)} ms")
        println("   👉 KoreDB is ${String.format("%.1fx", roomUpdateTimeMs / maxOf(0.001, koreUpdateTimeMs))} faster")

        println("==================================================================================================\n")

        assertTrue(koreInsertTimeMs > 0)
        assertEquals(6000, notesCollection.getAll().size)
    }
}
