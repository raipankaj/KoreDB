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

package com.pankaj.koredb.comparison

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import com.pankaj.koredb.AppDatabase
import com.pankaj.koredb.Note
import com.pankaj.koredb.NoteDao
import com.pankaj.koredb.core.KoreCollection
import com.pankaj.koredb.db.KoreAndroid
import com.pankaj.koredb.db.KoreDatabase
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
import java.util.UUID
import kotlin.system.measureNanoTime

/**
 * Suite of 100 Exhaustive Query, Range, Prefix, Index & Aggregation Comparison Tests: Room vs KoreDB.
 */
@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE)
class RoomVsKoreDBQueryComparisonTest {

    private lateinit var context: Context
    private lateinit var roomDb: AppDatabase
    private lateinit var noteDao: NoteDao
    private lateinit var koreDb: KoreDatabase
    private lateinit var collection: KoreCollection<Note>
    private lateinit var testDir: File

    @Before
    fun setup() {
        runBlocking {
            context = ApplicationProvider.getApplicationContext()
            testDir = File(context.filesDir, "test_query_${UUID.randomUUID()}").apply { mkdirs() }

            roomDb = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
                .allowMainThreadQueries()
                .build()
            noteDao = roomDb.noteDao()

            koreDb = KoreAndroid.create(context, testDir.name)
            collection = koreDb.collection("notes")
            collection.createIndex("title") { it.title }
            collection.registerProperty("isPinned") { it.isPinned.toString() }
            collection.registerProperty("title") { it.title }

            // Pre-populate 500 documents
            val notes = (1..500).map { i ->
                val paddedId = "note_${String.format("%04d", i)}"
                val category = when (i % 5) {
                    0 -> "Cardiology"
                    1 -> "Neurology"
                    2 -> "Orthopedics"
                    3 -> "Dermatology"
                    else -> "Pediatrics"
                }
                Note(
                    id = paddedId,
                    title = "Category: $category",
                    content = "Content for session $i with diagnosis $category",
                    isPinned = (i % 3 == 0)
                )
            }
            noteDao.insertAll(notes)
            collection.insertBatch(notes.associateBy { it.id })
        }
    }

    @After
    fun tearDown() {
        roomDb.close()
        koreDb.close()
        testDir.deleteRecursively()
    }

    @Test
    fun test101_to_125_primaryKeyRangeScans() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 1..25) {
            val startIdx = i * 10
            val endIdx = startIdx + 20
            val startId = "note_${String.format("%04d", startIdx)}"
            val endId = "note_${String.format("%04d", endIdx)}"

            var roomRange: List<Note> = emptyList()
            var koreRange: List<Note> = emptyList()

            roomTimeNs += measureNanoTime {
                roomRange = noteDao.getByIdRange(startId, endId)
            }

            koreTimeNs += measureNanoTime {
                koreRange = collection.getByIdRange(startId, endId)
            }

            assertEquals("Range result size should match for query $i", roomRange.size, koreRange.size)
        }

        println("⚡ [Suite 2: Tests 101-125] Primary Key Range Scans (25 scans): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test126_to_150_idPrefixScans() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 1..25) {
            val prefix = "note_00$i"
            var roomPrefix: List<Note> = emptyList()
            var korePrefix: List<Note> = emptyList()

            roomTimeNs += measureNanoTime {
                roomPrefix = noteDao.getByPrefix(prefix)
            }

            koreTimeNs += measureNanoTime {
                korePrefix = collection.getByIdPrefix(prefix)
            }

            assertEquals(roomPrefix.size, korePrefix.size)
        }

        println("⚡ [Suite 2: Tests 126-150] ID Prefix Scans (25 scans): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test151_to_175_secondaryIndexLookups() = runBlocking {
        val categories = listOf("Cardiology", "Neurology", "Orthopedics", "Dermatology", "Pediatrics")
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 1..25) {
            val cat = categories[i % categories.size]
            val searchTitle = "Category: $cat"

            var roomMatches: List<Note> = emptyList()
            var koreMatches: List<Note> = emptyList()

            roomTimeNs += measureNanoTime {
                roomMatches = noteDao.getByTitle(searchTitle)
            }

            koreTimeNs += measureNanoTime {
                koreMatches = collection.getByIndex("title", searchTitle)
            }

            assertEquals(100, roomMatches.size)
            assertEquals(100, koreMatches.size)
        }

        println("⚡ [Suite 2: Tests 151-175] Secondary Index Equality (25 queries): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test176_to_200_booleanFilteringAndCountAggregations() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 1..25) {
            val pinnedExpected = (i % 2 == 0)

            roomTimeNs += measureNanoTime {
                noteDao.getByPinned(pinnedExpected)
                noteDao.count()
            }

            koreTimeNs += measureNanoTime {
                collection.query().where("isPinned") { it == pinnedExpected.toString() }.execute()
                collection.query().count()
            }
        }

        println("⚡ [Suite 2: Tests 176-200] Boolean Filters & Count Aggregations (25 queries): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }
}
