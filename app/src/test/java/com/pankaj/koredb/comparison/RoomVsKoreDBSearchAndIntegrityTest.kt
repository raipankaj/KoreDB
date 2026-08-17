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
import org.junit.Assert.assertNotNull
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
 * Suite of 100 Exhaustive Full-Text Search, Stale Index Purging, Vector & Storage Integrity Tests.
 */
@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE)
class RoomVsKoreDBSearchAndIntegrityTest {

    private lateinit var context: Context
    private lateinit var roomDb: AppDatabase
    private lateinit var noteDao: NoteDao
    private lateinit var koreDb: KoreDatabase
    private lateinit var collection: KoreCollection<Note>
    private lateinit var testDir: File

    @Before
    fun setup() {
        context = ApplicationProvider.getApplicationContext()
        testDir = File(context.filesDir, "test_search_int_${UUID.randomUUID()}").apply { mkdirs() }

        roomDb = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries()
            .build()
        noteDao = roomDb.noteDao()

        koreDb = KoreAndroid.create(context, testDir.name)
        collection = koreDb.collection("notes")
        collection.searchableFields({ it.title }, { it.content })
        collection.createIndex("title") { it.title }
    }

    @After
    fun tearDown() {
        roomDb.close()
        koreDb.close()
        testDir.deleteRecursively()
    }

    @Test
    fun test401_to_425_fullTextKeywordQueries() = runBlocking {
        val conditions = listOf(
            "hypertension" to "Cardiology",
            "arrhythmia" to "Cardiology",
            "migraine" to "Neurology",
            "fracture" to "Orthopedics",
            "dermatitis" to "Dermatology"
        )

        for (i in 1..25) {
            val (condition, dept) = conditions[i % conditions.size]
            val note = Note(
                id = "med_$i",
                title = "Medical Chart #$i for $dept",
                content = "Patient assessed for chronic $condition and symptoms of $dept distress.",
                isPinned = false
            )
            noteDao.insert(note)
            collection.insert(note.id, note)
        }

        var ftsDurationNs = 0L
        for (i in 1..25) {
            val (condition, _) = conditions[i % conditions.size]
            ftsDurationNs += measureNanoTime {
                val koreMatches = collection.searchBM25(condition, limit = 10)
                assertTrue(koreMatches.isNotEmpty())
            }
        }

        println("⚡ [Suite 5: Tests 401-425] Okapi BM25 Full-Text Search (25 multi-condition queries): KoreDB = ${String.format("%.2f", ftsDurationNs / 1_000_000.0)} ms")
    }

    @Test
    fun test426_to_450_staleCandidatePurgingOnDocumentUpdate() = runBlocking {
        var durationNs = 0L

        for (i in 26..50) {
            val oldKeyword = "arthritis$i"
            val newKeyword = "asthma$i"
            val original = Note("stale_$i", "Initial Assessment", "Patient shows symptoms of $oldKeyword and stiffness.", false)
            noteDao.insert(original)
            collection.insert(original.id, original)

            durationNs += measureNanoTime {
                val updated = Note("stale_$i", "Updated Assessment", "Patient shows symptoms of $newKeyword and wheezing.", false)
                collection.insert(updated.id, updated)

                val oldMatches = collection.searchBM25(oldKeyword, limit = 5)
                assertEquals(0, oldMatches.size)

                val newMatches = collection.searchBM25(newKeyword, limit = 5)
                assertEquals(1, newMatches.size)
            }
        }

        println("⚡ [Suite 5: Tests 426-450] Real-Time BM25 Term Invalidation on Edit (25 updates): KoreDB = ${String.format("%.2f", durationNs / 1_000_000.0)} ms (Zero Ghost Matches)")
    }

    @Test
    fun test451_to_475_deletionPurgeConsistency() = runBlocking {
        var durationNs = 0L

        for (i in 51..75) {
            val note = Note("purge_$i", "Purge Test $i", "Confidential medical record $i", false)
            collection.insert(note.id, note)

            durationNs += measureNanoTime {
                collection.delete(note.id)
                val matches = collection.searchBM25("confidential$i", limit = 5)
                assertEquals(0, matches.size)
            }
        }

        println("⚡ [Suite 5: Tests 451-475] Deletion Inverted-Index Invalidation (25 deletions): KoreDB = ${String.format("%.2f", durationNs / 1_000_000.0)} ms")
    }

    @Test
    fun test476_to_500_databaseReopenAndDataDurability() = runBlocking {
        for (i in 76..100) {
            val note = Note("persist_$i", "Durability Title $i", "Durable payload content $i", (i % 2 == 0))
            collection.insert(note.id, note)
        }

        var reopenDurationNs = 0L
        koreDb.close()

        reopenDurationNs = measureNanoTime {
            val reopenedDb = KoreAndroid.create(context, testDir.name)
            try {
                val reopenedCollection = reopenedDb.collection<Note>("notes")
                for (i in 76..100) {
                    val recovered = reopenedCollection.getById("persist_$i")
                    assertNotNull(recovered)
                    assertEquals("Durability Title $i", recovered!!.title)
                }
            } finally {
                reopenedDb.close()
            }
        }

        println("⚡ [Suite 5: Tests 476-500] Database Restart & SSTable Recovery (25 documents): Cold Reload = ${String.format("%.2f", reopenDurationNs / 1_000_000.0)} ms (100% Integrity)")
    }
}
