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
import org.junit.Assert.assertNull
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
 * Suite of 100 Exhaustive CRUD & Data Integrity Comparison Tests: Room vs KoreDB.
 */
@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE)
class RoomVsKoreDBCrudComparisonTest {

    private lateinit var context: Context
    private lateinit var roomDb: AppDatabase
    private lateinit var noteDao: NoteDao
    private lateinit var koreDb: KoreDatabase
    private lateinit var collection: KoreCollection<Note>
    private lateinit var testDir: File

    @Before
    fun setup() {
        context = ApplicationProvider.getApplicationContext()
        testDir = File(context.filesDir, "test_crud_${UUID.randomUUID()}").apply { mkdirs() }

        roomDb = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries()
            .build()
        noteDao = roomDb.noteDao()

        koreDb = KoreAndroid.create(context, testDir.name)
        collection = koreDb.collection("notes")
    }

    @After
    fun tearDown() {
        roomDb.close()
        koreDb.close()
        testDir.deleteRecursively()
    }

    @Test
    fun test001_to_025_singleInsertsAndPointReads() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 1..25) {
            val note = Note(id = "doc_$i", title = "Title #$i", content = "Content for note $i", isPinned = (i % 2 == 0))
            
            roomTimeNs += measureNanoTime {
                noteDao.insert(note)
                noteDao.getById("doc_$i")
            }

            koreTimeNs += measureNanoTime {
                collection.insert(note.id, note)
                collection.getById("doc_$i")
            }

            val roomResult = noteDao.getById("doc_$i")
            val koreResult = collection.getById("doc_$i")

            assertNotNull("Room should find doc_$i", roomResult)
            assertNotNull("KoreDB should find doc_$i", koreResult)
            assertEquals(roomResult!!.id, koreResult!!.id)
        }

        println("⚡ [Suite 1: Tests 001-025] Single Insert + Read (25 pairs): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test026_to_050_inPlaceUpdatesAndOverwrites() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 26..50) {
            val initial = Note("update_$i", "Old Title $i", "Old Content $i", false)
            val updated = Note("update_$i", "New Title $i", "Updated Content $i", true)

            roomTimeNs += measureNanoTime {
                noteDao.insert(initial)
                noteDao.update(updated)
            }

            koreTimeNs += measureNanoTime {
                collection.insert(initial.id, initial)
                collection.insert(updated.id, updated)
            }

            val roomRes = noteDao.getById("update_$i")
            val koreRes = collection.getById("update_$i")

            assertEquals(roomRes!!.title, koreRes!!.title)
            assertEquals("New Title $i", koreRes.title)
        }

        println("⚡ [Suite 1: Tests 026-050] In-Place Updates (25 rounds): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test051_to_075_deletionsAndMissingKeyBehavior() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 51..75) {
            val note = Note("del_$i", "To Delete $i", "Will be deleted", false)
            
            roomTimeNs += measureNanoTime {
                noteDao.insert(note)
                noteDao.deleteById("del_$i")
            }

            koreTimeNs += measureNanoTime {
                collection.insert(note.id, note)
                collection.delete("del_$i")
            }

            assertNull(noteDao.getById("del_$i"))
            assertNull(collection.getById("del_$i"))
        }

        println("⚡ [Suite 1: Tests 051-075] Insert + Deletions (25 rounds): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test076_to_100_specialCharactersAndLargePayloads() = runBlocking {
        val testPayloads = listOf(
            "Hello, World! 🚀🔥✨",
            "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?`~",
            "CJK Characters: 这是一个中文测试，日本語のテスト，한국어 테스트",
            "Arabic/Hebrew RTL: مرحبا بالعالم / שלום עולם",
            "Escaped JSON strings: {\"key\": \"value\", \"nested\": [1, 2, 3]}",
            "SQL Injection attempts: Robert'); DROP TABLE notes;--",
            "Newlines and tabs:\n\n\tLine 1\n\tLine 2\r\nLine 3",
            "20KB Heavy Text: " + "A".repeat(20_000)
        )

        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 76..100) {
            val payload = testPayloads[i % testPayloads.size]
            val note = Note(id = "unicode_$i", title = "Special #$i: $payload".take(100), content = payload, isPinned = true)

            roomTimeNs += measureNanoTime {
                noteDao.insert(note)
                noteDao.getById("unicode_$i")
            }

            koreTimeNs += measureNanoTime {
                collection.insert(note.id, note)
                collection.getById("unicode_$i")
            }

            val roomRes = noteDao.getById("unicode_$i")
            val koreRes = collection.getById("unicode_$i")

            assertNotNull(roomRes)
            assertNotNull(koreRes)
            assertEquals(roomRes!!.content, koreRes!!.content)
        }

        println("⚡ [Suite 1: Tests 076-100] Special/Large Payloads (25 rounds): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }
}
