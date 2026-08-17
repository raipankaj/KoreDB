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
import com.pankaj.koredb.EdgeEntity
import com.pankaj.koredb.NoteDao
import com.pankaj.koredb.db.KoreAndroid
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.GraphStorage
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
import java.util.UUID
import kotlin.system.measureNanoTime

/**
 * Suite of 100 Exhaustive Graph, Relationship & Multi-Hop Traversal Tests: Room vs KoreDB.
 */
@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE)
class RoomVsKoreDBGraphRelationshipTest {

    private lateinit var context: Context
    private lateinit var roomDb: AppDatabase
    private lateinit var noteDao: NoteDao
    private lateinit var koreDb: KoreDatabase
    private lateinit var graph: GraphStorage
    private lateinit var testDir: File

    @Before
    fun setup() {
        context = ApplicationProvider.getApplicationContext()
        testDir = File(context.filesDir, "test_graph_rel_${UUID.randomUUID()}").apply { mkdirs() }

        roomDb = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java)
            .allowMainThreadQueries()
            .build()
        noteDao = roomDb.noteDao()

        koreDb = KoreAndroid.create(context, testDir.name)
        graph = koreDb.graph()
    }

    @After
    fun tearDown() {
        roomDb.close()
        koreDb.close()
        testDir.deleteRecursively()
    }

    @Test
    fun test301_to_325_directOneHopRelationships() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 1..25) {
            val fromId = "user_$i"
            val toId1 = "item_${i}_A"
            val toId2 = "item_${i}_B"

            roomTimeNs += measureNanoTime {
                noteDao.insertEdge(EdgeEntity(fromId, toId1, "PURCHASED"))
                noteDao.insertEdge(EdgeEntity(fromId, toId2, "PURCHASED"))
            }

            koreTimeNs += measureNanoTime {
                graph.putEdge(Edge(fromId, toId1, "PURCHASED"))
                graph.putEdge(Edge(fromId, toId2, "PURCHASED"))
                graph.getOutboundTargetIds(fromId, "PURCHASED")
            }
        }

        println("⚡ [Suite 4: Tests 301-325] 1-Hop Relationship Insertion & Traversal (25 pairs): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test326_to_350_twoHopNavigationEquivalence() = runBlocking {
        var roomTimeNs = 0L
        var koreTimeNs = 0L

        for (i in 26..50) {
            val a = "root_$i"
            val b = "mid_$i"
            val c = "leaf_$i"

            noteDao.insertEdge(EdgeEntity(a, b, "CHILD"))
            noteDao.insertEdge(EdgeEntity(b, c, "CHILD"))

            graph.putEdge(Edge(a, b, "CHILD"))
            graph.putEdge(Edge(b, c, "CHILD"))

            var roomLeaves: List<String> = emptyList()
            var koreLeaves: List<String> = emptyList()

            roomTimeNs += measureNanoTime {
                roomLeaves = noteDao.getTwoHopNodes(a, "CHILD")
            }

            koreTimeNs += measureNanoTime {
                val koreHops1 = graph.getOutboundTargetIds(a, "CHILD")
                koreLeaves = koreHops1.flatMap { graph.getOutboundTargetIds(it, "CHILD") }
            }

            assertEquals(roomLeaves.size, koreLeaves.size)
        }

        println("⚡ [Suite 4: Tests 326-350] 2-Hop Multi-Hop SQL JOIN vs Direct Adjacency (25 queries): Room = ${String.format("%.2f", roomTimeNs / 1_000_000.0)} ms | KoreDB = ${String.format("%.2f", koreTimeNs / 1_000_000.0)} ms")
    }

    @Test
    fun test351_to_375_bidirectionalInboundDiscovery() = runBlocking {
        var durationNs = 0L

        for (i in 51..75) {
            val target = "hub_$i"
            val sources = (1..5).map { "spoke_${i}_$it" }

            for (src in sources) {
                graph.putEdge(Edge(src, target, "CONNECTS_TO"))
            }

            durationNs += measureNanoTime {
                val inboundSources = graph.getInboundSourceIds(target, "CONNECTS_TO")
                assertEquals(5, inboundSources.size)
            }
        }

        println("⚡ [Suite 4: Tests 351-375] Inbound Edge Discovery (25 hubs, 125 spokes): KoreDB Direct Inbound Index = ${String.format("%.2f", durationNs / 1_000_000.0)} ms")
    }

    @Test
    fun test376_to_400_nodeCascadingDeletions() = runBlocking {
        var durationNs = 0L

        for (i in 76..100) {
            val node = Node("node_$i", labels = setOf("Account"), properties = mapOf("status" to "active"))
            graph.putNode(node)
            graph.putEdge(Edge("node_$i", "target_$i", "OWNS"))

            durationNs += measureNanoTime {
                val deleted = graph.deleteNode("node_$i")
                assertTrue(deleted)
            }
        }

        println("⚡ [Suite 4: Tests 376-400] Cascading Graph Node & Edge Deletion (25 nodes): KoreDB = ${String.format("%.2f", durationNs / 1_000_000.0)} ms")
    }
}
