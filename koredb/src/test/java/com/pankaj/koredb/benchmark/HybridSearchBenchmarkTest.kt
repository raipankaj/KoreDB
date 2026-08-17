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

import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.random.Random

/**
 * Benchmark measuring the duration of BM25 + Semantic Hybrid Search (RRF)
 * on the exact same 2,000 document dataset as [BaselineHybridSearchTest].
 */
class HybridSearchBenchmarkTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase
    private val dimensions = 128
    private val docCount = 2000
    private val rng = Random(42)

    private fun generateRandomVector(): FloatArray {
        val vec = FloatArray(dimensions) { rng.nextFloat() * 2f - 1f }
        val mag = VectorMath.getMagnitude(vec)
        return if (mag > 0f) FloatArray(dimensions) { vec[it] / mag } else vec
    }

    @Before
    fun setup() {
        testDir = File("build/tmp/test_bm25_hybrid_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun benchmarkBM25AndRRFHybridSearchPipeline() = runBlocking {
        val notesCollection = db.collection<KnowledgeNote>("notes")
        notesCollection.searchableFields({ it.title }, { it.content }, { it.category })

        val vectorCollection = db.vectorCollection("notes_vec") {
            dimensions = this@HybridSearchBenchmarkTest.dimensions
        }
        val bridge = db.graphVectorBridge(vectorCollection)

        println("=================================================================")
        println("🚀 OPTIMIZED HYBRID SEARCH BENCHMARK (WITH BM25 + RRF)")
        println("   Dataset: $docCount Documents | Dimension: $dimensions-d")
        println("=================================================================")

        val corpusWords = listOf(
            "cardiology", "neurology", "orthopedic", "pediatric", "dermatology",
            "appointment", "prescription", "consultation", "diagnosis", "therapy",
            "invoice", "receipt", "insurance", "statement", "payment",
            "algorithm", "database", "vector", "graph", "optimization"
        )

        // 1. INGESTION BENCHMARK (with FTS Indexing active)
        val insertStart = System.nanoTime()
        val docBatch = mutableMapOf<String, KnowledgeNote>()
        val vectors = mutableListOf<Pair<String, FloatArray>>()

        for (i in 1..docCount) {
            val id = "note_$i"
            val kw1 = corpusWords[rng.nextInt(corpusWords.size)]
            val kw2 = corpusWords[rng.nextInt(corpusWords.size)]
            val doc = KnowledgeNote(
                id = id,
                title = "Medical Record #$i - $kw1",
                content = "Patient consultation notes regarding $kw1 treatment plan and follow-up $kw2 session with Dr. Robert.",
                category = if (i % 2 == 0) "clinical" else "billing"
            )
            docBatch[id] = doc
            vectors.add(id to generateRandomVector())
        }

        notesCollection.insertBatch(docBatch)
        for ((id, vec) in vectors) {
            vectorCollection.insert(id, vec)
        }
        vectorCollection.waitForIndexing()
        db.engine.flushMemTableInternal()

        val insertDurationMs = (System.nanoTime() - insertStart) / 1_000_000.0
        println("⏱️  1. Ingestion & Inverted FTS + Vector Indexing ($docCount docs): ${String.format("%.2f", insertDurationMs)} ms")

        val queryVector = generateRandomVector()
        val targetKeyword = "cardiology"

        // 2. PURE BM25 KEYWORD SEARCH (Inverted Index Lookup)
        val bm25Start = System.nanoTime()
        val bm25Results = notesCollection.searchBM25(targetKeyword, limit = 10)
        val bm25DurationMs = (System.nanoTime() - bm25Start) / 1_000_000.0
        println("⏱️  2. Pure BM25 Inverted Search (Top-10): ${String.format("%.3f", bm25DurationMs)} ms (Top score: ${String.format("%.3f", bm25Results.firstOrNull()?.second ?: 0f)})")

        // 3. PURE HNSW VECTOR SEARCH
        val vecStart = System.nanoTime()
        val vecResults = vectorCollection.search(queryVector, limit = 10)
        val vecDurationMs = (System.nanoTime() - vecStart) / 1_000_000.0
        println("⏱️  3. Pure HNSW Vector Search (Top-10): ${String.format("%.3f", vecDurationMs)} ms")

        // 4. OPTIMIZED HYBRID SEARCH (BM25 Inverted Search + HNSW Vector Search via RRF)
        val hybridStart = System.nanoTime()
        val hybridResults = bridge.searchHybrid(
            collection = notesCollection,
            queryText = targetKeyword,
            queryVector = queryVector,
            limit = 10,
            bm25Weight = 1.0f,
            vectorWeight = 1.0f
        )
        val hybridDurationMs = (System.nanoTime() - hybridStart) / 1_000_000.0
        println("⏱️  4. Optimized Hybrid Search (BM25 + HNSW via RRF): ${String.format("%.3f", hybridDurationMs)} ms (Top: ${hybridResults.firstOrNull()?.first?.id})")

        // 5. DOCUMENT UPDATE & RE-QUERY
        val editStart = System.nanoTime()
        val updatedNote = KnowledgeNote(
            id = "note_100",
            title = "Medical Record #100 - neurology update",
            content = "Patient switched from cardiology to neurology specialist Dr. Robert.",
            category = "clinical"
        )
        notesCollection.insert("note_100", updatedNote)
        val reQueryResults = notesCollection.searchBM25("neurology", limit = 5)
        val editDurationMs = (System.nanoTime() - editStart) / 1_000_000.0
        println("⏱️  5. Document Edit & Instant FTS Read-After-Write: ${String.format("%.3f", editDurationMs)} ms")

        println("=================================================================\n")

        assertTrue(bm25Results.isNotEmpty())
        assertTrue(hybridResults.isNotEmpty())
        assertNotNull(reQueryResults)
    }
}
