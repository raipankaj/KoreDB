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
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.random.Random

@Serializable
data class KnowledgeNote(
    val id: String,
    val title: String,
    val content: String,
    val category: String
)

/**
 * Baseline benchmark measuring the duration of achieving keyword + vector search
 * using EXISTING KoreDB mechanisms (full-collection string scan + vector distance scoring)
 * before dedicated BM25 + RRF indexing.
 */
class BaselineHybridSearchTest {

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
        testDir = File("build/tmp/test_baseline_hybrid_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun benchmarkExistingHybridSearchPipeline() = runBlocking {
        val notesCollection = db.collection<KnowledgeNote>("notes")
        val vectorCollection = db.vectorCollection("notes_vec") {
            dimensions = this@BaselineHybridSearchTest.dimensions
        }

        println("=================================================================")
        println("📊 BASELINE HYBRID SEARCH BENCHMARK (WITHOUT BM25 + RRF)")
        println("   Dataset: $docCount Documents | Dimension: $dimensions-d")
        println("=================================================================")

        // 1. INGESTION BENCHMARK
        val corpusWords = listOf(
            "cardiology", "neurology", "orthopedic", "pediatric", "dermatology",
            "appointment", "prescription", "consultation", "diagnosis", "therapy",
            "invoice", "receipt", "insurance", "statement", "payment",
            "algorithm", "database", "vector", "graph", "optimization"
        )

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
        db.engine.flushMemTableInternal()

        val insertDurationMs = (System.nanoTime() - insertStart) / 1_000_000.0
        println("⏱️  1. Ingestion & Vector Indexing ($docCount docs): ${String.format("%.2f", insertDurationMs)} ms")

        val queryVector = generateRandomVector()
        val targetKeyword = "cardiology"

        // 2. PURE KEYWORD SEARCH (Existing approach: Linear Collection Scan + String Contains)
        val kwStart = System.nanoTime()
        val allDocs = notesCollection.getAll()
        val matchingKeywordDocs = allDocs.filter {
            it.title.contains(targetKeyword, ignoreCase = true) ||
            it.content.contains(targetKeyword, ignoreCase = true)
        }
        val kwDurationMs = (System.nanoTime() - kwStart) / 1_000_000.0
        println("⏱️  2. Pure Keyword Scan (Linear String Contains across $docCount docs): ${String.format("%.3f", kwDurationMs)} ms (Found ${matchingKeywordDocs.size} matches)")

        // 3. PURE VECTOR SEARCH (Existing HNSW Vector Search)
        val vecStart = System.nanoTime()
        val vecResults = vectorCollection.search(queryVector, limit = 10)
        val vecDurationMs = (System.nanoTime() - vecStart) / 1_000_000.0
        println("⏱️  3. Pure HNSW Vector Search (Top-10): ${String.format("%.3f", vecDurationMs)} ms")

        // 4. MANUAL HYBRID SEARCH (Existing approach: Filter Docs by Keyword, then score vectors)
        val hybridStart = System.nanoTime()
        val candidateDocs = notesCollection.getAll().filter {
            it.title.contains(targetKeyword, ignoreCase = true) ||
            it.content.contains(targetKeyword, ignoreCase = true)
        }
        val candidateIds = candidateDocs.map { it.id }
        val candidateVectors = vectorCollection.getBatchVectors(candidateIds)
        val hybridScored = candidateDocs.mapNotNull { doc ->
            val vec = candidateVectors[doc.id] ?: return@mapNotNull null
            var dot = 0f
            for (j in 0 until dimensions) {
                dot += queryVector[j] * vec[j]
            }
            doc to dot
        }.sortedByDescending { it.second }.take(10)
        val hybridDurationMs = (System.nanoTime() - hybridStart) / 1_000_000.0
        println("⏱️  4. Manual Hybrid Search (Scan Docs + Filter + Batch Vectors + Score): ${String.format("%.3f", hybridDurationMs)} ms (Top: ${hybridScored.firstOrNull()?.first?.id})")

        // 5. DOCUMENT UPDATE & RE-QUERY
        val editStart = System.nanoTime()
        val updatedNote = KnowledgeNote(
            id = "note_100",
            title = "Medical Record #100 - neurology update",
            content = "Patient switched from cardiology to neurology specialist Dr. Robert.",
            category = "clinical"
        )
        notesCollection.insert("note_100", updatedNote)
        val retrieved = notesCollection.getById("note_100")
        val editDurationMs = (System.nanoTime() - editStart) / 1_000_000.0
        println("⏱️  5. Document Edit & Read-After-Write Verification: ${String.format("%.3f", editDurationMs)} ms")

        println("=================================================================\n")

        assertTrue(matchingKeywordDocs.isNotEmpty())
        assertNotNull(retrieved)
    }
}
