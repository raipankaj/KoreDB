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

package com.pankaj.koredb.fts

import com.pankaj.koredb.core.VectorMath
import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

@Serializable
data class MedicalReport(
    val id: String,
    val patientName: String,
    val summary: String,
    val department: String
)

class BM25FtsTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_fts_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun testTokenizerStopwordsAndFrequencies() {
        val rawText = "The quick brown fox jumps over the lazy dog and a fast dog!"
        val result = KoreTokenizer.tokenize(rawText)

        // "the", "over", "and", "a" should be stripped
        assertFalse(result.termFrequencies.containsKey("the"))
        assertFalse(result.termFrequencies.containsKey("and"))
        assertFalse(result.termFrequencies.containsKey("a"))

        assertEquals(2, result.termFrequencies["dog"])
        assertEquals(1, result.termFrequencies["quick"])
        assertEquals(1, result.termFrequencies["brown"])
        assertEquals(1, result.termFrequencies["fox"])
        assertEquals(1, result.termFrequencies["jumps"])
        assertEquals(1, result.termFrequencies["fast"])
        assertEquals(8, result.totalTokenCount)
    }

    @Test
    fun testBM25ScoringLogic() {
        val scorer = BM25Scorer(k1 = 1.2f, b = 0.75f)

        // 1. IDF should be higher for rare terms
        val rareIdf = scorer.calculateIDF(totalDocs = 1000, docsWithTerm = 2)
        val commonIdf = scorer.calculateIDF(totalDocs = 1000, docsWithTerm = 500)
        assertTrue("Rare term should have significantly higher IDF", rareIdf > commonIdf)

        // 2. Term frequency saturation: score(tf=10) should not be 10x score(tf=1)
        val score1 = scorer.scoreTerm(idf = 2.0f, termFrequency = 1, docLength = 50, avgDocLength = 50f)
        val score10 = scorer.scoreTerm(idf = 2.0f, termFrequency = 10, docLength = 50, avgDocLength = 50f)
        assertTrue(score10 > score1)
        assertTrue("Term frequency saturation should prevent 10x explosion", score10 < score1 * 4)

        // 3. Length normalization: shorter focused document should score higher than long diluting document
        val shortDocScore = scorer.scoreTerm(idf = 2.0f, termFrequency = 2, docLength = 20, avgDocLength = 100f)
        val longDocScore = scorer.scoreTerm(idf = 2.0f, termFrequency = 2, docLength = 500, avgDocLength = 100f)
        assertTrue("Shorter focused document should score higher", shortDocScore > longDocScore)
    }

    @Test
    fun testFtsIndexSearchAndLiveUpdate() = runBlocking {
        val reports = db.collection<MedicalReport>("reports")
        reports.searchableFields({ it.patientName }, { it.summary }, { it.department })

        // 1. Insert documents
        reports.insert("rep_1", MedicalReport("rep_1", "Alice Smith", "Cardiology consultation for heart arrhythmia", "Cardiology"))
        reports.insert("rep_2", MedicalReport("rep_2", "Bob Jones", "Neurology consultation regarding migraines", "Neurology"))
        reports.insert("rep_3", MedicalReport("rep_3", "Charlie Brown", "Pediatric vaccination and heart check", "Pediatrics"))

        // 2. Query for "cardiology heart"
        val cardiologyMatches = reports.searchBM25("cardiology heart", limit = 5)
        assertEquals(2, cardiologyMatches.size)
        assertEquals("rep_1", cardiologyMatches[0].first.id) // rep_1 has both cardiology and heart
        assertEquals("rep_3", cardiologyMatches[1].first.id) // rep_3 has heart

        // 3. Query for "neurology"
        val neuroMatches = reports.searchBM25("neurology", limit = 5)
        assertEquals(1, neuroMatches.size)
        assertEquals("rep_2", neuroMatches[0].first.id)

        // 4. Update rep_2 to cardiology and re-query
        reports.insert("rep_2", MedicalReport("rep_2", "Bob Jones", "Patient transferred to cardiology for heart exam", "Cardiology"))
        
        val updatedNeuroMatches = reports.searchBM25("neurology", limit = 5)
        assertTrue("Old keyword should no longer match updated document", updatedNeuroMatches.isEmpty())

        val updatedCardioMatches = reports.searchBM25("cardiology", limit = 5)
        assertEquals(2, updatedCardioMatches.size)
        val cardioIds = updatedCardioMatches.map { it.first.id }.toSet()
        assertTrue(cardioIds.contains("rep_1"))
        assertTrue(cardioIds.contains("rep_2"))
    }

    @Test
    fun testReciprocalRankFusion() {
        val bm25Results = listOf(
            "doc_1" to 14.5f,
            "doc_2" to 8.2f,
            "doc_3" to 3.1f
        )
        val vectorResults = listOf(
            "doc_2" to 0.95f,
            "doc_1" to 0.88f,
            "doc_4" to 0.72f
        )

        val fused = ReciprocalRankFusion.fuse(
            bm25Results = bm25Results,
            vectorResults = vectorResults,
            limit = 5,
            k = 60
        )

        assertEquals(4, fused.size)
        // doc_1 and doc_2 are in top 2 for both, should dominate fused ranking
        val top2Ids = setOf(fused[0].first, fused[1].first)
        assertTrue(top2Ids.contains("doc_1"))
        assertTrue(top2Ids.contains("doc_2"))
        
        // RRF scores should be monotonically descending
        for (i in 0 until fused.size - 1) {
            assertTrue(fused[i].second >= fused[i + 1].second)
        }
    }

    @Test
    fun testEndToEndHybridSearch() = runBlocking {
        val reports = db.collection<MedicalReport>("reports")
        reports.searchableFields({ it.patientName }, { it.summary }, { it.department })

        val vectors = db.vectorCollection("report_vecs") {
            dimensions = 4
        }
        val bridge = db.graphVectorBridge(vectors)

        // Insert documents + vectors
        reports.insert("rep_1", MedicalReport("rep_1", "Alice Smith", "Cardiology arrhythmia", "Cardiology"))
        vectors.insert("rep_1", floatArrayOf(0.9f, 0.1f, 0.0f, 0.0f))

        reports.insert("rep_2", MedicalReport("rep_2", "Bob Jones", "Neurology headache", "Neurology"))
        vectors.insert("rep_2", floatArrayOf(0.0f, 0.9f, 0.1f, 0.0f))

        reports.insert("rep_3", MedicalReport("rep_3", "Charlie", "Cardiology checkup", "Cardiology"))
        vectors.insert("rep_3", floatArrayOf(0.8f, 0.2f, 0.0f, 0.0f))

        val queryVec = floatArrayOf(0.85f, 0.15f, 0.0f, 0.0f)
        val hybridResults = bridge.searchHybrid(
            collection = reports,
            queryText = "Cardiology arrhythmia",
            queryVector = queryVec,
            limit = 5
        )

        assertTrue(hybridResults.isNotEmpty())
        // rep_1 has both exact keyword "arrhythmia" AND highest vector similarity
        assertEquals("rep_1", hybridResults[0].first.id)
    }

    @Test
    fun testFtsDocumentDeleteAndReopenPersistence() = runBlocking {
        val reports = db.collection<MedicalReport>("reports")
        reports.searchableFields({ it.patientName }, { it.summary }, { it.department })

        reports.insert("rep_1", MedicalReport("rep_1", "Alice Smith", "Orthopedic fracture", "Orthopedics"))
        reports.insert("rep_2", MedicalReport("rep_2", "Bob Jones", "Dermatology rash", "Dermatology"))
        db.engine.flushMemTableInternal()

        // 1. Delete rep_1
        reports.delete("rep_1")
        val orthoMatches = reports.searchBM25("orthopedic", limit = 5)
        assertTrue("Deleted document should not match FTS search", orthoMatches.isEmpty())

        // 2. Reopen DB to simulate cold-start
        db.close()
        val reopenedDb = KoreDatabase(testDir)
        try {
            val reopenedReports = reopenedDb.collection<MedicalReport>("reports")
            reopenedReports.searchableFields({ it.patientName }, { it.summary }, { it.department })

            val dermaMatches = reopenedReports.searchBM25("dermatology", limit = 5)
            assertEquals(1, dermaMatches.size)
            assertEquals("rep_2", dermaMatches[0].first.id)

            // 3. Test deleteAll
            reopenedReports.deleteAll()
            val afterDeleteAllMatches = reopenedReports.searchBM25("dermatology", limit = 5)
            assertTrue("deleteAll should clear all FTS matches", afterDeleteAllMatches.isEmpty())
        } finally {
            reopenedDb.close()
        }
    }
}
