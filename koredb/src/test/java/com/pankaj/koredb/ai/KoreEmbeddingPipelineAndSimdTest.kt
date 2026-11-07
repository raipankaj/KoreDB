package com.pankaj.koredb.ai

import com.pankaj.koredb.core.SimdVectorMath
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.hnsw.DistanceMetric
import com.pankaj.koredb.hnsw.ScalarQuantizer
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs
import kotlin.math.sqrt
import kotlin.random.Random

class KoreEmbeddingPipelineAndSimdTest {

    @Test
    fun `cosineSimilarityFused16 matches multi-pass cosine across arbitrary dimensions`() {
        val rng = Random(42)
        val dimsToTest = listOf(1, 3, 7, 8, 9, 15, 16, 17, 64, 133, 384, 768)

        for (dim in dimsToTest) {
            val a = FloatArray(dim) { rng.nextFloat() * 2f - 1f }
            val b = FloatArray(dim) { rng.nextFloat() * 2f - 1f }

            val magA = SimdVectorMath.getMagnitude16(a)
            val magB = SimdVectorMath.getMagnitude16(b)
            val expected = SimdVectorMath.cosineSimilarity16(a, magA, b, magB)
            val fused = SimdVectorMath.cosineSimilarityFused16(a, b)
            val metricScore = DistanceMetric.COSINE.compute(a, b)

            assertEquals("Mismatch at dim=$dim", expected, fused, 1e-5f)
            assertEquals("Metric mismatch at dim=$dim", expected, metricScore, 1e-5f)
        }

        // Zero vector guard
        val zeros = FloatArray(64)
        val nonZero = FloatArray(64) { 1f }
        assertEquals(0f, SimdVectorMath.cosineSimilarityFused16(zeros, nonZero), 1e-6f)
    }

    @Test
    fun `normalizeL2InPlace16 and sliceAndNormalizeL216 produce unit-length vectors`() {
        val rng = Random(99)
        val raw = FloatArray(384) { (rng.nextFloat() - 0.5f) * 10f }

        val sliced64 = SimdVectorMath.sliceAndNormalizeL216(raw, targetDimensions = 64, normalize = true)
        assertEquals(64, sliced64.size)
        val mag64 = SimdVectorMath.getMagnitude16(sliced64)
        assertEquals(1.0f, mag64, 1e-5f)

        val copy = raw.clone()
        val origMag = SimdVectorMath.normalizeL2InPlace16(copy)
        assertTrue(origMag > 0f)
        assertEquals(1.0f, SimdVectorMath.getMagnitude16(copy), 1e-5f)
    }

    @Test
    fun `ScalarQuantizer 8-lane unrolled distance and prefix match reference across all metrics`() {
        val rng = Random(1234)
        for (dim in listOf(13, 64, 135, 384)) {
            val sq = ScalarQuantizer(dim)
            val training = List(64) { FloatArray(dim) { rng.nextFloat() * 4f - 2f } }
            sq.train(training)

            val query = FloatArray(dim) { rng.nextFloat() * 4f - 2f }
            val target = FloatArray(dim) { rng.nextFloat() * 4f - 2f }
            val quantized = sq.quantize(target)
            val dequantized = sq.dequantize(quantized)

            for (metric in DistanceMetric.entries) {
                val expected = metric.compute(query, dequantized)
                val actual = sq.computeDistance(query, quantized, metric)
                assertEquals("SQ8 metric=$metric dim=$dim", expected, actual, 1e-4f)

                val prefixDim = (dim / 2).coerceAtLeast(1)
                val expectedPrefix = metric.computePrefix(query, dequantized, prefixDim)
                val actualPrefix = sq.computeDistancePrefix(query, quantized, metric, prefixDim)
                assertEquals("SQ8 prefix metric=$metric dim=$dim", expectedPrefix, actualPrefix, 1e-4f)

                // Verify quantized-to-quantized zero-allocation distance
                val qQuery = sq.quantize(query)
                val deqQuery = sq.dequantize(qQuery)
                val expectedQ2Q = metric.compute(deqQuery, dequantized)
                val actualQ2Q = sq.computeDistanceQuantized(qQuery, quantized, metric)
                assertEquals("SQ8 Q2Q metric=$metric dim=$dim", expectedQ2Q, actualQ2Q, 1e-4f)
            }
        }
    }

    @Test
    fun `ScalarQuantizer untrained safety and re-training reset`() {
        val sq = ScalarQuantizer(16)
        val q = FloatArray(16) { 1f }
        val untrainedBytes = sq.quantize(q)
        val deq = sq.dequantize(untrainedBytes)
        // Must not overflow to Float.MAX_VALUE or NaN when untrained
        assertTrue(deq.all { it.isFinite() && it == 0f })
        for (metric in DistanceMetric.entries) {
            val score = sq.computeDistance(q, untrainedBytes, metric)
            assertTrue("Untrained score for $metric must be finite", score.isFinite())
        }

        // First training with wide bounds [-100, 100]
        sq.train(listOf(FloatArray(16) { -100f }, FloatArray(16) { 100f }))
        assertEquals(-100f, sq.getCalibration().first[0], 1e-4f)
        assertEquals(100f, sq.getCalibration().second[0], 1e-4f)

        // Second training with narrower bounds [-1, 1] must overwrite stale [-100, 100] bounds
        sq.train(listOf(FloatArray(16) { -1f }, FloatArray(16) { 1f }))
        assertEquals(-1f, sq.getCalibration().first[0], 1e-4f)
        assertEquals(1f, sq.getCalibration().second[0], 1e-4f)
    }

    @Test
    fun `SimdVectorMath guards out-of-range size, empty arrays, and clamps cosine drift`() {
        val a = FloatArray(19) { (it + 1) * 0.1f }
        val b = FloatArray(19) { (it + 1) * 0.1f }

        // Out-of-bounds size and negative size must not throw ArrayIndexOutOfBoundsException
        assertEquals(SimdVectorMath.dotProduct16(a, b, 19), SimdVectorMath.dotProduct16(a, b, 999), 1e-5f)
        assertEquals(0f, SimdVectorMath.dotProduct16(a, b, -5), 1e-6f)
        assertEquals(0f, SimdVectorMath.euclideanDistanceSq16(a, b, -5), 1e-6f)
        assertEquals(0f, SimdVectorMath.manhattanDistance16(a, b, -5), 1e-6f)

        // Identical vectors must clamp cleanly to <= 1.0f
        val selfCos = SimdVectorMath.cosineSimilarityFused16(a, b)
        assertTrue(selfCos in -1.0f..1.0f)
        assertEquals(1.0f, selfCos, 1e-6f)

        // Empty array in sliceAndNormalizeL216 must return empty array
        val emptySlice = SimdVectorMath.sliceAndNormalizeL216(FloatArray(0), targetDimensions = 64)
        assertEquals(0, emptySlice.size)
    }

    @Test
    fun `KoreEmbeddingPipeline rejects under-sized vectors and tracks in-batch deduplication hits`() = runBlocking {
        val pipeline = KoreEmbeddingPipeline(
            targetDimensions = 128,
            normalizeL2 = true
        ) {
            FloatArray(64) { 1f } // Smaller than targetDimensions (128)
        }

        try {
            pipeline.embed("test")
            fail("Expected IllegalArgumentException when embedder returns fewer dimensions than targetDimensions")
        } catch (expected: IllegalArgumentException) {
            assertTrue(expected.message!!.contains("targetDimensions"))
        }

        val validPipeline = KoreEmbeddingPipeline(
            targetDimensions = 32,
            normalizeL2 = true
        ) {
            FloatArray(64) { 1f }
        }
        validPipeline.embedBatch(listOf("same", "  same ", "same"))
        val stats = validPipeline.getStats()
        // 1 unique model miss + 2 in-batch deduplicated hits
        assertEquals(1L, stats.misses)
        assertEquals(2L, stats.hits)
    }

    @Test
    fun `KoreEmbeddingPipeline caches embeddings, normalizes whitespace, and isolates mutations`() = runBlocking {
        val modelCalls = AtomicInteger(0)
        val pipeline = KoreEmbeddingPipeline(
            targetDimensions = 64,
            normalizeL2 = true,
            maxCacheEntries = 4,
            defensiveCopy = true
        ) { text ->
            modelCalls.incrementAndGet()
            FloatArray(128) { idx -> (text.hashCode() + idx + 1).toFloat() }
        }

        val v1 = pipeline.embed("Hello   KoreDB  World")
        val v2 = pipeline.embed("  Hello KoreDB World ")
        assertEquals(1, modelCalls.get())
        assertEquals(64, v1.size)
        assertArrayEquals(v1, v2, 1e-6f)
        assertEquals(1.0f, SimdVectorMath.getMagnitude16(v1), 1e-5f)

        // Mutating v1 must not corrupt cached vector
        v1[0] = 99999f
        val v3 = pipeline("Hello KoreDB World")
        assertEquals(v2[0], v3[0], 1e-6f)
        assertEquals(1, modelCalls.get())

        val stats = pipeline.getStats()
        assertEquals(2L, stats.hits)
        assertEquals(1L, stats.misses)
        assertEquals(1, stats.cachedEntries)
    }

    @Test
    fun `KoreEmbeddingPipeline embedBatch deduplicates misses and respects LRU capacity`() = runBlocking {
        val batchInvocations = AtomicInteger(0)
        val embeddedInputs = mutableListOf<String>()

        val pipeline = KoreEmbeddingPipeline(
            targetDimensions = 32,
            normalizeL2 = true,
            maxCacheEntries = 3,
            batchEmbedder = { batch ->
                batchInvocations.incrementAndGet()
                embeddedInputs.addAll(batch)
                batch.map { text ->
                    FloatArray(64) { i -> (text.length * (i + 1)).toFloat() }
                }
            },
            embedder = { text ->
                FloatArray(64) { i -> (text.length * (i + 1)).toFloat() }
            }
        )

        // Pre-populate 1 entry
        pipeline.embed("alpha")
        assertEquals(0, batchInvocations.get())

        // Batch with cache hit ("alpha") and duplicate misses ("beta", "  beta ", "gamma")
        val batchResults = pipeline.embedBatch(listOf("alpha", "beta", "  beta ", "gamma"))
        assertEquals(4, batchResults.size)
        assertEquals(1, batchInvocations.get())
        // Only "beta" and "gamma" should have been sent to batchEmbedder!
        assertEquals(listOf("beta", "gamma"), embeddedInputs)
        assertArrayEquals(batchResults[1], batchResults[2], 1e-6f)

        // Adding a 4th unique key ("delta") evicts oldest ("alpha") since maxCacheEntries = 3
        pipeline.embed("delta")
        assertEquals(3, pipeline.getStats().cachedEntries)
    }

    @Test
    fun `KoreEmbeddingPipeline integrates seamlessly with KoreDatabase and GraphVectorBridge`() = runBlocking {
        val db = KoreDatabase.inMemory()
        try {
            val modelCalls = AtomicInteger(0)
            val pipeline = db.embeddingPipeline(
                targetDimensions = 64,
                normalizeL2 = true,
                maxCacheEntries = 128
            ) { text ->
                modelCalls.incrementAndGet()
                val out = FloatArray(128)
                for (i in out.indices) {
                    out[i] = ((text.hashCode() xor (i * 31)) and 0xFF).toFloat() + 1f
                }
                out
            }

            val vectors = db.vectorCollection("docs") {
                dimensions = 64
                metric = DistanceMetric.COSINE
            }
            val bridge = db.graphVectorBridge(vectors)

            val chunkIds = bridge.ingestDocumentWithChunks(
                documentId = "doc_ai",
                title = "KoreDB SOTA Memory",
                text = "KoreDB combines HNSW with prefix short-circuiting and rotated 1-bit quantization.",
                embedder = pipeline.asEmbedder()
            )
            assertTrue(chunkIds.isNotEmpty())
            assertEquals(1, modelCalls.get())

            val queryVec = pipeline("KoreDB combines HNSW with prefix short-circuiting and rotated 1-bit quantization.")
            // Identical text hits the LRU cache without invoking the model a second time!
            assertEquals(1, modelCalls.get())

            val results = vectors.search(queryVec, limit = 1)
            assertEquals(1, results.size)
            assertEquals(chunkIds.first(), results.first().first)
            assertTrue(results.first().second > 0.99f)
        } finally {
            db.close()
        }
    }

    @Test
    fun `HNSWIndex with SQ8 quantization and 64-dim prefix short-circuiting achieves high recall`() {
        val dim = 128
        val numVectors = 160 // > 100 so SQ8 auto-trains and upper HNSW layers (l >= 1) are populated
        val rng = Random(2026)

        val sq8PrefixIndex = com.pankaj.koredb.hnsw.HNSWIndex(
            dimensions = dim,
            metric = DistanceMetric.COSINE,
            m = 16,
            efConstruction = 120,
            efSearch = 64,
            quantizer = ScalarQuantizer(dim)
        )

        val exactIndex = com.pankaj.koredb.hnsw.HNSWIndex(
            dimensions = dim,
            metric = DistanceMetric.COSINE,
            m = 16,
            efConstruction = 120,
            efSearch = 120,
            quantizer = null
        )

        val dataset = HashMap<String, FloatArray>(numVectors)
        for (i in 0 until numVectors) {
            val id = "vec_$i"
            // Generate multi-resolution prefix vector (higher energy in first 64 dims)
            val v = FloatArray(dim) { d ->
                val scale = if (d < 64) 2.0f else 0.5f
                (rng.nextFloat() * 2f - 1f) * scale
            }
            SimdVectorMath.normalizeL2InPlace16(v)
            dataset[id] = v
            sq8PrefixIndex.insert(id, v)
            exactIndex.insert(id, v)
        }

        // Verify upper layers and SQ8 training occurred
        var totalHits = 0
        val numQueries = 20
        for (qIdx in 0 until numQueries) {
            val targetId = "vec_${qIdx * 5}"
            val base = dataset[targetId]!!
            val query = FloatArray(dim) { d -> base[d] + (rng.nextFloat() - 0.5f) * 0.08f }
            SimdVectorMath.normalizeL2InPlace16(query)

            val exactTop10 = exactIndex.search(query, limit = 10, coarsePrefixDims = 0).map { it.first }.toSet()
            val sq8PrefixTop10 = sq8PrefixIndex.search(query, limit = 10, coarsePrefixDims = 64)

            assertEquals("Top-1 must match target $targetId", targetId, sq8PrefixTop10.first().first)
            totalHits += sq8PrefixTop10.count { it.first in exactTop10 }
        }

        val recallAt10 = totalHits.toDouble() / (numQueries * 10)
        assertTrue("SQ8 + Prefix64 Recall@10 should be >= 0.85, was $recallAt10", recallAt10 >= 0.85)
    }

    @Test
    fun `KoreEmbeddingPipeline handles 32-coroutine concurrent stress without race conditions`() = runBlocking {
        val pipeline = KoreEmbeddingPipeline(
            targetDimensions = 64,
            normalizeL2 = true,
            maxCacheEntries = 64,
            defensiveCopy = true
        ) { text ->
            val h = text.hashCode()
            FloatArray(128) { i -> ((h xor (i * 37)) and 0xFF).toFloat() + 1.0f }
        }

        val coroutines = 32
        val iterationsPerCoroutine = 25
        val jobs = (0 until coroutines).map { workerId ->
            launch(kotlinx.coroutines.Dispatchers.Default) {
                for (step in 0 until iterationsPerCoroutine) {
                    val sharedKey = "shared_key_${step % 16}"
                    val uniqueKey = "worker_${workerId}_step_$step"

                    val v1 = pipeline.embed(sharedKey)
                    assertEquals(64, v1.size)
                    assertEquals(1.0f, SimdVectorMath.getMagnitude16(v1), 1e-4f)

                    val batch = pipeline.embedBatch(listOf(sharedKey, uniqueKey, "  $sharedKey  "))
                    assertEquals(3, batch.size)
                    for (vec in batch) {
                        assertEquals(64, vec.size)
                        assertEquals(1.0f, SimdVectorMath.getMagnitude16(vec), 1e-4f)
                    }

                    if (step % 7 == 0) {
                        pipeline.putAll(mapOf("prewarm_${step % 8}" to FloatArray(128) { (it + 1).toFloat() }))
                    }
                    if (step % 11 == 0) {
                        pipeline.evict("shared_key_${(step + 3) % 16}")
                    }
                }
            }
        }
        jobs.forEach { it.join() }

        val stats = pipeline.getStats()
        assertTrue("Cache size must respect maxCacheEntries (64), was ${stats.cachedEntries}", stats.cachedEntries <= 64)
        assertTrue("Total hits (${stats.hits}) + misses (${stats.misses}) must be positive", stats.hits + stats.misses > 0)
        assertTrue("Hit rate should be substantial under shared keys, was ${stats.hitRate}", stats.hitRate > 0.3f)
    }
}
