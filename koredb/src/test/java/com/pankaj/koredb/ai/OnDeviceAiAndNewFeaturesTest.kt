package com.pankaj.koredb.ai

import com.pankaj.koredb.bridge.GraphVectorBridge
import com.pankaj.koredb.core.KoreCompositeIndex
import com.pankaj.koredb.core.KoreIndex
import com.pankaj.koredb.core.KoreNumericIndex
import com.pankaj.koredb.core.KoreProperty
import com.pankaj.koredb.core.KoreSearchable
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Edge
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.hnsw.BinaryQuantizer
import com.pankaj.koredb.hnsw.DistanceMetric
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.nio.file.Files
import kotlin.random.Random

class OnDeviceAiAndNewFeaturesTest {

    private lateinit var tempDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    enum class Tier { FREE, PRO, ENTERPRISE }

    @Serializable
    @KoreCompositeIndex(fields = ["category", "tier"])
    data class AnnotatedArticle(
        val id: String,
        @KoreIndex val category: String = "tech",
        @KoreIndex val tier: Tier = Tier.PRO,
        @KoreNumericIndex val score: Double,
        @KoreSearchable val title: String,
        @KoreSearchable val body: String,
        @KoreProperty val author: String,
        @KoreIndex val tags: List<String> = emptyList()
    )

    @Serializable
    data class LegacyUserV1(
        val id: String,
        val role: String,
        val points: Int
    )

    @Serializable
    data class UpgradedUserV2(
        val id: String,
        @KoreIndex val role: String,
        @KoreNumericIndex val points: Int
    )

    @Before
    fun setUp() {
        tempDir = Files.createTempDirectory("koredb_ai_features_test").toFile()
        db = KoreDatabase(tempDir)
    }

    @After
    fun tearDown() {
        db.close()
        tempDir.deleteRecursively()
    }

    @Test
    fun `test zero-KSP declarative annotations and new KoreQuery operators`() = runBlocking {
        // Notice: zero manual createIndex / createNumericIndex / searchableFields calls!
        val articles = db.binaryCollection<AnnotatedArticle>("articles")

        assertTrue("Secondary index on 'category' should be auto-registered", articles.hasIndex("category"))
        assertTrue("Secondary index on 'tier' should be auto-registered", articles.hasIndex("tier"))
        assertTrue("Numeric index on 'score' should be auto-registered", articles.hasNumericIndex("score"))

        articles.insertBatch(
            mapOf(
                "a1" to AnnotatedArticle("a1", "ai", Tier.PRO, 95.5, "Gemini Nano On-Device", "Local SLM inference with GraphRAG", "Alice"),
                "a2" to AnnotatedArticle("a2", "db", Tier.ENTERPRISE, 88.0, "LSM Storage Engine", "Fast SSTable compaction and MVCC", "Bob"),
                "a3" to AnnotatedArticle("a3", "tech", Tier.FREE, 72.0, "Kotlin Coroutines", "Structured concurrency on Android", "Alice"),
                "a4" to AnnotatedArticle("a4", "ai", Tier.ENTERPRISE, 99.0, "Binary Quantization", "1-bit vector compression with popcount", "Charlie")
            )
        )

        // 1. Test auto-configured equality index & whereIn index pushdown
        val aiAndDb = articles.query()
            .whereIn("category", listOf("ai", "db"))
            .sortBy("score", descending = true) { it.toDouble() }
            .execute()
        assertEquals(listOf("a4", "a1", "a2"), aiAndDb.map { it.id })

        // 2. Test enum @KoreIndex + default property value ("tech")
        val defaultCategory = articles.getByIndex("category", "tech")
        assertEquals(1, defaultCategory.size)
        assertEquals("a3", defaultCategory.first().id)

        val enterpriseTier = articles.getByIndex("tier", "ENTERPRISE")
        assertEquals(setOf("a2", "a4"), enterpriseTier.map { it.id }.toSet())

        // 3. Test auto-configured @KoreNumericIndex range query
        val highScorers = articles.query()
            .whereBetween("score", 90.0, 100.0)
            .execute()
        assertEquals(setOf("a1", "a4"), highScorers.map { it.id }.toSet())

        // 4. Test auto-configured @KoreSearchable BM25 full-text search
        val ftsResults = articles.searchBM25("GraphRAG inference")
        assertTrue(ftsResults.isNotEmpty())
        assertEquals("a1", ftsResults.first().first.id)

        // 5. Test whereNotEq, whereStartsWith, whereContains, firstOrNull, exists
        val aliceNonFree = articles.query()
            .whereEq("author", "Alice")
            .whereNotEq("category", "tech")
            .firstOrNull()
        assertNotNull(aliceNonFree)
        assertEquals("a1", aliceNonFree?.id)

        assertTrue(articles.query().whereStartsWith("title", "Binary").exists())
        assertFalse(articles.query().whereStartsWith("title", "NonExistent").exists())
        assertEquals(1, articles.query().whereContains("body", "compaction").count())
    }

    @Test
    fun `test 1-bit BinaryQuantizer 32x compression and two-stage retrieval`() {
        val dims = 256
        val bq = BinaryQuantizer(dims)
        assertEquals(4, bq.wordCount) // 256 bits = 4 Long words = 32 bytes (vs 1024 bytes for Float32 = 32x compression)

        val rng = Random(42)
        val exactVectors = HashMap<String, FloatArray>()
        val binaryIndex = HashMap<String, LongArray>()

        for (i in 0 until 200) {
            val vec = FloatArray(dims) { (rng.nextFloat() - 0.5f) * 2f }
            val id = "vec_$i"
            exactVectors[id] = vec
            binaryIndex[id] = bq.quantize(vec)
        }

        // Verify identical vector has 0 Hamming distance and 1.0 similarity
        val sampleVec = exactVectors["vec_0"]!!
        val sampleCode = binaryIndex["vec_0"]!!
        assertEquals(0, bq.hammingDistance(sampleCode, sampleCode))
        assertEquals(1.0f, bq.hammingSimilarity(sampleCode, sampleCode), 1e-5f)

        // Verify pack/unpack and quantizer serialization round-trip
        val packedBytes = bq.packToBytes(sampleCode)
        assertEquals(32, packedBytes.size)
        assertArrayEquals(sampleCode, bq.unpackFromBytes(packedBytes))

        bq.train(exactVectors.values)
        val restoredBq = BinaryQuantizer.fromByteArray(bq.toByteArray())
        assertTrue(restoredBq.isTrained)
        assertEquals(dims, restoredBq.dimensions)

        // Verify two-stage search finds a slightly perturbed target vector as #1 match
        val query = FloatArray(dims) { idx -> sampleVec[idx] + (rng.nextFloat() - 0.5f) * 0.05f }
        val results = bq.searchTwoStage(
            query = query,
            binaryIndex = binaryIndex,
            limit = 5,
            oversampleFactor = 5,
            metric = DistanceMetric.COSINE
        ) { id -> exactVectors[id] }

        assertEquals(5, results.size)
        assertEquals("vec_0", results.first().first)
        assertTrue("Top match cosine similarity should be > 0.98", results.first().second > 0.98f)
    }

    @Test
    fun `test KoreTextChunker and auto-chunked GraphRAG ingestion with context packing`() = runBlocking {
        val longDoc = """
            KoreDB is a multi-model embedded storage engine for Android and Kotlin.
            It combines an LSM-Tree key-value store, HNSW vector search, and property graphs.

            For on-device AI applications, KoreDB provides GraphRAG and Reciprocal Rank Fusion.
            Documents are automatically split into overlapping chunks and linked in the knowledge graph.

            Small language models like Gemini Nano have strict context window token budgets.
            KoreDB packs the most relevant chunks and graph relations without exceeding the token limit.
        """.trimIndent()

        val vecCol = db.vectorCollection("rag_vectors") {
            dimensions = 16
            metric = DistanceMetric.COSINE
        }
        val bridge = db.graphVectorBridge(vecCol)

        // Deterministic mock embedder based on keywords
        val embedder: suspend (String) -> FloatArray = { text ->
            val lower = text.lowercase()
            FloatArray(16) { i ->
                when (i) {
                    0 -> if ("lsm" in lower || "storage" in lower) 1.0f else 0.1f
                    1 -> if ("graphrag" in lower || "chunks" in lower) 1.0f else 0.1f
                    2 -> if ("gemini" in lower || "token" in lower || "budget" in lower) 1.0f else 0.1f
                    else -> 0.05f * (i + 1)
                }
            }
        }

        val chunkIds = bridge.ingestDocumentWithChunks(
            documentId = "doc_koredb",
            title = "KoreDB AI Guide",
            text = longDoc,
            maxChunkChars = 180,
            overlapChars = 30,
            metadata = mapOf("topic" to "database"),
            embedder = embedder
        )

        assertTrue("Should produce multiple chunks", chunkIds.size >= 3)

        // Verify graph structure: HAS_CHUNK, CHUNK_OF, and NEXT_CHUNK edges
        val graph = db.graph()
        val hasChunkTargets = graph.getOutboundTargetIds("doc_koredb", "HAS_CHUNK")
        assertEquals(chunkIds.toSet(), hasChunkTargets.toSet())
        assertTrue(graph.getOutboundTargetIds(chunkIds[0], "NEXT_CHUNK").contains(chunkIds[1]))

        // Add a custom domain relationship to test relationship inclusion in context packer
        graph.putNode(Node("gemini_nano", setOf("Model"), mapOf("title" to "Gemini Nano")))
        graph.putEdge(Edge(chunkIds.last(), "gemini_nano", "OPTIMIZED_FOR"))

        // Assemble token-budgeted context for a query about Gemini Nano token budgets
        val queryVec = embedder("gemini nano token budget")
        val contextWindow = bridge.assemblePromptContext(
            query = queryVec,
            maxTokenBudget = 80,
            candidateLimit = 5,
            maxHops = 1,
            includeGraphRelationships = true
        )

        assertTrue(contextWindow.formattedContext.isNotEmpty())
        assertTrue(
            "Estimated tokens (${contextWindow.estimatedTokens}) must not exceed budget (80)",
            contextWindow.estimatedTokens <= 80
        )
        assertTrue(contextWindow.includedNodeIds.isNotEmpty())
    }

    @Test
    fun `test agentic episodic memory retrieval with temporal decay and importance`() = runBlocking {
        val vecCol = db.vectorCollection("agent_memories") {
            dimensions = 8
            metric = DistanceMetric.COSINE
        }
        val graph = db.graph()
        val bridge = GraphVectorBridge(graph, vecCol)

        val now = 1_700_000_000_000L
        val oneHourMs = 3_600_000L
        val baseEmbedding = floatArrayOf(1f, 0.5f, 0.2f, 0.1f, 0f, 0f, 0f, 0f)

        // Memory 1: Very old (7 days ago), low importance (0.1), identical embedding
        graph.putNode(
            Node(
                id = "mem_stale",
                labels = setOf("Memory"),
                properties = mapOf(
                    "text" to "User mentioned liking tea last week",
                    "timestampMs" to (now - 168 * oneHourMs).toString(),
                    "importance" to "0.1"
                )
            )
        )
        vecCol.insert("mem_stale", baseEmbedding)

        // Memory 2: Fresh (30 mins ago), high importance (0.95), nearly identical embedding
        graph.putNode(
            Node(
                id = "mem_fresh_important",
                labels = setOf("Memory"),
                properties = mapOf(
                    "text" to "User is allergic to peanuts (critical preference)",
                    "timestampMs" to (now - oneHourMs / 2).toString(),
                    "importance" to "0.95"
                )
            )
        )
        vecCol.insert("mem_fresh_important", floatArrayOf(0.99f, 0.5f, 0.2f, 0.1f, 0f, 0f, 0f, 0f))
        vecCol.waitForIndexing()

        val memories = bridge.searchMemory(
            query = baseEmbedding,
            limit = 5,
            nowMs = now,
            halfLifeHours = 24.0
        )

        assertEquals(2, memories.size)
        assertEquals("Fresh, high-importance memory must rank first", "mem_fresh_important", memories[0].id)
        assertTrue(memories[0].recencyScore > 0.95f)
        assertTrue(memories[1].recencyScore < 0.05f)
        assertTrue(memories[0].finalScore > memories[1].finalScore)
    }

    @Test
    fun `test KoreSemanticCache hit, miss, context partitioning, and stats`() = runBlocking {
        val cache = db.semanticCache(
            name = "assistant_cache",
            dimensions = 8,
            similarityThreshold = 0.92f
        )

        val promptVec1 = floatArrayOf(0.9f, 0.4f, 0.1f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f)
        // Paraphrased prompt vector (cosine similarity > 0.99)
        val paraphrasedVec1 = floatArrayOf(0.89f, 0.42f, 0.11f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f)
        // Orthogonal prompt vector
        val unrelatedVec = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f, 0.9f, 0.4f, 0.1f, 0.0f)

        var generatorCalls = 0
        val (resp1, hit1) = cache.getOrGenerate(
            prompt = "What is the weather in Tokyo?",
            promptVector = promptVec1,
            contextTag = "weather_bot"
        ) {
            generatorCalls++
            "It is 22°C and sunny in Tokyo."
        }
        assertFalse("First call should be a cache miss", hit1)
        assertEquals(1, generatorCalls)
        assertEquals("It is 22°C and sunny in Tokyo.", resp1)

        // Second call with paraphrased embedding and same contextTag -> Semantic Cache HIT!
        val (resp2, hit2) = cache.getOrGenerate(
            prompt = "Tell me Tokyo's current weather",
            promptVector = paraphrasedVec1,
            contextTag = "weather_bot"
        ) {
            generatorCalls++
            "Should not be called"
        }
        assertTrue("Paraphrased prompt should be a semantic cache hit", hit2)
        assertEquals("Generator must not execute on cache hit", 1, generatorCalls)
        assertEquals("It is 22°C and sunny in Tokyo.", resp2)

        // Different contextTag -> Cache MISS
        val missDifferentContext = cache.lookup(paraphrasedVec1, contextTag = "coding_bot")
        assertNull("Different contextTag must not hit weather_bot cache", missDifferentContext)

        // Unrelated vector -> Cache MISS
        val missUnrelated = cache.lookup(unrelatedVec, contextTag = "weather_bot")
        assertNull("Unrelated prompt embedding must miss cache", missUnrelated)

        val stats = cache.getStats()
        assertEquals(1L, stats.hits)
        assertEquals(3L, stats.misses)

        // Invalidate by contextTag
        cache.invalidateByContext("weather_bot")
        assertNull(cache.lookup(promptVec1, contextTag = "weather_bot"))
    }

    @Test
    fun `test automatic lazy index backfill on schema evolution (V1 to V2 upgrade)`() = runBlocking {
        // 1. Simulate App V1: Insert records WITHOUT any @KoreIndex or @KoreNumericIndex
        val usersV1 = db.collection<LegacyUserV1>("users_upgrade_test")
        usersV1.insertBatch(
            mapOf(
                "u1" to LegacyUserV1("u1", "admin", 500),
                "u2" to LegacyUserV1("u2", "member", 120),
                "u3" to LegacyUserV1("u3", "admin", 950)
            )
        )
        db.clearCollectionCache()

        // 2. Simulate App V2: Open the exact same collection with UpgradedUserV2 (@KoreIndex + @KoreNumericIndex)
        // Notice: NO manual rebuildIndexes() call!
        val usersV2 = db.collection<UpgradedUserV2>("users_upgrade_test")
        assertTrue(usersV2.hasIndex("role"))
        assertTrue(usersV2.hasNumericIndex("points"))

        val admins = usersV2.query().whereEq("role", "admin").execute()
        assertEquals("Existing V1 documents must be lazily backfilled into 'role' index", setOf("u1", "u3"), admins.map { it.id }.toSet())

        val highPoints = usersV2.query().whereBetween("points", 400.0, 1000.0).execute()
        assertEquals("Existing V1 documents must be lazily backfilled into 'points' numeric index", setOf("u1", "u3"), highPoints.map { it.id }.toSet())
    }

    @Test
    fun `test multi-value List KSerializer KoreIndex and KoreCompositeIndex pushdown`() = runBlocking {
        val articles = db.binaryCollection<AnnotatedArticle>("articles_multi_comp")

        assertTrue("Multi-value index on 'tags' should be auto-registered", articles.hasMultiValueIndex("tags"))
        assertTrue("Composite index 'category+tier' should be auto-registered", articles.hasIndex("category+tier"))

        articles.insertBatch(
            mapOf(
                "a1" to AnnotatedArticle("a1", "ai", Tier.PRO, 95.5, "Gemini Nano", "SLM inference", "Alice", listOf("android", "llm", "edge")),
                "a2" to AnnotatedArticle("a2", "ai", Tier.ENTERPRISE, 99.0, "GraphRAG", "Knowledge graphs", "Bob", listOf("graph", "llm", "rag")),
                "a3" to AnnotatedArticle("a3", "db", Tier.ENTERPRISE, 88.0, "LSM Trees", "Storage engine", "Charlie", listOf("storage", "kotlin", "android"))
            )
        )

        // 1. Multi-value tag lookup via getByIndex and Query DSL whereEq
        val llmArticles = articles.query().whereEq("tags", "llm").execute()
        assertEquals(setOf("a1", "a2"), llmArticles.map { it.id }.toSet())

        // 2. Multi-value whereIn with automatic deduplication (a1 matches both "android" and "llm")
        val androidOrLlm = articles.query().whereIn("tags", listOf("android", "llm")).execute()
        assertEquals(3, androidOrLlm.size)
        assertEquals(setOf("a1", "a2", "a3"), androidOrLlm.map { it.id }.toSet())

        // 3. Composite index lookup via getByCompositeIndex and automatic Query DSL pushdown
        val compositeDirect = articles.getByCompositeIndex("category+tier", listOf("ai", "ENTERPRISE"))
        assertEquals(listOf("a2"), compositeDirect.map { it.id })

        val compositeQuery = articles.query()
            .whereEq("category", "ai")
            .whereEq("tier", "ENTERPRISE")
            .execute()
        assertEquals(listOf("a2"), compositeQuery.map { it.id })
    }

    @Test
    fun `test Maximal Marginal Relevance (MMR) diversity search and Matryoshka progressive search`() = runBlocking {
        // 1. Test MMR Diversity Search
        val mmrCol = db.vectorCollection("mmr_vectors") {
            dimensions = 8
            metric = DistanceMetric.COSINE
        }

        // Insert 3 near-duplicate vectors on Topic A and 1 distinct vector on Topic B
        val query = floatArrayOf(1.0f, 0.5f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f)
        mmrCol.insert("topicA_1", floatArrayOf(1.0f, 0.49f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f))
        mmrCol.insert("topicA_2", floatArrayOf(0.99f, 0.48f, 0.01f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f))
        mmrCol.insert("topicA_3", floatArrayOf(0.98f, 0.47f, 0.02f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f))
        // Topic B is relevant to query (shares dim 1 = 0.85f, dim 0 = 0.5f) but orthogonal to Topic A's redundancy
        mmrCol.insert("topicB_diverse", floatArrayOf(0.55f, 0.85f, 0.0f, 0.4f, 0.0f, 0.0f, 0.0f, 0.0f))
        mmrCol.waitForIndexing()

        val standardTop2 = mmrCol.search(query, limit = 2).map { it.first }
        assertEquals("Standard top-2 picks two redundant Topic A items", listOf("topicA_1", "topicA_2"), standardTop2)

        val diverseTop2 = mmrCol.searchMMR(query, limit = 2, fetchK = 4, diversityLambda = 0.35f).map { it.first }
        assertEquals("MMR top-2 must pick best Topic A item and then diverse Topic B item", listOf("topicA_1", "topicB_diverse"), diverseTop2)

        // 2. Test Matryoshka Representation Learning (MRL) Two-Stage Search
        val dims = 128
        val mrlCol = db.vectorCollection("mrl_vectors") {
            dimensions = dims
            metric = DistanceMetric.COSINE
        }
        val rng = Random(99)
        var targetVec = FloatArray(dims)
        for (i in 0 until 120) {
            val vec = FloatArray(dims) { (rng.nextFloat() - 0.5f) * 2f }
            if (i == 7) targetVec = vec
            mrlCol.insert("mrl_$i", vec)
        }
        mrlCol.waitForIndexing()

        val mrlQuery = FloatArray(dims) { idx -> targetVec[idx] + (rng.nextFloat() - 0.5f) * 0.02f }
        val mrlResults = mrlCol.searchMatryoshka(
            query = mrlQuery,
            limit = 5,
            coarseDims = 32,
            oversampleFactor = 4
        )
        assertEquals(5, mrlResults.size)
        assertEquals("Matryoshka 32d->128d search must find exact target mrl_7", "mrl_7", mrlResults.first().first)
    }

    @Test
    fun `test auto-extracted Entity graph linking across documents in GraphRAG`() = runBlocking {
        val vecCol = db.vectorCollection("entity_rag_vectors") {
            dimensions = 8
            metric = DistanceMetric.COSINE
        }
        val bridge = db.graphVectorBridge(vecCol)
        val graph = db.graph()

        // Doc 1 mentions "Project Apollo" and "#OnDeviceAI" with an embedding close to the query
        bridge.ingestDocumentWithChunks(
            documentId = "doc_1",
            title = "Apollo Launch Memo",
            text = "The team initialized Project Apollo for #OnDeviceAI inference on mobile.",
            extractEntities = true,
            embedder = { floatArrayOf(1.0f, 0.2f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f) }
        )

        // Doc 2 also mentions "Project Apollo" and "@sarah" (linked via shared entity:project_apollo!)
        bridge.ingestDocumentWithChunks(
            documentId = "doc_2",
            title = "Apollo Architecture Spec",
            text = "Lead engineer @sarah confirmed Project Apollo uses 1-bit BinaryQuantizer.",
            extractEntities = true,
            embedder = { floatArrayOf(0.5f, 0.8f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f) }
        )

        // Verify shared entity node "entity:project_apollo" was created and connects both chunks
        val apolloEntity = graph.getNode("entity:project_apollo")
        assertNotNull("Shared entity node 'entity:project_apollo' must be created", apolloEntity)
        assertEquals("Project Apollo", apolloEntity?.properties?.get("name"))

        val mentionedInChunks = graph.getOutboundTargetIds("entity:project_apollo", "MENTIONED_IN").toSet()
        assertEquals(setOf("doc_1#chunk_0", "doc_2#chunk_0"), mentionedInChunks)

        // Verify 2-hop GraphRAG starting from doc_1#chunk_0 (initialLimit = 1) discovers doc_2#chunk_0 via entity:project_apollo!
        val ragResults = bridge.graphRAGQuery(
            query = floatArrayOf(1.0f, 0.2f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f),
            initialLimit = 1,
            maxHops = 2,
            finalLimit = 10
        )
        val discoveredIds = ragResults.map { it.id }.toSet()
        assertTrue("2-hop GraphRAG must discover doc_2#chunk_0 via shared entity node", "doc_2#chunk_0" in discoveredIds)

        // Verify assemblePromptContext with diversityLambda packs both chunks cleanly without standalone Entity clutter
        val context = bridge.assemblePromptContext(
            query = floatArrayOf(1.0f, 0.2f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f),
            maxTokenBudget = 256,
            candidateLimit = 1,
            maxHops = 2,
            diversityLambda = 0.6f
        )
        assertTrue(context.includedNodeIds.containsAll(listOf("doc_1#chunk_0", "doc_2#chunk_0")))
        assertFalse("Bare Entity node should not be emitted as a passage bullet", "entity:project_apollo" in context.includedNodeIds)
    }
}
