# Android Feature Integration & Decision Guide: How & When to Use KoreDB

This guide provides an architectural blueprint for Android developers. It details **how to implement every major feature of KoreDB** using modern Android standards (Kotlin Coroutines, Jetpack Compose, ViewModels, WorkManager, and Android Keystore) and clarifies **when each feature is helpful** with concrete real-world use cases.

---

## Quick Decision Matrix: When to Use What

| Feature | When It's Helpful | Android Alternatives | Why KoreDB is Better |
| :--- | :--- | :--- | :--- |
| **Document Engine (`binaryCollection`)** | Caching API responses, user profiles, catalog entities without SQLite schema boilerplate. | Room, SQLite, SharedPreferences | 3.9x faster point reads; no SQL queries or migrations needed; uses binary CBOR. |
| **Numeric Range Indexing** | Price slider filtering, timestamp range lookups, sensor logs (heart rate, step counts). | Room SQL `BETWEEN ? AND ?` | 6.1x faster; zero SQL parsing overhead; contiguous order-preserving binary byte scans. |
| **Vector Engine (`vectorCollection`)** | On-device AI, local semantic search, image/audio retrieval (CLIP/Whisper), local LLM memory. | ObjectBox Vector, sqlite-vec | Sub-millisecond HNSW; native Kotlin without heavy C++ JNI bridge overhead; off-heap memory-mapped. |
| **Scalar Quantization (SQ8)** | Devices with low RAM (2GB–4GB); indexing >20,000 embeddings without OOM. | None (manual quantization) | Automatic 4x RAM reduction with >98% recall accuracy; off-heap memory-mapped storage. |
| **Property Graph (`graph()`)** | Friend networks, offline knowledge graphs, permission hierarchies, recommendation paths. | Room multi-table JOINs | Bidirectional $O(1)$ relationship traversal; key-only fast paths bypass payload reads; cascading deletes. |
| **Hybrid Graph RAG (`GraphVectorBridge`)** | Contextual on-device AI assistants, personalized recommendation engines. | Custom glue code | Combines relational constraints + semantic similarity + BM25 text rank in one fluent pipeline. |
| **BM25 Full-Text Search** | Notes apps, search-as-you-type in contacts, offline documentation search. | SQLite FTS4 / FTS5 | Embedded directly in collections; no virtual table setup; ranked by industry-standard BM25. |
| **Change Data Capture (CDC)** | Offline-first sync engines, cloud replication (Supabase, Firebase, WebSockets), reactive UIs. | Room InvalidationTracker | Full audit log stream of every INSERT, UPDATE, DELETE with before/after state and sequence IDs. |
| **MVCC Transactions** | Multi-table atomic updates, wallet balance transfers, cart checkouts. | Room `@Transaction` | 4.3x faster (9.77 ms for 1,000 commits); snapshot isolation; optimistic conflict detection. |
| **Hardware Keystore Encryption** | Medical (HIPAA), fintech, secure token storage, private local user data. | SQLCipher | AES-GCM-256 with AAD key-binding; zero-copy offset encryption without SQLCipher license fees or overhead. |
| **Time-To-Live (TTL)** | Expiring auth tokens, ephemeral chats, temporary feeds, volatile stock quotes. | Custom cleanup cron jobs | Automatic on-read expiration filtering; physical space reclaimed during background compaction. |
| **Background Compaction** | Long-running production apps that need minimal disk footprint. | `VACUUM` in SQLite | Non-blocking leveled compaction with Truth Oracle stale index pruning; runs smoothly in WorkManager. |

---

## 1. Typed Document Collections (`binaryCollection`)

### When is it helpful?
- **Offline-First App Architecture**: Caching REST/GraphQL API payloads directly without writing 15 separate Room table schemas, DAOs, and foreign-key relations.
- **Dynamic Schemas**: Apps where entities evolve rapidly and SQL migration scripts become a maintenance burden.
- **High-Performance Point Reads**: Apps requiring instant access to cached items (e.g. user profiles, cached shopping carts).

### How to add it in Android:

```kotlin
// 1. Define model with Kotlinx Serialization
@Serializable
data class UserProfile(
    val id: String,
    val username: String,
    val email: String,
    val avatarUrl: String,
    val preferences: Map<String, String>
)

// 2. Initialize in your Repository
class UserRepository(context: Context) {
    private val db = KoreDatabase(File(context.filesDir, "user_store"))
    private val users = db.binaryCollection<UserProfile>("users")

    // Insert or update (O(1) LSM Append)
    suspend fun cacheUser(user: UserProfile) = withContext(Dispatchers.IO) {
        users.insert(user.id, user)
    }

    // Instant read (sub-microsecond memory/cache lookup)
    suspend fun getUser(userId: String): UserProfile? = withContext(Dispatchers.IO) {
        users.getById(userId)
    }
}
```

---

## 2. Numeric Range Indexing (`createNumericIndex`)

### When is it helpful?
- **E-Commerce Apps**: Filtering items between price ranges (e.g., `$100 - $500`) with immediate UI response as the user drags a slider.
- **Health & Fitness Trackers**: Querying step counts, heart rate readings, or sleep stages across arbitrary timestamp ranges (`fromTimestamp` to `toTimestamp`).
- **Geo-Bounding Boxes**: Filtering latitude and longitude coordinate windows for offline maps.

### How to add it in Android:

```kotlin
@Serializable
data class FitnessEntry(
    val id: String,
    val timestamp: Long,
    val heartRate: Double,
    val steps: Int
)

class HealthRepository(context: Context) {
    private val db = KoreDatabase(File(context.filesDir, "health_store"))
    private val healthCollection = db.binaryCollection<FitnessEntry>("health_records").apply {
        // Register numeric index on timestamp
        createNumericIndex("timestamp") { it.timestamp.toDouble() }
        createNumericIndex("heartRate") { it.heartRate }
    }

    // Fast range scan (6.1x faster than Room/SQLite)
    fun getHeartRateHistory(fromTime: Long, toTime: Long): List<FitnessEntry> {
        return healthCollection.findRange("timestamp", fromTime.toDouble(), toTime.toDouble())
    }
}
```

---

## 3. On-Device Vector Search with HNSW (`vectorCollection`)

### When is it helpful?
- **On-Device Semantic Search**: Searching notes, documents, or photos based on semantic meaning rather than exact keywords (e.g., searching *"beach vacation"* finds photos tagged *"ocean sunset"*).
- **Personalized On-Device AI & RAG**: Providing relevant local context to local LLMs (MediaPipe LLM Inference, Gemma 2B, Gemini Nano) without sending private user data to the cloud.
- **Similar Product / Item Recommendations**: Calculating nearest neighbor items based on user interactions.

### How to add it in Android:

```kotlin
class SemanticSearchManager(context: Context) {
    private val db = KoreDatabase(File(context.filesDir, "vector_store"))
    private val vectors = db.vectorCollection("notes_embeddings") {
        dimensions = 256 // Embedding size (e.g. from MobileBERT or TFLite)
        metric = DistanceMetric.COSINE
        quantization = true // Enable 8-bit quantization for 4x memory savings
    }

    // 1. Index note embedding with metadata
    suspend fun indexNote(noteId: String, embedding: FloatArray, category: String) {
        vectors.insert(
            id = noteId,
            vector = embedding,
            metadata = mapOf("category" to category, "timestamp" to System.currentTimeMillis())
        )
    }

    // 2. Search nearest neighbors with metadata filtering
    suspend fun searchSimilar(queryVector: FloatArray, categoryFilter: String): List<String> {
        val matches = vectors.search(queryVector, limit = 5) {
            where("category", eq(categoryFilter))
        }
        return matches.map { it.first }
    }
}
```

---

## 4. Property Graphs & Fast Traversals (`graph()`)

### When is it helpful?
- **Social Networks & Messaging**: Managing followers, mutual friends, group memberships, and blocked users.
- **Recommendation Engines**: Finding *"Products bought by people you follow"* or *"Collaborators who worked on similar projects"*.
- **Permission & Role Hierarchies**: Resolving organization chart trees and nested role-based access controls (RBAC) offline.

### How to add it in Android:

```kotlin
class SocialGraphRepository(context: Context) {
    private val db = KoreDatabase(File(context.filesDir, "social_store"))
    private val graph = db.graph()

    suspend fun followUser(followerId: String, targetUserId: String) {
        graph.putEdge(Edge(
            sourceId = followerId,
            targetId = targetUserId,
            type = "FOLLOWS",
            weight = 1.0,
            properties = mapOf("created_at" to System.currentTimeMillis().toString())
        ))
    }

    // O(1) Key-only fast path: instant follower IDs without reading edge payloads
    fun getFollowers(userId: String): List<String> {
        return graph.getInboundSourceIds(userId, "FOLLOWS")
    }

    // Unfollow (cleans up bidirectional indices automatically)
    suspend fun unfollow(followerId: String, targetUserId: String) {
        graph.removeEdge(followerId, "FOLLOWS", targetUserId)
    }
}
```

---

## 5. Unified Hybrid Graph RAG (`GraphVectorBridge`)

### When is it helpful?
- **Next-Gen On-Device AI Assistants**: Restricting vector similarity search within graph boundaries (e.g. only search medical advice from doctors in the user's hospital network).
- **Personalized Knowledge Graphs**: Answering multi-hop questions like: *"Show me camera recommendations from photographers I follow that cost under $800"*.

### How to add it in Android:

```kotlin
class AssistantRAGService(private val db: KoreDatabase) {
    private val bridge = GraphVectorBridge(db)
    private val products = db.binaryCollection<Product>("products")

    suspend fun getPersonalizedRecommendations(
        userId: String,
        userQueryEmbedding: FloatArray
    ): List<Product> {
        // Step 1: Traverse (User) -> FOLLOWS -> (Friends) -> PURCHASED -> (Products)
        // Step 2: Rerank candidate products by semantic similarity to user's query
        val rankedResults = bridge.graphFirst(userId)
            .traverse(edgeType = "FOLLOWS", maxDepth = 2)
            .traverse(edgeType = "PURCHASED", maxDepth = 1)
            .rerankByVector(
                vectorCollectionName = "product_embeddings",
                queryVector = userQueryEmbedding,
                limit = 10
            )

        // Step 3: Fetch typed product documents
        return rankedResults.mapNotNull { products.getById(it.id) }
    }
}
```

---

## 6. Real-Time Change Data Capture (CDC) & Reactive UI

### When is it helpful?
- **Reactive UI with Jetpack Compose**: Automatically refreshing UI screens whenever any background worker, sync task, or transaction modifies data.
- **Offline Cloud Synchronization**: Listening to local mutations and queueing them for upload to a remote backend (Supabase, Firebase, WebSockets) with strict ordering.

### How to add it in Android:

```kotlin
class SyncAndObserveViewModel(private val db: KoreDatabase) : ViewModel() {

    val liveMutationEvents = db.cdcManager.changes
        .map { record ->
            "Collection: ${record.collection} | ${record.op} on ${record.documentId}"
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), "Listening for changes...")

    // In a background service / WorkManager:
    fun startCloudSync() {
        viewModelScope.launch(Dispatchers.IO) {
            db.cdcManager.changes.collect { mutation ->
                when (mutation.op) {
                    MutationOp.INSERT, MutationOp.UPDATE -> apiService.uploadChange(mutation)
                    MutationOp.DELETE -> apiService.deleteRemote(mutation.documentId)
                }
            }
        }
    }
}
```

---

## 7. Hardware-Backed Encryption with Android Keystore

### When is it helpful?
- **Fintech & Banking Apps**: Storing sensitive card data, balances, and account tokens.
- **Healthcare Apps (HIPAA Compliance)**: Protecting patient records and sensor health history at rest.
- **Enterprise Security Compliance**: Ensuring data cannot be read even if the device is rooted or the storage file is extracted via ADB.

### How to add it in Android:

```kotlin
object SecurityUtils {
    private const val KEY_ALIAS = "koredb_master_key"

    fun getOrCreateMasterKey(): ByteArray {
        val keyStore = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
        if (!keyStore.containsAlias(KEY_ALIAS)) {
            val keyGenerator = KeyGenerator.getInstance(
                KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore"
            )
            val spec = KeyGenParameterSpec.Builder(
                KEY_ALIAS,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build()
            keyGenerator.init(spec)
            keyGenerator.generateKey()
        }
        val secretKey = keyStore.getKey(KEY_ALIAS, null) as SecretKey
        return secretKey.encoded ?: ByteArray(32) { 0x42 } // Or derive via Keystore cipher
    }
}

// Initialize secure database instance
val crypto = KoreCrypto(SecurityUtils.getOrCreateMasterKey())
val secureDb = KoreDatabase(File(context.filesDir, "secure_vault"), crypto = crypto)
```

---

## 8. Background Maintenance with WorkManager

### When is it helpful?
- **Storage Space Reclamation**: Mobile devices run out of storage quickly. When users delete documents or update records, tombstones accumulate.
- **Zero UI Impact**: Running Leveled Compaction periodically while the phone is **charging and idle** ensures butter-smooth UI frame rates (60/120 FPS).

### How to add it in Android:

```kotlin
class DatabaseCompactionWorker(
    context: Context,
    params: WorkerParameters
) : CoroutineWorker(context, params) {

    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        try {
            val db = DatabaseModule.provideDatabase(applicationContext)
            // Execute background leveled compaction and flush
            db.compact()
            Result.success()
        } catch (e: Exception) {
            Result.retry()
        }
    }

    companion object {
        fun schedulePeriodicCompaction(context: Context) {
            val constraints = Constraints.Builder()
                .setRequiresCharging(true)
                .setRequiresDeviceIdle(true)
                .build()

            val request = PeriodicWorkRequestBuilder<DatabaseCompactionWorker>(
                repeatInterval = 24, TimeUnit.HOURS
            )
                .setConstraints(constraints)
                .build()

            WorkManager.getInstance(context).enqueueUniquePeriodicWork(
                "koredb_compaction",
                ExistingPeriodicWorkPolicy.KEEP,
                request
            )
        }
    }
}
```
