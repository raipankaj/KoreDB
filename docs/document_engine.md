# Document Collection Engine

The Document Engine in KoreDB provides high-performance persistence and querying for Kotlin `@Serializable` data classes.

KoreDB offers two collection types:
1. **`binaryCollection<T>` (Recommended)**: Backed by **CBOR (Concise Binary Object Representation)**. Provides up to 3x faster serialization and 40% smaller storage size than JSON.
2. **`collection<T>`**: Backed by **JSON**. Ideal for debugging, human-readable inspection, or interoperability.

---

## 1. Defining Models and Collections

```kotlin
import kotlinx.serialization.Serializable
import com.pankaj.koredb.db.KoreDatabase
import java.io.File

@Serializable
data class User(
    val id: String,
    val name: String,
    val email: String,
    val age: Int,
    val balance: Double,
    val role: String
)

val db = KoreDatabase(File(context.filesDir, "koredb"))

// Obtain typed binary collection
val users = db.binaryCollection<User>("users")
```

---

## 2. Ingestion (CRUD)

### Single Ingestion
```kotlin
// Insert or replace (O(1) LSM Append)
users.insert("u_101", User("u_101", "Alice Smith", "alice@example.com", 29, 1450.50, "admin"))

// Point Lookup (10 µs latency, 3.9x faster than Room)
val alice: User? = users.getById("u_101")

// Delete (Instant O(1) Tombstone Write)
users.delete("u_101")
```

### High-Throughput Bulk Ingestion
KoreDB supports inserting directly from an arbitrary `Collection<T>` using an ID extractor lambda to avoid allocating intermediate hash maps:

```kotlin
val newUsers: List<User> = fetchUsersFromNetwork()

// Ingests thousands of items in parallel with pre-sized batch allocation
users.insertBatch(newUsers) { it.id }
```

### Batch Deletion
```kotlin
users.deleteBatch(listOf("u_101", "u_102", "u_103"))
```

---

## 3. Secondary Indexing

Secondary indices in KoreDB are maintained atomically alongside primary documents in the LSM write batch.

### String Indexing
```kotlin
// Register secondary index on 'role'
users.createIndex("role") { it.role }

// Fast O(log N) Seek (5.7x faster than Room/SQLite)
val admins: List<User> = users.find("role", "admin")
```

### Numeric Range Indexing
Numeric indices use **Order-Preserving IEEE-754 Float/Double Encoding**, mapping double values into byte arrays that preserve lexical sort order.

```kotlin
// Register numeric index on 'balance'
users.createNumericIndex("balance") { it.balance }

// Range query: [min, max] inclusive (6.1x faster than Room/SQLite)
val affluentUsers = users.findRange("balance", 1000.0, 5000.0)

// Open-ended range query
val lowBalanceUsers = users.findRange("balance", Double.MIN_VALUE, 50.0)
```

---

## 4. Full-Text Search (BM25)

KoreDB includes a built-in Okapi BM25 full-text search engine embedded directly inside the document collection:

```kotlin
// Register searchable text fields
users.createSearchableIndex("name") { it.name }
users.createSearchableIndex("email") { it.email }

// Search BM25 ranked by relevance
val searchResults: List<Pair<User, Float>> = users.searchBM25("Alice Smith", limit = 10)
searchResults.forEach { (user, score) ->
    println("Found ${user.name} with BM25 score: $score")
}
```

---

## 5. Aggregations & Analytics

Execute high-speed in-memory aggregations across indexed datasets:

```kotlin
// Count total users
val totalUsers = users.count()

// Sum, Average, Min, Max
val totalBalance = users.sum("balance") { it.balance }
val avgAge = users.average("age") { it.age.toDouble() }
val maxAge = users.max("age") { it.age.toDouble() }
val minBalance = users.min("balance") { it.balance }
```

---

## 6. Time-To-Live (TTL) & Auto-Expiration

KoreDB allows configuring automatic expiration per document or globally across a collection:

```kotlin
// Insert with 60-second expiration
users.insertWithTtl("session_temp", tempUser, ttlSeconds = 60)

// Expired keys are automatically filtered on read and reclaimed during compaction
```
