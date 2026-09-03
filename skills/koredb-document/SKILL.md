---
name: koredb-document
description: Create, query, index, and manage typed document collections in KoreDB. Use when modeling entities with @Serializable, performing CRUD, building queries with the Query DSL, configuring secondary and numeric range indexes, setting up Okapi BM25 full-text search, managing TTL, or observing changes via Kotlin Flow.
---

# KoreDB Document Engine Guide

KoreDB stores structured documents as zero-copy binary CBOR records within a typed collection. It supports secondary indexing, order-preserving numeric range scans, BM25 text search, and reactive Flow emissions.

---

## 1. Entity Modeling

Declare entities as standard Kotlin `@Serializable` data classes. No database annotations (`@Entity`, `@ColumnInfo`, `@PrimaryKey`) are needed:

```kotlin
import kotlinx.serialization.Serializable

@Serializable
data class Note(
    val id: String,
    val title: String,
    val category: String,
    val priority: Int,
    val price: Double = 0.0,
    val tags: List<String> = emptyList(),
    val createdAt: Long = System.currentTimeMillis()
)
```

---

## 2. Collection Creation & Indexing

Always register collections and configure indices during repository or database initialization:

```kotlin
val notes = db.collection("notes", Note.serializer()) { it.id }

// 1. String Secondary Index (for O(log N) equality searches)
notes.createIndex("category") { it.category }

// 2. Numeric Range Index (for order-preserving byte range pushdown)
notes.createNumericIndex("price") { it.price }
notes.createNumericIndex("priority") { it.priority.toDouble() }

// 3. Okapi BM25 Full-Text Search Index
notes.createSearchIndex { "${it.title} ${it.tags.joinToString(" ")}" }
```

> **Performance Note**: Indices are maintained atomically using dual-write LSM records (`idx:col:field:val:id`). Stale index entries are automatically purged during background compaction via the Truth Oracle.

---

## 3. CRUD Operations

### Ingestion & Batch Writes
```kotlin
val note = Note("n1", "Groceries", "personal", priority = 1, price = 25.50)

// Single write (~100 microseconds, direct commit)
notes.insert(note.id, note)

// Bulk Ingest (1.5x - 25x faster than Room)
val noteList: List<Note> = fetchNotesFromNetwork()
notes.insertAll(noteList) // Auto-extracts IDs via idExtractor

// Map-based Batch
notes.insertBatch(mapOf("n1" to note1, "n2" to note2))
```

### Point Reads & Checks
```kotlin
// Direct O(1) read from MemTable or Mmap SSTable
val item: Note? = notes.getById("n1")

// Check existence without loading payload
val exists: Boolean = notes.exists("n1")

// Total count (accounting for expired TTLs)
val total: Int = notes.count()
```

### Deletions
```kotlin
// Delete single note (appends tombstone and clears indices)
notes.delete("n1")

// Delete multiple
notes.deleteBatch(listOf("n1", "n2"))

// Clear entire collection
notes.deleteAll()
```

---

## 4. Query DSL & Range Scans

Use the type-safe Query DSL to filter, sort, and paginate:

### Multi-Predicate Query
```kotlin
val results: List<Note> = notes.query {
    where("category", eq("personal"))
    where("price", lte(100.0))
    where("priority", inList(listOf(1, 2)))
    sortBy("price", ascending = true)
    limit(20)
    offset(0)
}
```

### Supported Operators
| Operator | Function | Example |
| :--- | :--- | :--- |
| `eq(value)` | Exact match | `where("status", eq("active"))` |
| `neq(value)` | Not equal | `where("status", neq("archived"))` |
| `gt(val)` / `gte(val)` | Greater than / or equal | `where("price", gte(50.0))` |
| `lt(val)` / `lte(val)` | Less than / or equal | `where("price", lt(200.0))` |
| `between(min, max)` | Range inclusive | `where("price", between(10.0, 99.0))` |
| `inList(list)` | Set inclusion | `where("category", inList(listOf("a", "b")))` |
| `contains(sub)` | String substring | `where("title", contains("meeting"))` |

### Fast Direct Index Lookups
When querying a single indexed property, use direct helper methods to bypass query planner overhead:

```kotlin
// Direct secondary index seek
val personalNotes: List<Note> = notes.find("category", "personal")

// Order-preserving LSM byte range scan (6.1x faster than SQLite)
val affordable: List<Note> = notes.findRange("price", 10.0, 50.0)

// Prefix scan on primary key
val userItems: List<Note> = notes.getByIdPrefix("user_100_")
```

---

## 5. Okapi BM25 Full-Text Keyword Search

KoreDB includes a pure-Kotlin BM25 inverted index engine:

```kotlin
// Execute BM25 search with Robertson-Spärck Jones IDF scoring
val searchResults: List<Pair<String, Float>> = notes.search("organic groceries", limit = 10)

// Load top matching documents
val topNotes: List<Note> = searchResults.mapNotNull { (id, score) ->
    notes.getById(id)
}
```

---

## 6. Time-To-Live (TTL) Auto-Expiration

KoreDB supports per-record TTL for session tokens, caches, and ephemeral messages:

```kotlin
// Store token that automatically expires in 1 hour (3600 seconds)
notes.insert("session_token", authData, ttlMs = 3600_000L)

// Expired records return null on getById() and are purged during compaction
val active = notes.getById("session_token") // Returns null if TTL elapsed
```

---

## 7. Reactive Flows & Jetpack Compose Integration

Observe collection updates in ViewModels:

```kotlin
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.*

class NotesViewModel(private val notes: KoreCollection<Note>) : ViewModel() {

    val notesState: StateFlow<List<Note>> = notes.observe()
        .map { notes.getAll() }
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )
}
```

---

## 8. Built-in Aggregations

Compute statistics without writing SQL:

```kotlin
val totalNotes = notes.count()
val totalPrice = notes.sum { it.price }
val averagePrice = notes.avg { it.price }
val maxPrice = notes.max { it.price }
val minPrice = notes.min { it.price }
```
