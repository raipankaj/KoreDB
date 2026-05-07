# Document Collection Engine

The KoreDB Collection engine stores standard Kotlin Data Classes using Kotlinx Serialization. It is designed to be a direct, higher-performance replacement for Room or Realm.

## Core Concepts

- **LSM Storage**: Writes are strictly $O(1)$ appends.
- **Tombstones**: Deletions do not rewrite data; they append a tombstone marker, making deletes 8x faster than Room.
- **Caching**: Contains a 65K-entry Object Cache to bypass JSON deserialization on hot paths.

## Setup
```kotlin
@Serializable
data class Note(
    val id: String,
    val title: String,
    val content: String,
    val isPinned: Boolean = false
)

val notes = database.collection<Note>("notes")
```

## Basic CRUD

```kotlin
// Insert (Upsert)
notes.insert("n1", Note("n1", "Groceries", "Milk, Eggs"))

// High-throughput Batch Insert (Uses Nitro parallel serialization)
val batch = mapOf(
    "n2" to Note("n2", "Work", "Finish report"),
    "n3" to Note("n3", "Gym", "Leg day")
)
notes.insertBatch(batch)

// Retrieve
val note = notes.getById("n1")
val allNotes = notes.getAll()

// Range / Prefix (Very fast due to LSM MemTable)
val recent = notes.getByIdRange("n1", "n10")
val userNotes = notes.getByIdPrefix("user_123_")

// Delete
notes.delete("n1")
notes.deleteAll()
```

## Partial Updates

Update specific fields without rewriting the entire document from the application layer. This reads, modifies, and re-inserts the document atomically.

```kotlin
// Single
notes.updateFields("n2") { existingNote ->
    existingNote.copy(isPinned = true)
}

// Batch partial update
notes.updateFieldsBatch(listOf("n2", "n3")) {
    it.copy(isPinned = true)
}
```

## Secondary Indexes

You can create $O(\log N)$ indexes on any field. KoreDB uses a Reverse-Pointer architecture to handle index staleness automatically.

```kotlin
// 1. Create the index
notes.createIndex("isPinned") { it.isPinned.toString() }

// 2. Query the index
val pinnedNotes = notes.getByIndex("isPinned", "true")
```

## Query DSL & Aggregation

KoreDB includes a fluent query builder to filter, sort, limit, and aggregate data without needing SQL.

First, register the properties you want to query:
```kotlin
notes.registerProperty("isPinned") { it.isPinned.toString() }
notes.registerProperty("title") { it.title }
```

### Filtering & Sorting
```kotlin
val results = notes.query()
    .where("isPinned") { it == "true" }
    .sortBy("title", descending = true) { it }
    .limit(10)
    .offset(0)
    .execute()
```

### Aggregation
No need to map/reduce in memory; the database engine handles it.

```kotlin
val stats = products.query()
    .where("category") { it == "electronics" }
    .aggregate {
        count()
        sum("price") { it.toDouble() }
        avg("price") { it.toDouble() }
        max("price") { it.toDouble() }
        min("price") { it.toDouble() }
    }

println("Total items: ${stats.getCount()}")
println("Average price: ${stats.getAvg("price")}")
```

## Reactive Data (Flows)

Observe data changes in real-time to drive reactive UIs (e.g., Jetpack Compose).

```kotlin
// Observe a specific document
notes.observeById("n1").collect { note ->
    println("Note changed: $note")
}

// Observe the whole collection
notes.observeAll().collect { allNotes ->
    println("Collection updated. New size: ${allNotes.size}")
}
```
