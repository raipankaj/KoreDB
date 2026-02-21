# KoreDB 🚀

**The AI-Native, High-Performance NoSQL Database for Modern Android.**

KoreDB is a pure Kotlin, embedded database engine built from the ground up using a **Log-Structured Merge-tree (LSM)** architecture. Unlike SQLite (designed for spinning disks in 2000), KoreDB is optimized for modern flash storage, high-concurrency Coroutines, and on-device AI applications.

---

## ✨ Features

*   **⚡ Blazing Performance:** LSM architecture offers $O(1)$ write performance.
*   **🤖 AI-Native Vector Store:** Built-in high-performance vector similarity search using Cosine Similarity, optimized with SIMD-like loop unrolling.
*   **🏗️ Pure Kotlin:** 100% Kotlin with Zero JNI overhead. No more `sqlite3.so` bloat.
*   **🔗 Coroutine First:** Built for non-blocking I/O and reactive UI with `Flow`.
*   **🛡️ Crash Resilient:** Write-Ahead Logging (WAL) ensures your data survives process death or system crashes.
*   **🔍 Optimized Reads:** Bloom Filters and Sparse Indexing ensure fast lookups by avoiding unnecessary disk I/O.
*   **📦 Lightweight:** Minimal footprint, perfect for mobile apps.

---

## 🚀 Quick Start

### 1. Installation

Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.raipankaj:koredb:0.0.1")
    // Note: Ensure you have kotlinx-serialization plugin applied
}
```

### 2. Initialize Database

Initialize KoreDB once in your `Application` class:

```kotlin
class MyApplication : Application() {
    lateinit var database: KoreDatabase

    override fun onCreate() {
        super.onCreate()
        // Initializes the engine in the app's secure internal storage
        database = KoreAndroid.create(this, "my_notes_db")
    }
}
```

### 3. Basic CRUD Operations

Define your data model using `@Serializable`:

```kotlin
@Serializable
data class Note(val id: String, val title: String, val content: String)

// Get a collection
val notesCollection = database.collection<Note>("notes")

// Insert or Update
runBlocking {
    notesCollection.insert("1", Note("1", "Hello KoreDB", "This is so fast!"))
}

// Get by ID
val note = notesCollection.getById("1")

// Observe changes reactively
notesCollection.observeById("1").collect { updatedNote ->
    println("Note updated: $updatedNote")
}
```

### 4. 🤖 AI Vector Similarity Search

Perfect for RAG, image search, or recommendation engines:

```kotlin
val vectorStore = database.vectorCollection("embeddings")

runBlocking {
    // Store vectors (e.g., from a machine learning model)
    vectorStore.insert("doc_1", floatArrayOf(0.1f, 0.5f, 0.9f))
    
    // Search for top 5 similar items
    val results = vectorStore.search(queryVector = floatArrayOf(0.11f, 0.49f, 0.88f), limit = 5)
    
    results.forEach { (id, score) ->
        println("Found $id with similarity score: $score")
    }
}
```

---

## 🛠️ Architecture

KoreDB follows the classic LSM-Tree pattern used by Bigtable, Cassandra, and RocksDB:
1.  **MemTable:** In-memory sorted buffer (ConcurrentSkipListMap).
2.  **Write-Ahead Log (WAL):** Sequential log for durability.
3.  **SSTables:** Immutable disk files created when MemTable is flushed.
4.  **Compaction:** Background merging of SSTables to reclaim space and maintain read speed.
5.  **Bloom Filter:** Per-SSTable probabilistic filter to eliminate 99% of unnecessary disk reads.

---

## 📊 KoreDB vs SQLite

| Operation | KoreDB (LSM) | Room (B-Tree) | Winner |
| :--- | :--- | :--- | :--- |
| **Bulk Write** | ⚡ Fast (Appends) | 🐢 Slower (Page Updates) | **KoreDB** |
| **Vector Search** | 🔥 Ultra Fast (Zero-copy) | 🐌 Slow (Blob Fetching) | **KoreDB** |
| **Cold Start** | 🚀 Immediate | ⏳ Slower (Schema Init) | **KoreDB** |
| **Concurrency** | 🟢 0ms Read Latency | 🟡 Lock Contention | **KoreDB** |

---

## 📜 License

```text
Copyright 2024 KoreDB Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

## 🤝 Contributing

We welcome contributions! Please feel free to submit a Pull Request or open an issue on our [GitHub repository](https://github.com/pankajrai/KoreDB).

Made with ❤️ by Pankaj Rai.
