# KoreDB Production Readiness & Deployment Checklist

Before rolling out **KoreDB** into production Android applications, review this verification checklist to ensure maximum stability, security, and performance.

---

## 1. Production Architecture Checklist

- [x] **Thread-Safety**: `KoreDatabase`, `KoreCollection`, `KoreVectorCollection`, and `KoreGraph` are completely thread-safe and can be accessed concurrently across any number of coroutines or threads.
- [x] **Direct Commit Pipeline**: Single writes and transaction commits execute directly against the MemTable and WAL on the calling thread without queue or coroutine context switching.
- [x] **Off-Heap Memory Safety**: HNSW index vectors and SSTable blocks use bounded direct buffers and memory-mapped files (`MappedByteBuffer`), avoiding Java GC pressure.
- [x] **WAL Crash Recovery**: Every transaction is committed with a 32-bit CRC checksum. On ungraceful termination (crash, power loss, OOM kill), KoreDB detects truncated tails and replays valid commits automatically.
- [x] **Integrity Verification**: Built-in `db.engine.verifyIntegrity()` checks file checksums and SSTable manifests on startup or background diagnostics.

---

## 2. Android Configuration & Storage

### Storage Directory Selection
Always initialize KoreDB using `KoreAndroid.builder` to ensure proper internal storage isolation:

```kotlin
val db = KoreAndroid.builder(context, "production.db")
    .useDatabaseDirectory() // Recommended: /data/user/0/<pkg>/databases/
    .minFreeSpaceMb(50)     // Prevent writes if disk has < 50MB free
    .build()
```

| Location Method | Path | Use Case |
| :--- | :--- | :--- |
| `useDatabaseDirectory()` (Default) | `context.getDatabasePath("app.db")` | Standard application data. Visible in Android Studio Database Inspector / Device File Explorer. |
| `useFilesDirectory()` | `File(context.filesDir, "app.db")` | Legacy file storage. |
| `useNoBackupDirectory()` | `File(context.noBackupFilesDir, "app.db")` | Large caches, vector indices, or temporary offline sync replicas that should **never** be uploaded to Google Drive Auto Backup. |

---

## 3. ProGuard & R8 Optimization

KoreDB includes consumer ProGuard rules automatically in its AAR packaging via `consumer-rules.pro`. When building your release APK/AAB with `isMinifyEnabled = true`, ensure your custom model classes with `@Serializable` are retained:

```proguard
# In your app/proguard-rules.pro
-keepattributes *Annotation*,Signature,InnerClasses,EnclosingMethod

# Keep your data models
-keepclassmembers class com.yourapp.models.** {
    *** Companion;
}
-keep class com.yourapp.models.** {
    *;
}
```

---

## 4. Android Auto-Backup Configuration

By default, Android Auto Backup may attempt to backup your entire database directory to the user's Google Drive (capped at 25MB). If your database stores large vector embeddings or media-heavy graphs, exclude it in `res/xml/backup_rules.xml`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<data-extraction-rules>
    <cloud-backup>
        <!-- Exclude large vector or graph databases from cloud backup -->
        <exclude domain="database" path="embeddings.db" />
        <include domain="database" path="user_profile.db" />
    </cloud-backup>
    <device-transfer>
        <include domain="database" path="." />
    </device-transfer>
</data-extraction-rules>
```

In your `AndroidManifest.xml`:
```xml
<application
    android:allowBackup="true"
    android:dataExtractionRules="@xml/backup_rules"
    android:fullBackupContent="@xml/backup_rules">
    ...
</application>
```

---

## 5. Low Memory & Lifecycle Management

KoreDB automatically hooks into Android's `ComponentCallbacks2` when built via `KoreAndroid.builder`:
- **`TRIM_MEMORY_RUNNING_CRITICAL` / `TRIM_MEMORY_COMPLETE`**: Automatically evicts cached SSTable data blocks from the `BlockCache` while keeping open file channels intact.
- **`TRIM_MEMORY_UI_HIDDEN`**: Flushes dirty WAL buffers to the kernel page cache when the user backgrounds the app.

To release file descriptors manually (e.g. on user logout):
```kotlin
db.close() // Gracefully flushes active MemTable to SSTable and releases locks
```

---

## 6. Multi-Process Limitations

> **Important**: KoreDB is designed as an **embedded single-process** database engine.
> Like SQLite in WAL mode or RocksDB, multiple threads within the **same OS process** can read and write concurrently with complete ACID guarantees.
> However, opening the **same database directory** simultaneously from **two separate Android OS processes** (such as an independent `:remote` Service and the main Activity process) will result in a `DatabaseLockedException` because KoreDB places an exclusive file lock (`kore.lock`) on startup.
>
> If your application architecture uses multiple processes, interact with KoreDB in the main process and communicate via Android `Binder` / `ContentProvider` / AIDL.

---

## 7. Runtime Observability & Health Monitoring

Monitor database performance and storage health in production:

```kotlin
val stats = db.engine.getStats()
Log.i("KoreDB", """
    Writes: ${stats.totalWrites}
    Reads : ${stats.totalReads}
    MemTable: ${stats.memTableBytes / 1024} KB
    SSTables: ${stats.sstableCount} (Level 0: ${stats.level0Count})
    Cache Hit Rate: ${String.format("%.1f", stats.cacheHitRate * 100)}%
""".trimIndent())

// Run integrity check periodically or after unexpected crashes
val report = db.engine.verifyIntegrity()
if (!report.isHealthy) {
    Log.e("KoreDB", "Database issues detected: ${report.issues.joinToString()}")
}
```

---

## 8. Summary: Is KoreDB Production-Ready?

| Evaluation Criterion | Status | Notes |
| :--- | :---: | :--- |
| **ACID MVCC Transactions** | ✅ PASSED | First-committer-wins validation, snapshot isolation |
| **Crash Durability** | ✅ PASSED | WAL CRC32 verification, automatic corrupt tail discard |
| **Performance vs SQLite** | ✅ PASSED | Up to 1,484x faster point reads, 25x faster writes on Pixel 7 Pro |
| **Hardware Verification** | ✅ PASSED | 64/64 integration & benchmark tests passed on physical Pixel 7 Pro |
| **Memory Management** | ✅ PASSED | Streaming direct buffers, ComponentCallbacks2 memory trimming |
| **Enterprise Features** | ✅ PASSED | AES-GCM-256 encryption, LZ4 compression, CDC change streams |
| **Android Tooling** | ✅ PASSED | Fluent builder, ProGuard rules, CBOR binary serializer |

**Conclusion**: **Yes, KoreDB is production-ready** for Android applications seeking high performance, embedded vector search, property graphs, and unified multi-model data storage.
