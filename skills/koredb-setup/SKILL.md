---
name: koredb-setup
description: Setup, configure, and initialize KoreDB in Android and pure Kotlin projects. Use when adding dependencies, configuring Dependency Injection (Hilt/Koin), setting up encryption, compression, schema versioning, or handling Android memory trimming and storage locations.
---

# KoreDB Setup & Configuration Guide

KoreDB is an embedded, zero-dependency, pure-Kotlin multi-model database engine (Documents, Vectors, Graphs) built on an ultra-fast Log-Structured Merge-tree (LSM) foundation.

## 1. Gradle Dependencies

Add KoreDB to the app/module `build.gradle.kts`:

```kotlin
plugins {
    alias(libs.plugins.android.application) // or kotlin("jvm")
    alias(libs.plugins.kotlin.serialization) // required for @Serializable models
}

dependencies {
    // KoreDB Core Engine
    implementation("io.github.raipankaj:koredb:0.2.0")

    // Kotlinx Serialization & Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-cbor:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
}
```

---

## 2. Initializing KoreDatabase in Android

Always use `KoreAndroid.builder` to ensure proper internal storage resolution, Logcat routing, and OS memory trimming:

### Standard Production Setup
```kotlin
import android.content.Context
import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.db.KoreAndroid
import com.pankaj.koredb.db.KoreDatabase

fun provideDatabase(context: Context): KoreDatabase {
    return KoreAndroid.builder(context, "app_data.db")
        .useDatabaseDirectory()                 // Stores in /data/user/0/<pkg>/databases/
        .withCompression(Lz4CompressionCodec()) // High-speed LZ4 block compression
        .minFreeSpaceMb(30)                     // Prevents disk exhaustion crashes
        .schemaVersion(1) { db, oldVersion, newVersion ->
            // Migration hook executed automatically when version increases
        }
        .build()
}
```

### Storage Directory Selection
* `.useDatabaseDirectory()` (**Default & Recommended**): Resolves to `context.getDatabasePath(name)`. Visible in Android Studio Device File Explorer.
* `.useNoBackupDirectory()`: Resolves to `context.noBackupFilesDir`. Ideal for large vector indices or local AI caches that should **never** sync to Google Drive Auto-Backup.
* `.useFilesDirectory()`: Resolves to `context.filesDir`.

### Fast In-Memory Database (for Unit / Instrumentation Tests)
```kotlin
// Instantly creates an isolated database in cacheDir and deletes all files on db.close()
val testDb = KoreAndroid.inMemory(context)
```

---

## 3. Dependency Injection (Hilt & Koin)

### Hilt Setup
```kotlin
import android.content.Context
import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.db.KoreAndroid
import com.pankaj.koredb.db.KoreDatabase
import dagger.Module
import dagger.Provides
import dagger.hilt.InstallIn
import dagger.hilt.android.qualifiers.ApplicationContext
import dagger.hilt.components.SingletonComponent
import javax.inject.Singleton

@Module
@InstallIn(SingletonComponent::class)
object DatabaseModule {

    @Provides
    @Singleton
    fun provideKoreDatabase(@ApplicationContext context: Context): KoreDatabase {
        return KoreAndroid.builder(context, "main_app.db")
            .useDatabaseDirectory()
            .withCompression(Lz4CompressionCodec())
            .minFreeSpaceMb(50)
            .build()
    }
}
```

### Koin Setup
```kotlin
import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.db.KoreAndroid
import org.koin.dsl.module

val databaseModule = module {
    single {
        KoreAndroid.builder(get(), "main_app.db")
            .useDatabaseDirectory()
            .withCompression(Lz4CompressionCodec())
            .build()
    }
}
```

---

## 4. Security & Encryption (AES-GCM-256 with AAD)

To encrypt all stored document payloads and vectors on disk at rest using hardware-backed keys:

```kotlin
import com.pankaj.koredb.crypto.AesGcmCrypto

// Generate or retrieve a 256-bit (32 bytes) key from Android Keystore
val masterKeyBytes: ByteArray = getOrCreateKeystoreKey()

val secureDb = KoreAndroid.builder(context, "secure_vault.db")
    .withCrypto(AesGcmCrypto(masterKeyBytes))
    .build()
```
* Every payload is encrypted with AES/GCM/NoPadding.
* Uses the record key as Additional Authenticated Data (AAD) to prevent ciphertext swapping attacks.

---

## 5. Pure Kotlin & JVM Applications (Non-Android)

For desktop (JVM), server, or CLI Kotlin projects:

```kotlin
import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.db.KoreDatabase
import java.io.File

val db = KoreDatabase(
    directory = File(System.getProperty("user.home"), ".myapp/data"),
    compressionCodec = Lz4CompressionCodec(),
    minFreeSpaceBytes = 100 * 1024 * 1024L // 100 MB
)
```

---

## 6. ProGuard / R8 Configuration

KoreDB includes its own `consumer-rules.pro` automatically. In your app's `proguard-rules.pro`, simply retain your data models:

```proguard
-keepattributes *Annotation*,Signature,InnerClasses,EnclosingMethod

# Keep your @Serializable application models
-keepclassmembers class com.yourapp.models.** {
    *** Companion;
}
-keep class com.yourapp.models.** { *; }
```

---

## 7. Critical Architecture Rules for Agents

1. **Singleton Pattern**: Maintain a single `KoreDatabase` instance per database file across your application.
2. **Thread Safety**: `KoreDatabase` is 100% thread-safe. You do not need external mutexes when reading or writing from multiple coroutines.
3. **Single Process Only**: KoreDB acquires an OS file lock (`kore.lock`). Do NOT open the same database directory from two different Android OS processes (e.g. an isolated `:remote` service). Use standard Android IPC (Binder/ContentProvider) to communicate with the main process.
4. **Graceful Shutdown**: Call `db.close()` when shutting down or tearing down tests to flush in-memory MemTables to SSTables, guaranteeing instant startup on next launch.
