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

package com.pankaj.koredb.db

import android.content.Context
import com.pankaj.koredb.compression.CompressionCodec
import com.pankaj.koredb.compression.NoOpCompressionCodec
import com.pankaj.koredb.crypto.AesGcmCrypto
import com.pankaj.koredb.crypto.KoreCrypto
import com.pankaj.koredb.log.AndroidLogcatLogger
import com.pankaj.koredb.log.KoreLogger
import java.io.File

/**
 * Android-specific entry point for KoreDB.
 *
 * This object provides convenience methods and a fluent builder to initialize
 * the database using Android's context, storage conventions, and Logcat logging.
 */
object KoreAndroid {

    init {
        // Automatically route KoreLogger logs to Android Logcat
        KoreLogger.factory = { tag -> AndroidLogcatLogger(tag) }
    }
    
    /**
     * Creates and initializes a [KoreDatabase] instance within the app's internal database directory.
     *
     * @param context The Android [Context].
     * @param dbName The name of the database folder. Defaults to "kore_default.db".
     * @return A thread-safe [KoreDatabase] instance.
     */
    fun create(context: Context, dbName: String = "kore_default.db"): KoreDatabase {
        return builder(context, dbName).build()
    }

    /**
     * Creates a fluent [Builder] to configure storage paths, encryption, and compression.
     */
    fun builder(context: Context, dbName: String = "kore_default.db"): Builder = Builder(context, dbName)

    /**
     * Creates a transient in-memory [KoreDatabase] backed by a temporary cache directory,
     * ideal for lightning-fast unit tests and Android instrumentation tests.
     * Automatically purges its files when [KoreDatabase.close] is invoked.
     */
    fun inMemory(context: Context): KoreDatabase {
        val tempDir = File(context.cacheDir, "koredb_mem_${java.util.UUID.randomUUID()}")
        tempDir.mkdirs()
        val db = KoreDatabase(tempDir)
        db.onCloseCallback = {
            tempDir.deleteRecursively()
        }
        return db
    }

    /**
     * Fluent configuration builder for Android [KoreDatabase] instances.
     */
    class Builder(private val context: Context, private val dbName: String) {
        private var crypto: KoreCrypto? = null
        private var compressionCodec: CompressionCodec = NoOpCompressionCodec
        private var storageLocation: StorageLocation = StorageLocation.DATABASE_DIR
        private var schemaVersion: Int = 1
        private var onMigrate: ((KoreDatabase, Int, Int) -> Unit)? = null
        private var enableMemoryTrimCallbacks: Boolean = true

        enum class StorageLocation {
            DATABASE_DIR,
            FILES_DIR,
            NO_BACKUP_DIR
        }

        fun withCrypto(crypto: KoreCrypto) = apply {
            this.crypto = crypto
        }

        fun withEncryption(secretKey: ByteArray) = apply {
            this.crypto = AesGcmCrypto(secretKey)
        }

        fun withCompression(codec: CompressionCodec) = apply {
            this.compressionCodec = codec
        }

        fun schemaVersion(version: Int, onMigrate: (db: KoreDatabase, oldVersion: Int, newVersion: Int) -> Unit) = apply {
            this.schemaVersion = version
            this.onMigrate = onMigrate
        }

        private var minFreeSpaceBytes: Long = 10 * 1024 * 1024L

        fun minFreeSpaceMb(mb: Long) = apply {
            this.minFreeSpaceBytes = mb * 1024 * 1024L
        }

        fun disableMemoryTrimCallbacks() = apply {
            this.enableMemoryTrimCallbacks = false
        }

        fun useDatabaseDirectory() = apply {
            this.storageLocation = StorageLocation.DATABASE_DIR
        }

        fun useFilesDirectory() = apply {
            this.storageLocation = StorageLocation.FILES_DIR
        }

        fun useNoBackupDirectory() = apply {
            this.storageLocation = StorageLocation.NO_BACKUP_DIR
        }

        fun build(): KoreDatabase {
            val dbDirectory = when (storageLocation) {
                StorageLocation.DATABASE_DIR -> context.getDatabasePath(dbName)
                StorageLocation.FILES_DIR -> File(context.filesDir, dbName)
                StorageLocation.NO_BACKUP_DIR -> File(context.noBackupFilesDir, dbName)
            }
            val db = KoreDatabase(
                directory = dbDirectory,
                crypto = crypto,
                compressionCodec = compressionCodec,
                targetSchemaVersion = schemaVersion,
                minFreeSpaceBytes = minFreeSpaceBytes,
                onMigrate = onMigrate
            )

            if (enableMemoryTrimCallbacks) {
                try {
                    val appCtx = context.applicationContext ?: context
                    @Suppress("DEPRECATION")
                    val callbacks = object : android.content.ComponentCallbacks2 {
                        override fun onTrimMemory(level: Int) {
                            when (level) {
                                android.content.ComponentCallbacks2.TRIM_MEMORY_RUNNING_CRITICAL,
                                android.content.ComponentCallbacks2.TRIM_MEMORY_RUNNING_LOW,
                                android.content.ComponentCallbacks2.TRIM_MEMORY_COMPLETE,
                                android.content.ComponentCallbacks2.TRIM_MEMORY_MODERATE -> {
                                    db.engine.blockCache.clear()
                                }
                                android.content.ComponentCallbacks2.TRIM_MEMORY_UI_HIDDEN -> {
                                    try {
                                        db.engine.flushHardware()
                                    } catch (_: Exception) {}
                                }
                            }
                        }

                        override fun onConfigurationChanged(newConfig: android.content.res.Configuration) {}

                        @Deprecated("Deprecated in Java", ReplaceWith("onTrimMemory(TRIM_MEMORY_COMPLETE)"))
                        override fun onLowMemory() {
                            db.engine.blockCache.clear()
                        }
                    }
                    appCtx.registerComponentCallbacks(callbacks)
                    val existingOnClose = db.onCloseCallback
                    db.onCloseCallback = {
                        existingOnClose?.invoke()
                        try {
                            appCtx.unregisterComponentCallbacks(callbacks)
                        } catch (_: Exception) {}
                    }
                } catch (e: Throwable) {
                    KoreLogger.getLogger("KoreAndroid").warn("Could not register ComponentCallbacks2: ${e.message}")
                }
            }

            return db
        }
    }

    /**
     * Completely deletes a KoreDB database from disk across all standard storage locations.
     */
    fun delete(context: Context, dbName: String = "kore_default.db") {
        listOf(
            context.getDatabasePath(dbName),
            File(context.filesDir, dbName),
            File(context.noBackupFilesDir, dbName)
        ).forEach { dir ->
            if (dir.exists()) {
                dir.deleteRecursively()
            }
        }
    }
}
