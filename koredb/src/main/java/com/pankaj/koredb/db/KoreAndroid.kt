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
import java.io.File

/**
 * Android-specific entry point for KoreDB.
 *
 * This object provides convenience methods to initialize the database using
 * Android's context and file system conventions.
 */
object KoreAndroid {
    
    /**
     * Creates and initializes a [KoreDatabase] instance within the app's internal storage.
     *
     * By default, this uses the app's secure `files` directory, ensuring the database
     * is not accessible by other applications and does not require special permissions.
     *
     * @param context The Android [Context] used to locate the internal files directory.
     * @param dbName The name of the database folder. Defaults to "kore_default.db".
     * @return A thread-safe [KoreDatabase] instance.
     */
    fun create(context: Context, dbName: String = "kore_default.db"): KoreDatabase {
        val dbDirectory = File(context.filesDir, dbName)
        return KoreDatabase(dbDirectory)
    }
}
