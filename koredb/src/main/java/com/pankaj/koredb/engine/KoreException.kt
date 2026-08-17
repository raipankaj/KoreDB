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

package com.pankaj.koredb.engine

/**
 * Base exception class for all KoreDB database errors.
 */
sealed class KoreDBException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Thrown when data corruption is detected in SSTables, WAL, or indices.
 */
class CorruptionException(message: String, cause: Throwable? = null) : KoreDBException(message, cause)

/**
 * Thrown when LSM leveled compaction encounters an unrecoverable failure.
 */
class CompactionException(message: String, cause: Throwable? = null) : KoreDBException(message, cause)

/**
 * Thrown when a database transaction fails or encounters a conflict.
 */
class TransactionException(message: String, cause: Throwable? = null) : KoreDBException(message, cause)

/**
 * Thrown when backup creation or restore verification fails.
 */
class BackupRestoreException(message: String, cause: Throwable? = null) : KoreDBException(message, cause)
