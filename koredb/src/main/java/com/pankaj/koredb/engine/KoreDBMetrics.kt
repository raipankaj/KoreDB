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

import kotlinx.serialization.Serializable

/**
 * Storage and operational metrics snapshot for KoreDB.
 */
@Serializable
data class KoreDBMetrics(
    val readCount: Long,
    val writeCount: Long,
    val compactionCount: Long,
    val activeSSTables: Int,
    val memTableEntries: Int,
    val memTableSizeBytes: Long,
    val totalDiskUsageBytes: Long
)
