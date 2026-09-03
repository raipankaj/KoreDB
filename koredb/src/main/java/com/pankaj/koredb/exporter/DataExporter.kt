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

package com.pankaj.koredb.exporter

import com.pankaj.koredb.core.KoreCollection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileReader
import java.io.FileWriter

/**
 * Supported data export formats.
 */
enum class ExportFormat {
    JSON,
    CSV
}

/**
 * Statistics returned after a data export or import operation.
 */
data class ExportStats(
    val totalRecords: Long,
    val totalBytes: Long,
    val executionTimeMs: Long
)

/**
 * Exports all documents in this collection to a JSON file.
 *
 * @param outputFile The destination file where the JSON array will be written.
 * @return [ExportStats] with record counts, byte size, and duration.
 */
suspend fun <T : Any> KoreCollection<T>.exportToJson(outputFile: File): ExportStats = withContext(Dispatchers.IO) {
    val startTime = System.currentTimeMillis()
    outputFile.parentFile?.mkdirs()
    var count = 0L

    outputFile.bufferedWriter(Charsets.UTF_8).use { writer ->
        writer.write("[\n")
        var isFirst = true
        for ((id, doc) in getAllWithIds()) {
            if (!isFirst) {
                writer.write(",\n")
            }
            val docJson = String(serializer.serialize(doc), Charsets.UTF_8)
            writer.write("  {\"_id\": \"${escapeJson(id)}\", \"_doc\": $docJson}")
            isFirst = false
            count++
        }
        writer.write("\n]\n")
    }

    val elapsed = System.currentTimeMillis() - startTime
    ExportStats(
        totalRecords = count,
        totalBytes = outputFile.length(),
        executionTimeMs = elapsed
    )
}

/**
 * Imports documents from a JSON file created by [exportToJson] into this collection.
 *
 * @param inputFile The source JSON file.
 * @param batchSize Batch size for database ingestion.
 * @return [ExportStats] detailing inserted records and elapsed time.
 */
suspend fun <T : Any> KoreCollection<T>.importFromJson(inputFile: File, batchSize: Int = 1000): ExportStats = withContext(Dispatchers.IO) {
    val startTime = System.currentTimeMillis()
    val content = inputFile.readText(Charsets.UTF_8)
    val parsedArray = Json.parseToJsonElement(content).jsonArray

    var importedCount = 0L
    val batch = mutableMapOf<String, T>()

    for (element in parsedArray) {
        val obj = element.jsonObject
        val id = obj["_id"]?.jsonPrimitive?.content ?: continue
        val docElement = obj["_doc"] ?: continue
        val docBytes = docElement.toString().toByteArray(Charsets.UTF_8)
        val doc = serializer.deserialize(docBytes)

        batch[id] = doc
        importedCount++

        if (batch.size >= batchSize) {
            insertBatch(batch)
            batch.clear()
        }
    }

    if (batch.isNotEmpty()) {
        insertBatch(batch)
        batch.clear()
    }

    val elapsed = System.currentTimeMillis() - startTime
    ExportStats(
        totalRecords = importedCount,
        totalBytes = inputFile.length(),
        executionTimeMs = elapsed
    )
}

/**
 * Exports collection records to a CSV file.
 *
 * @param outputFile The destination CSV file.
 * @param headers CSV header column names.
 * @param rowMapper Function mapping each document [T] to a list of column values.
 */
suspend fun <T : Any> KoreCollection<T>.exportToCsv(
    outputFile: File,
    headers: List<String>,
    rowMapper: (T) -> List<String>
): ExportStats = withContext(Dispatchers.IO) {
    val startTime = System.currentTimeMillis()
    outputFile.parentFile?.mkdirs()

    val allDocs = getAllWithIds()
    var count = 0L

    outputFile.bufferedWriter(Charsets.UTF_8).use { writer ->
        // Write header
        writer.write(headers.joinToString(",") { escapeCsv(it) })
        writer.newLine()

        for ((id, doc) in allDocs) {
            val values = mutableListOf(id)
            values.addAll(rowMapper(doc))
            writer.write(values.joinToString(",") { escapeCsv(it) })
            writer.newLine()
            count++
        }
    }

    val elapsed = System.currentTimeMillis() - startTime
    ExportStats(
        totalRecords = count,
        totalBytes = outputFile.length(),
        executionTimeMs = elapsed
    )
}

/**
 * Imports records from a CSV file into this collection.
 *
 * @param inputFile The source CSV file.
 * @param hasHeader Whether the first line is a header row (defaults to true).
 * @param rowParser Maps a list of CSV cell tokens to a Pair of (ID, Document).
 */
suspend fun <T : Any> KoreCollection<T>.importFromCsv(
    inputFile: File,
    hasHeader: Boolean = true,
    rowParser: (List<String>) -> Pair<String, T>
): ExportStats = withContext(Dispatchers.IO) {
    val startTime = System.currentTimeMillis()
    var count = 0L
    val batch = mutableMapOf<String, T>()

    inputFile.bufferedReader(Charsets.UTF_8).use { reader ->
        var lineIndex = 0
        var line: String? = reader.readLine()
        while (line != null) {
            if (lineIndex == 0 && hasHeader) {
                lineIndex++
                line = reader.readLine()
                continue
            }

            val tokens = parseCsvLine(line)
            if (tokens.isNotEmpty()) {
                val (id, doc) = rowParser(tokens)
                batch[id] = doc
                count++

                if (batch.size >= 1000) {
                    insertBatch(batch)
                    batch.clear()
                }
            }
            lineIndex++
            line = reader.readLine()
        }
    }

    if (batch.isNotEmpty()) {
        insertBatch(batch)
        batch.clear()
    }

    val elapsed = System.currentTimeMillis() - startTime
    ExportStats(
        totalRecords = count,
        totalBytes = inputFile.length(),
        executionTimeMs = elapsed
    )
}

private fun escapeJson(value: String): String {
    return value.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
}

private fun escapeCsv(value: String): String {
    return if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
        "\"" + value.replace("\"", "\"\"") + "\""
    } else {
        value
    }
}

private fun parseCsvLine(line: String): List<String> {
    val tokens = mutableListOf<String>()
    val sb = StringBuilder()
    var inQuotes = false
    var i = 0

    while (i < line.length) {
        val c = line[i]
        if (c == '\"') {
            if (inQuotes && i + 1 < line.length && line[i + 1] == '\"') {
                sb.append('\"')
                i++
            } else {
                inQuotes = !inQuotes
            }
        } else if (c == ',' && !inQuotes) {
            tokens.add(sb.toString())
            sb.clear()
        } else {
            sb.append(c)
        }
        i++
    }
    tokens.add(sb.toString())
    return tokens
}
