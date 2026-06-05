package com.pankaj.koredb.engine

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.launch
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.Dispatchers
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.system.measureTimeMillis

class CompactionBenchmark {

    private lateinit var testDir: File
    private lateinit var db: KoreDB

    @Before
    fun setup() {
        testDir = File("build/tmp/benchmark_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDB(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun runBenchmark() = runBlocking {
        println("=== Starting Compaction Benchmark ===")

        val prefixes = listOf("c:users:", "c:products:", "g:idx:nodes:", "idx:users:age:")
        val numberOfWrites = 200000
        val valueSize = 512 // 512 bytes value to fill memory quickly and trigger flushes
        val dummyValue = ByteArray(valueSize) { 0x01.toByte() }

        // Generate data upfront to avoid measuring generation time
        val testData = List(numberOfWrites) { i ->
            val prefix = prefixes[i % prefixes.size]
            val key = "$prefix${UUID.randomUUID()}".toByteArray(Charsets.UTF_8)
            key to dummyValue
        }

        // 1. Write Benchmark
        println("Performing $numberOfWrites writes...")
        val writeTime = measureTimeMillis {
            // Write in batches of 100 to simulate typical batch usage
            testData.chunked(100).forEach { batch ->
                db.writeBatchRaw(batch)
            }
        }
        val writeThroughput = (numberOfWrites.toDouble() / writeTime) * 1000.0
        println("Write completed in $writeTime ms (${writeThroughput.toInt()} ops/sec)")

        // Wait for background compaction and flushes to complete
        while (db.isCompacting) {
            kotlinx.coroutines.delay(50)
        }
        kotlinx.coroutines.delay(500) // Small safety window for final disk writes to settle

        // Check file counts
        val files = testDir.listFiles() ?: emptyArray()
        val sstFiles = files.filter { it.name.endsWith(".sst") }
        println("Total SST files on disk: ${sstFiles.size}")
        sstFiles.forEach { file ->
            println(" - ${file.name}: ${file.length() / 1024} KB")
        }

        // 2. Read Benchmark (Point Lookups)
        println("Performing 10000 point lookups...")
        // Lookup keys that exist
        val readTime = measureTimeMillis {
            for (i in 0 until 10000) {
                val key = testData[i % testData.size].first
                db.getRaw(key)
            }
        }
        val readThroughput = (10000.0 / readTime) * 1000.0
        println("Read completed in $readTime ms (${readThroughput.toInt()} ops/sec)")

        println("=== Benchmark Completed ===")
    }

    @Test
    fun runConcurrentWriteBenchmark() = runBlocking {
        println("=== Starting Concurrent Write Benchmark ===")

        val concurrency = 8
        val writesPerThread = 25000
        val totalWrites = concurrency * writesPerThread
        val valueSize = 512
        val dummyValue = ByteArray(valueSize) { 0x01.toByte() }

        val testData = List(totalWrites) { i ->
            val key = "c:users:${UUID.randomUUID()}".toByteArray(Charsets.UTF_8)
            key to dummyValue
        }

        println("Performing $totalWrites writes across $concurrency threads...")
        val writeTime = measureTimeMillis {
            val jobs = List(concurrency) { threadId ->
                launch(Dispatchers.Default) {
                    val startIndex = threadId * writesPerThread
                    val endIndex = (threadId + 1) * writesPerThread
                    val threadData = testData.subList(startIndex, endIndex)
                    
                    threadData.chunked(100).forEach { batch ->
                        db.writeBatchRaw(batch)
                    }
                }
            }
            jobs.joinAll()
        }
        val writeThroughput = (totalWrites.toDouble() / writeTime) * 1000.0
        println("Concurrent Write completed in $writeTime ms (${writeThroughput.toInt()} ops/sec)")
        println("=== Concurrent Benchmark Completed ===")
    }
}
