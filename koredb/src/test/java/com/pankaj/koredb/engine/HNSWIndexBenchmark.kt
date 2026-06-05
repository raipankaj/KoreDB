package com.pankaj.koredb.engine

import com.pankaj.koredb.hnsw.HNSWIndex
import com.pankaj.koredb.hnsw.MmapHNSWIndex
import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.system.measureTimeMillis

class HNSWIndexBenchmark {

    private lateinit var testDir: File

    @Before
    fun setup() {
        testDir = File("build/tmp/hnsw_bench_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    @Test
    fun runHNSWIndexBenchmark() = runBlocking {
        println("=== Starting HNSW Index Hydration & Memory Benchmark ===")

        val dimensions = 128
        val vectorCount = 5000
        val dummyVector = FloatArray(dimensions) { i -> i.toFloat() / dimensions }

        // 1. Initial Insertion Pass
        val initialDb = KoreDatabase(testDir)
        val collection = initialDb.vectorCollection("bench_vectors") {
            this.dimensions = dimensions
            this.quantization = false // Test with raw FloatArrays for maximum heap impact
        }

        println("Populating database with $vectorCount vectors of size $dimensions...")
        val populateTime = measureTimeMillis {
            val vectors = mutableMapOf<String, FloatArray>()
            for (i in 0 until vectorCount) {
                vectors["v_$i"] = dummyVector
            }
            collection.insertBatch(vectors)
            collection.waitForIndexing()
        }
        println("Population & indexing complete in $populateTime ms.")
        initialDb.close()

        val snapshotFile = File(testDir, "hnsw_bench_vectors.bin")
        if (!snapshotFile.exists()) {
            println("Error: Snapshot file was not created!")
            return@runBlocking
        }

        // Settle JVM memory
        System.gc()
        Thread.sleep(1000)

        // 2. Measure Memory-Mapped HNSW Hydration (Off-Heap)
        val heapBeforeMmap = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        var mmapHnsw: MmapHNSWIndex
        val mmapHydrationTime = measureTimeMillis {
            mmapHnsw = MmapHNSWIndex(snapshotFile)
        }
        val heapAfterMmap = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        val mmapHeapFootprint = (heapAfterMmap - heapBeforeMmap) / (1024 * 1024)

        println("\n[MEMORY-MAPPED HNSW (OFF-HEAP)]:")
        println(" - Isolated Hydration Time: $mmapHydrationTime ms")
        println(" - JVM Heap Memory Allocated: $mmapHeapFootprint MB")

        // Search Benchmark on Mapped HNSW
        val mmapSearchTime = measureTimeMillis {
            for (i in 0 until 1000) {
                mmapHnsw.search(dummyVector, limit = 5)
            }
        }
        val mmapSearchThroughput = (1000.0 / mmapSearchTime) * 1000.0
        println(" - Search Throughput: ${mmapSearchThroughput.toInt()} searches/sec")
        mmapHnsw.close()

        // Settle JVM memory
        System.gc()
        Thread.sleep(1000)

        // 3. Measure Standard In-Memory HNSW Hydration (On-Heap)
        val heapBeforeInMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        val inMemoryHnsw = HNSWIndex(maxNeighbors = 16, efConstruction = 200)
        val inMemoryHydrationTime = measureTimeMillis {
            inMemoryHnsw.loadFromDisk(snapshotFile)
        }
        val heapAfterInMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        val inMemoryHeapFootprint = (heapAfterInMemory - heapBeforeInMemory) / (1024 * 1024)

        println("\n[STANDARD IN-MEMORY HNSW (ON-HEAP)]:")
        println(" - Isolated Hydration Time: $inMemoryHydrationTime ms")
        println(" - JVM Heap Memory Allocated: $inMemoryHeapFootprint MB")

        // Search Benchmark on InMemory HNSW
        val inMemorySearchTime = measureTimeMillis {
            for (i in 0 until 1000) {
                inMemoryHnsw.search(dummyVector, limit = 5)
            }
        }
        val inMemorySearchThroughput = (1000.0 / inMemorySearchTime) * 1000.0
        println(" - Search Throughput: ${inMemorySearchThroughput.toInt()} searches/sec")

        println("\n=== HNSW Index Hydration Benchmark Completed ===")
    }
}
