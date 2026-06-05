package com.pankaj.koredb.engine

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.graph.Node
import com.pankaj.koredb.graph.Edge
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.launch
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.system.measureTimeMillis

class CollectionVectorGraphBenchmark {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Serializable
    data class User(val id: String, val email: String, val age: Int)

    @Before
    fun setup() {
        testDir = File("build/tmp/bench_cvg_${UUID.randomUUID()}")
        testDir.mkdirs()
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun runConcurrentCVGBenchmark() = runBlocking {
        println("=== Starting Concurrent Document/Vector/Graph Write Benchmark ===")

        val concurrency = 8
        val opsPerThread = 5000
        val totalOps = concurrency * opsPerThread

        // 1. Benchmark Document Collection writes
        val users = db.collection<User>("users")
        val collectionWriteTime = measureTimeMillis {
            val jobs = List(concurrency) { threadId ->
                launch(Dispatchers.Default) {
                    for (i in 0 until opsPerThread) {
                        val id = "user_${threadId}_$i"
                        users.insert(id, User(id, "user_$id@example.com", i % 100))
                    }
                }
            }
            jobs.joinAll()
        }
        val collectionThroughput = (totalOps.toDouble() / collectionWriteTime) * 1000.0
        println("[COLLECTION] Concurrent Writes of $totalOps documents: $collectionWriteTime ms (${collectionThroughput.toInt()} ops/sec)")

        // 2. Benchmark Vector Collection writes
        val vectors = db.vectorCollection("vectors") {
            dimensions = 128
            quantization = false
        }
        val dummyVector = FloatArray(128) { 0.5f }
        val vectorWriteTime = measureTimeMillis {
            val jobs = List(concurrency) { threadId ->
                launch(Dispatchers.Default) {
                    for (i in 0 until opsPerThread) {
                        val id = "vec_${threadId}_$i"
                        vectors.insert(id, dummyVector)
                    }
                }
            }
            jobs.joinAll()
            // Wait for background HNSW indexing threads to complete
            vectors.waitForIndexing()
        }
        val vectorThroughput = (totalOps.toDouble() / vectorWriteTime) * 1000.0
        println("[VECTORS] Concurrent Writes of $totalOps vectors (including HNSW): $vectorWriteTime ms (${vectorThroughput.toInt()} ops/sec)")

        // 3. Benchmark Graph writes (Nodes & Edges)
        val graph = db.graph()
        val graphWriteTime = measureTimeMillis {
            val jobs = List(concurrency) { threadId ->
                launch(Dispatchers.Default) {
                    for (i in 0 until opsPerThread) {
                        val nodeId = "node_${threadId}_$i"
                        // Insert node
                        graph.putNode(Node(nodeId))
                        // Insert edge to a companion node
                        if (i > 0) {
                            graph.putEdge(Edge(nodeId, "node_${threadId}_${i - 1}", "FOLLOWS"))
                        }
                    }
                }
            }
            jobs.joinAll()
        }
        val graphThroughput = (totalOps.toDouble() / graphWriteTime) * 1000.0
        println("[GRAPH] Concurrent Node/Edge inserts of $totalOps nodes/edges: $graphWriteTime ms (${graphThroughput.toInt()} ops/sec)")

        println("=== Concurrent Document/Vector/Graph Write Benchmark Completed ===")
    }
}
