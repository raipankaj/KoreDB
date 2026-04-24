package com.pankaj.koredb

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.system.measureTimeMillis

@RunWith(AndroidJUnit4::class)
class RangeQueryBenchmark {
    private lateinit var app: MyApplication

    @Before
    fun setup() {
        app = ApplicationProvider.getApplicationContext<MyApplication>()
        runBlocking {
            app.database.collection("notes", Note.serializer()).deleteAll()
            app.roomDatabase.noteDao().deleteAll()
        }
    }

    private fun generateLargeString(sizeKb: Int): String {
        val sb = StringBuilder(sizeKb * 1024)
        val charPool = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        repeat(sizeKb * 1024) {
            sb.append(charPool[it % charPool.length])
        }
        return sb.toString()
    }

    @Test
    fun benchmarkComplexRangeScenarios() = runBlocking {
        // Reduced size to avoid OOM on devices with 256MB heap
        val SIZE_MEDIUM = 10_000
        val SIZE_LARGE = 2_000 
        val RANGE_SIZE = 500
        
        val collection = app.database.collection("notes_large", Note.serializer())
        val dao = app.roomDatabase.noteDao()

        val mediumContent = "This is a medium length content that simulates a typical note or a small call log entry."
        val largeContent = generateLargeString(50) // 50KB per record

        val scenarios = listOf(
            Triple("MEDIUM CONTENT (100 bytes)", mediumContent, SIZE_MEDIUM),
            Triple("LARGE CONTENT (50 KB)", largeContent, SIZE_LARGE)
        )

        for ((scenarioName, content, size) in scenarios) {
            println("\n📊 SCENARIO: $scenarioName")
            collection.deleteAll()
            dao.deleteAll()
            System.gc() // Suggest GC to clear previous scenario data

            val contactId = "user_456"
            
            println("⏳ Inserting $size records in chunks...")
            
            // Insert in chunks of 500 to keep memory low
            (1..size).chunked(500).forEach { chunk ->
                val chunkData = chunk.map {
                    Note(
                        id = "$contactId:${1000000 + it}", 
                        title = "Title $it", 
                        content = content
                    )
                }
                collection.insertBatch(chunkData.associateBy { it.id })
                dao.insertAll(chunkData)
            }

            val startId = "$contactId:${1000000 + 1000}"
            val endId = "$contactId:${1000000 + 1000 + RANGE_SIZE}"

            // Warm up
            collection.getByIdRange(startId, endId)
            dao.getByIdRange(startId, endId)

            val koreTime = measureTimeMillis {
                repeat(5) {
                    val results = collection.getByIdRange(startId, endId)
                    assert(results.size == RANGE_SIZE)
                }
            } / 5

            val roomTime = measureTimeMillis {
                repeat(5) {
                    val results = dao.getByIdRange(startId, endId)
                    assert(results.size == RANGE_SIZE)
                }
            } / 5 

            println("KoreDB Range Time: ${koreTime}ms")
            println("Room Range Time:   ${roomTime}ms")
            
            val diff = (roomTime).toFloat() / koreTime.toFloat()
            println("Result: KoreDB is ${String.format("%.2f", diff)}x faster than Room for $scenarioName")
        }
    }

    @Test
    fun testSparseIndexEfficiency() = runBlocking {
        val SIZE = 50_000
        val collection = app.database.collection("notes_sparse", Note.serializer())
        collection.deleteAll()

        println("\n🎯 Testing Sparse Index Efficiency (N=$SIZE)...")
        
        // Insert in chunks to avoid OOM
        (1..SIZE).chunked(5000).forEach { chunk ->
            collection.insertBatch(chunk.associate { 
                "key_${1000000 + it}" to Note("key_${1000000 + it}", "T$it", "C$it") 
            })
        }

        // Query records at the very end
        val startKey = "key_1049990"
        val endKey = "key_2000000" 

        val time = measureTimeMillis {
            val results = collection.getByIdRange(startKey, endKey)
            println("Found ${results.size} records at the end of $SIZE records")
        }
        
        println("Time to jump to the end: ${time}ms")
        assert(time < 150)
    }

    @Test
    fun testRangeQueryCorrectness() = runBlocking {
        val collection = app.database.collection("test_range_correct", Note.serializer())
        collection.deleteAll()

        val notes = mapOf(
            "a" to Note("a", "T1", "C1"),
            "b" to Note("b", "T2", "C2"),
            "c" to Note("c", "T3", "C3"),
            "d" to Note("d", "T4", "C4"),
            "e" to Note("e", "T5", "C5")
        )

        collection.insertBatch(notes)

        val results = collection.getByIdRange("b", "d")
        assert(results.size == 2)
        assert(results[0].id == "b")
        assert(results[1].id == "c")
        
        println("✅ Range Query Correctness Test Passed")
    }
}
