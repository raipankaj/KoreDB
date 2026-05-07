package com.pankaj.koredb

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.system.measureTimeMillis

@RunWith(AndroidJUnit4::class)
class CollectionPillarTest {
    private lateinit var app: MyApplication
    private val COUNT = 5000

    @Before
    fun setup() = runBlocking {
        app = ApplicationProvider.getApplicationContext()
        app.database.collection("bench_col", Note.serializer()).deleteAll()
        app.roomDatabase.noteDao().deleteAll()
    }

    private fun printResult(op: String, kore: Long, room: Long) {
        val w = if (kore <= room) "✅ KoreDB" else "⚠️ Room"
        println("  $op → KoreDB: ${kore}ms | Room: ${room}ms | $w")
    }

    @Test
    fun test_01_BulkInsert() = runBlocking {
        println("\n📦 COLLECTION: BULK INSERT ($COUNT docs)")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        val kore = measureTimeMillis {
            col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T${it % 50}", "C$it", it % 3 == 0) })
        }
        val room = measureTimeMillis {
            dao.insertAll((1..COUNT).map { Note("n$it", "T${it % 50}", "C$it", it % 3 == 0) })
        }
        printResult("INSERT ($COUNT)", kore, room)
    }

    @Test
    fun test_02_PointRead() = runBlocking {
        println("\n📦 COLLECTION: POINT READ (1000 ops)")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T$it", "C$it") })
        dao.insertAll((1..COUNT).map { Note("n$it", "T$it", "C$it") })

        val kore = measureTimeMillis { repeat(1000) { col.getById("n${it + 1}") } }
        val room = measureTimeMillis { repeat(1000) { dao.getById("n${it + 1}") } }
        printResult("POINT READ (x1000)", kore, room)
    }

    @Test
    fun test_03_SecondaryIndex() = runBlocking {
        println("\n📦 COLLECTION: SECONDARY INDEX LOOKUP")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        col.createIndex("title") { it.title }
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T${it % 50}", "C$it") })
        dao.insertAll((1..COUNT).map { Note("n$it", "T${it % 50}", "C$it") })

        val kore = measureTimeMillis { repeat(100) { col.getByIndex("title", "T${it % 50}") } }
        val room = measureTimeMillis { repeat(100) { dao.getByTitle("T${it % 50}") } }
        printResult("INDEX LOOKUP (x100)", kore, room)
    }

    @Test
    fun test_04_Delete() = runBlocking {
        println("\n📦 COLLECTION: DELETE (1000 docs)")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T$it", "C$it") })
        dao.insertAll((1..COUNT).map { Note("n$it", "T$it", "C$it") })

        val kore = measureTimeMillis { repeat(1000) { col.delete("n${it + 1}") } }
        val room = measureTimeMillis { repeat(1000) { dao.deleteById("n${it + 1}") } }
        printResult("DELETE (x1000)", kore, room)
    }

    @Test
    fun test_05_Count() = runBlocking {
        println("\n📦 COLLECTION: COUNT")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T$it", "C$it") })
        dao.insertAll((1..COUNT).map { Note("n$it", "T$it", "C$it") })

        val kore = measureTimeMillis { repeat(50) { col.count() } }
        val room = measureTimeMillis { repeat(50) { dao.count() } }
        printResult("COUNT (x50)", kore, room)
    }

    @Test
    fun test_06_PartialUpdate() = runBlocking {
        println("\n📦 COLLECTION: PARTIAL UPDATE (500 docs)")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T$it", "C$it", false) })
        dao.insertAll((1..COUNT).map { Note("n$it", "T$it", "C$it", false) })

        val kore = measureTimeMillis {
            repeat(500) { col.updateFields("n${it + 1}") { it.copy(isPinned = true) } }
        }
        val room = measureTimeMillis {
            repeat(500) { dao.update(Note("n${it + 1}", "T${it + 1}", "C${it + 1}", true)) }
        }
        printResult("PARTIAL UPDATE (x500)", kore, room)
    }

    @Test
    fun test_07_QueryDSL() = runBlocking {
        println("\n📦 COLLECTION: QUERY DSL (range + sort + limit)")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        col.registerProperty("title") { it.title }
        col.registerProperty("isPinned") { it.isPinned.toString() }
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T${it % 50}", "C$it", it % 3 == 0) })
        dao.insertAll((1..COUNT).map { Note("n$it", "T${it % 50}", "C$it", it % 3 == 0) })

        val kore = measureTimeMillis {
            repeat(50) {
                col.query().where("isPinned") { it == "true" }.limit(20).execute()
            }
        }
        val room = measureTimeMillis {
            repeat(50) { dao.getByPinned(true) }
        }
        printResult("QUERY+FILTER (x50)", kore, room)
    }

    @Test
    fun test_08_Aggregation() = runBlocking {
        println("\n📦 COLLECTION: AGGREGATION (count+sum)")
        val col = app.database.collection("bench_col", Note.serializer())
        col.registerProperty("title") { it.title }
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T${it % 50}", "C$it") })

        val kore = measureTimeMillis {
            repeat(20) {
                val r = col.query().aggregate { count() }
                assert(r.getCount() > 0)
            }
        }
        println("  AGGREGATE (x20) → KoreDB: ${kore}ms (Room has no equivalent API)")
    }

    @Test
    fun test_09_RangeRead() = runBlocking {
        println("\n📦 COLLECTION: RANGE READ")
        val col = app.database.collection("bench_col", Note.serializer())
        val dao = app.roomDatabase.noteDao()
        col.insertBatch((1..COUNT).associate { "n$it" to Note("n$it", "T$it", "C$it") })
        dao.insertAll((1..COUNT).map { Note("n$it", "T$it", "C$it") })

        val kore = measureTimeMillis { repeat(50) { col.getByIdRange("n1", "n100") } }
        val room = measureTimeMillis { repeat(50) { dao.getByIdRange("n1", "n100") } }
        printResult("RANGE READ (x50)", kore, room)
    }

    @Test
    fun test_10_FullReport() = runBlocking {
        println("\n" + "═".repeat(55))
        println("  📦 COLLECTION PILLAR — FULL REPORT ($COUNT docs)")
        println("═".repeat(55))
        val col = app.database.collection("bench_full_col", Note.serializer())
        col.registerProperty("isPinned") { it.isPinned.toString() }
        col.createIndex("title") { it.title }
        val dao = app.roomDatabase.noteDao()
        dao.deleteAll(); col.deleteAll()

        val data = (1..COUNT).associate { "n$it" to Note("n$it", "T${it%50}", "C$it", it%3==0) }
        val ki = measureTimeMillis { col.insertBatch(data) }
        val ri = measureTimeMillis { dao.insertAll(data.values.toList()) }
        val kr = measureTimeMillis { repeat(1000) { col.getById("n${it+1}") } }
        val rr = measureTimeMillis { repeat(1000) { dao.getById("n${it+1}") } }
        val kix = measureTimeMillis { repeat(100) { col.getByIndex("title", "T${it%50}") } }
        val rix = measureTimeMillis { repeat(100) { dao.getByTitle("T${it%50}") } }
        val kd = measureTimeMillis { repeat(500) { col.delete("n${it+1}") } }
        val rd = measureTimeMillis { repeat(500) { dao.deleteById("n${it+1}") } }
        val kq = measureTimeMillis { repeat(50) { col.query().where("isPinned") { it=="true" }.limit(20).execute() } }
        val rq = measureTimeMillis { repeat(50) { dao.getByPinned(true) } }

        println("  ┌────────────────────────┬──────────┬──────────┐")
        println("  │ Operation              │ KoreDB   │ Room     │")
        println("  ├────────────────────────┼──────────┼──────────┤")
        println("  │ Bulk Insert ($COUNT)   │ ${ki.toString().padStart(5)}ms  │ ${ri.toString().padStart(5)}ms  │")
        println("  │ Point Read (x1000)     │ ${kr.toString().padStart(5)}ms  │ ${rr.toString().padStart(5)}ms  │")
        println("  │ Index Lookup (x100)    │ ${kix.toString().padStart(5)}ms  │ ${rix.toString().padStart(5)}ms  │")
        println("  │ Delete (x500)          │ ${kd.toString().padStart(5)}ms  │ ${rd.toString().padStart(5)}ms  │")
        println("  │ Query+Filter (x50)     │ ${kq.toString().padStart(5)}ms  │ ${rq.toString().padStart(5)}ms  │")
        println("  └────────────────────────┴──────────┴──────────┘")
    }
}
