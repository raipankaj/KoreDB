package com.pankaj.koredb

import com.pankaj.koredb.core.KoreCollection
import com.pankaj.koredb.core.KoreSerializer
import com.pankaj.koredb.engine.KoreDB
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.io.File
import kotlin.system.measureTimeMillis
import org.junit.Assert.*

/**
 * Example local unit test, which will execute on the development machine (host).
 *
 * See [testing documentation](http://d.android.com/tools/testing).
 */
class ExampleUnitTest {


    // 1. Data Model & Serializer
    data class Employee(val id: String, val name: String, val department: String)

    object EmployeeSerializer : KoreSerializer<Employee> {
        override fun serialize(obj: Employee) = "${obj.id},${obj.name},${obj.department}".toByteArray(Charsets.UTF_8)
        override fun deserialize(bytes: ByteArray): Employee {
            val csv = String(bytes, Charsets.UTF_8).split(",")
            return Employee(csv[0], csv[1], csv[2])
        }
    }

    @Test
    fun addition_isCorrect() {
        assertEquals(4, 2 + 2)
    }

    @Test
    fun koreDB_benchmark() {
        runBlocking {
            val dbDir = File("kore_data")
            dbDir.listFiles()?.forEach { it.delete() } // Clean slate

            // Initialize DB
            val db = KoreDB(dbDir)
            println("🚀 COMPACTION STRESS TEST STARTED")

            // We need to generate enough data to trigger 5 flushes.
            // Assuming 1MB threshold.
            val payload = "A".repeat(1024 * 10) // 10KB payload

            // Loop to create 5 Segments
            for (segment in 1..5) {
                println("--- Generating Segment $segment ---")

                // Write 150 records (150 * 10KB = ~1.5MB) -> Forces a Flush
                val batch = mutableListOf<Pair<ByteArray, ByteArray>>()
                for (i in 1..150) {
                    val key = "seg_${segment}_user_$i".toByteArray()
                    batch.add(Pair(key, payload.toByteArray()))
                }

                // Also add a Tombstone in Segment 2 to test deletion
                if (segment == 2) {
                    println("⚰️ Injecting Tombstone for 'seg_1_user_50'...")
                    db.deleteRaw("seg_1_user_50".toByteArray())
                }

                db.writeBatchRaw(batch) // Should trigger flush here

                // Small delay to ensure file system timestamp differences
                Thread.sleep(100)
            }

            println("\n⏳ Waiting for background operations...")

            // Verify File Count
            val sstFiles = dbDir.listFiles { _, name -> name.endsWith(".sst") }
            println("📂 Final SST File Count: ${sstFiles?.size}")

            // Ideally, we want 1 or 2 files (depending on if compaction caught up).
            // If we had 5 flushes, and threshold is 3:
            // 1, 2, 3 -> Compact -> 1
            // 1, 2 -> Compact -> 1
            // Final result should be very low file count.

            if (sstFiles != null && sstFiles.size < 3) {
                println("✅ SUCCESS! Compaction reduced 5 segments down to ${sstFiles.size}.")
            } else {
                println("❌ FAILED. Still have too many files.")
            }

            // Verify Tombstone Logic (The deleted item should be GONE)
            val deletedUser = db.getRaw("seg_1_user_50".toByteArray())
            if (deletedUser == null) {
                println("✅ SUCCESS! 'seg_1_user_50' is permanently deleted.")
            } else {
                println("❌ FAILED. Deleted user resurrected!")
            }

            db.close()
        }
    }
}
