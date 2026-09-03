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

package com.pankaj.koredb.benchmark

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import kotlin.system.measureNanoTime

/**
 * Head-to-Head Comparative Benchmark: KoreDB vs Room/SQLite Engine.
 *
 * Evaluates real-world performance between:
 * - Room / SQLite (Android's standard local storage engine using sqlite-jdbc WAL mode)
 * - KoreDB (LSM-tree document engine with order-preserving numeric indices and zero-copy block cache)
 *
 * Workloads:
 * 1. Bulk Inserts (10,000 documents)
 * 2. Point Reads (10,000 random ID lookups)
 * 3. Secondary String Index Lookups (2,000 queries)
 * 4. Numeric Range Queries with Pushdown (2,000 queries)
 * 5. Batch Deletes (2,000 deletions)
 * 6. Discrete ACID Transactions (1,000 transactions)
 * 7. Storage Footprint on Disk
 */
class RoomVsKoreDbBenchmarkTest {

    private lateinit var koreDir: File
    private lateinit var sqliteDir: File
    private lateinit var sqliteFile: File
    private lateinit var sqliteConn: Connection

    @Serializable
    data class Product(
        val id: String,
        val title: String,
        val category: String,
        val price: Double,
        val stock: Int
    )

    @Before
    fun setUp() {
        val runId = UUID.randomUUID().toString()
        koreDir = File("build/tmp/bench_koredb_$runId")
        sqliteDir = File("build/tmp/bench_sqlite_$runId")
        koreDir.mkdirs()
        sqliteDir.mkdirs()

        sqliteFile = File(sqliteDir, "bench_room.db")
        sqliteConn = DriverManager.getConnection("jdbc:sqlite:${sqliteFile.absolutePath}")

        sqliteConn.createStatement().use { stmt ->
            // Match Android Room's default production configuration (WAL journal mode + NORMAL sync)
            stmt.execute("PRAGMA journal_mode = WAL;")
            stmt.execute("PRAGMA synchronous = NORMAL;")
            stmt.execute(
                """
                CREATE TABLE products (
                    id TEXT PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL,
                    category TEXT NOT NULL,
                    price REAL NOT NULL,
                    stock INTEGER NOT NULL
                );
                """.trimIndent()
            )
            stmt.execute("CREATE INDEX idx_products_category ON products(category);")
            stmt.execute("CREATE INDEX idx_products_price ON products(price);")
        }
    }

    @After
    fun tearDown() {
        try {
            if (!sqliteConn.isClosed) sqliteConn.close()
        } catch (_: Exception) {}
        koreDir.deleteRecursively()
        sqliteDir.deleteRecursively()
    }

    @Test
    fun runComparativeHeadToHeadBenchmark() {
        val count = 10_000
        val categories = listOf("electronics", "books", "fashion", "home", "sports")

        val dataset = (1..count).map { i ->
            Product(
                id = String.format("prod_%06d", i),
                title = "Product Item $i High Quality Specification",
                category = categories[i % categories.size],
                price = 10.0 + (i % 500) * 1.5,
                stock = i % 100
            )
        }

        println("\n=========================================================================================")
        println("                      KOREDB vs ROOM/SQLITE HEAD-TO-HEAD BENCHMARK                       ")
        println("=========================================================================================")
        println(String.format("%-32s | %-16s | %-16s | %-12s", "Workload", "Room / SQLite", "KoreDB", "Winner"))
        println("---------------------------------+------------------+------------------+-------------")

        // --------------------------------------------------------------------
        // WORKLOAD 1: 10,000 Bulk Inserts
        // --------------------------------------------------------------------
        val sqliteInsertNanos = measureNanoTime {
            sqliteConn.autoCommit = false
            val insertSql = "INSERT INTO products (id, title, category, price, stock) VALUES (?, ?, ?, ?, ?);"
            sqliteConn.prepareStatement(insertSql).use { pstmt ->
                for (p in dataset) {
                    pstmt.setString(1, p.id)
                    pstmt.setString(2, p.title)
                    pstmt.setString(3, p.category)
                    pstmt.setDouble(4, p.price)
                    pstmt.setInt(5, p.stock)
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
            }
            sqliteConn.commit()
            sqliteConn.autoCommit = true
        }
        val sqliteInsertMs = sqliteInsertNanos / 1_000_000.0

        val koreDb = KoreDatabase(koreDir, enableCdc = false)
        val koreCol = koreDb.binaryCollection<Product>("products")
        koreCol.createIndex("category") { it.category }
        koreCol.createNumericIndex("price") { it.price }

        val koreInsertNanos = measureNanoTime {
            runBlocking {
                koreCol.insertBatch(dataset) { it.id }
            }
        }
        val koreInsertMs = koreInsertNanos / 1_000_000.0
        val insertWinner = if (koreInsertMs < sqliteInsertMs) "KoreDB (${String.format("%.1fx", sqliteInsertMs / koreInsertMs)})" else "SQLite"

        println(String.format("%-32s | %13.2f ms | %13.2f ms | %-12s", "10,000 Bulk Inserts", sqliteInsertMs, koreInsertMs, insertWinner))

        // --------------------------------------------------------------------
        // WORKLOAD 2: 10,000 Random Point Reads (getById)
        // --------------------------------------------------------------------
        val sampleIds = dataset.map { it.id }.shuffled()

        val sqliteReadNanos = measureNanoTime {
            val selectSql = "SELECT id, title, category, price, stock FROM products WHERE id = ?;"
            sqliteConn.prepareStatement(selectSql).use { pstmt ->
                for (id in sampleIds) {
                    pstmt.setString(1, id)
                    val rs = pstmt.executeQuery()
                    if (rs.next()) {
                        Product(rs.getString(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getInt(5))
                    }
                    rs.close()
                }
            }
        }
        val sqliteReadMs = sqliteReadNanos / 1_000_000.0

        val koreReadNanos = measureNanoTime {
            runBlocking {
                for (id in sampleIds) {
                    val p = koreCol.getById(id)
                    assertNotNull(p)
                }
            }
        }
        val koreReadMs = koreReadNanos / 1_000_000.0
        val readWinner = if (koreReadMs < sqliteReadMs) "KoreDB (${String.format("%.1fx", sqliteReadMs / koreReadMs)})" else "SQLite"

        println(String.format("%-32s | %13.2f ms | %13.2f ms | %-12s", "10,000 Point Reads (PK)", sqliteReadMs, koreReadMs, readWinner))

        // --------------------------------------------------------------------
        // WORKLOAD 3: 2,000 Secondary String Index Lookups
        // --------------------------------------------------------------------
        val queryCategories = (1..2000).map { categories[it % categories.size] }

        val sqliteIndexNanos = measureNanoTime {
            val selectSql = "SELECT id, title, category, price, stock FROM products WHERE category = ? LIMIT 50;"
            sqliteConn.prepareStatement(selectSql).use { pstmt ->
                for (cat in queryCategories) {
                    pstmt.setString(1, cat)
                    val rs = pstmt.executeQuery()
                    val list = mutableListOf<Product>()
                    while (rs.next()) {
                        list.add(Product(rs.getString(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getInt(5)))
                    }
                    rs.close()
                }
            }
        }
        val sqliteIndexMs = sqliteIndexNanos / 1_000_000.0

        val koreIndexNanos = measureNanoTime {
            runBlocking {
                for (cat in queryCategories) {
                    val res = koreCol.query().whereEq("category", cat).limit(50).execute()
                    assertTrue(res.isNotEmpty())
                }
            }
        }
        val koreIndexMs = koreIndexNanos / 1_000_000.0
        val indexWinner = if (koreIndexMs < sqliteIndexMs) "KoreDB (${String.format("%.1fx", sqliteIndexMs / koreIndexMs)})" else "SQLite"

        println(String.format("%-32s | %13.2f ms | %13.2f ms | %-12s", "2,000 Secondary Index Scans", sqliteIndexMs, koreIndexMs, indexWinner))

        // --------------------------------------------------------------------
        // WORKLOAD 4: 2,000 Numeric Range Queries
        // --------------------------------------------------------------------
        val priceRanges = (1..2000).map {
            val min = 50.0 + (it % 100) * 2.0
            val max = min + 50.0
            min to max
        }

        val sqliteRangeNanos = measureNanoTime {
            val selectSql = "SELECT id, title, category, price, stock FROM products WHERE price BETWEEN ? AND ? LIMIT 50;"
            sqliteConn.prepareStatement(selectSql).use { pstmt ->
                for ((min, max) in priceRanges) {
                    pstmt.setDouble(1, min)
                    pstmt.setDouble(2, max)
                    val rs = pstmt.executeQuery()
                    val list = mutableListOf<Product>()
                    while (rs.next()) {
                        list.add(Product(rs.getString(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getInt(5)))
                    }
                    rs.close()
                }
            }
        }
        val sqliteRangeMs = sqliteRangeNanos / 1_000_000.0

        val koreRangeNanos = measureNanoTime {
            runBlocking {
                for ((min, max) in priceRanges) {
                    val res = koreCol.query().whereBetween("price", min, max).limit(50).execute()
                    assertNotNull(res)
                }
            }
        }
        val koreRangeMs = koreRangeNanos / 1_000_000.0
        val rangeWinner = if (koreRangeMs < sqliteRangeMs) "KoreDB (${String.format("%.1fx", sqliteRangeMs / koreRangeMs)})" else "SQLite"

        println(String.format("%-32s | %13.2f ms | %13.2f ms | %-12s", "2,000 Numeric Range Scans", sqliteRangeMs, koreRangeMs, rangeWinner))

        // --------------------------------------------------------------------
        // WORKLOAD 5: 2,000 Batch Deletions
        // --------------------------------------------------------------------
        val toDelete = sampleIds.take(2000)

        val sqliteDeleteNanos = measureNanoTime {
            sqliteConn.autoCommit = false
            val deleteSql = "DELETE FROM products WHERE id = ?;"
            sqliteConn.prepareStatement(deleteSql).use { pstmt ->
                for (id in toDelete) {
                    pstmt.setString(1, id)
                    pstmt.addBatch()
                }
                pstmt.executeBatch()
            }
            sqliteConn.commit()
            sqliteConn.autoCommit = true
        }
        val sqliteDeleteMs = sqliteDeleteNanos / 1_000_000.0

        val koreDeleteNanos = measureNanoTime {
            runBlocking {
                koreCol.deleteBatch(toDelete)
            }
        }
        val koreDeleteMs = koreDeleteNanos / 1_000_000.0
        val deleteWinner = if (koreDeleteMs < sqliteDeleteMs) "KoreDB (${String.format("%.1fx", sqliteDeleteMs / koreDeleteMs)})" else "SQLite"

        println(String.format("%-32s | %13.2f ms | %13.2f ms | %-12s", "2,000 Batch Deletes", sqliteDeleteMs, koreDeleteMs, deleteWinner))

        // --------------------------------------------------------------------
        // WORKLOAD 6: 1,000 Discrete ACID Transactions
        // --------------------------------------------------------------------
        val txProducts = (1..1000).map { i ->
            Product("tx_$i", "TX Item $i", "tx_cat", 99.0, 1)
        }

        val sqliteTxNanos = measureNanoTime {
            val insertSql = "INSERT OR REPLACE INTO products (id, title, category, price, stock) VALUES (?, ?, ?, ?, ?);"
            for (p in txProducts) {
                sqliteConn.autoCommit = false
                sqliteConn.prepareStatement(insertSql).use { pstmt ->
                    pstmt.setString(1, p.id)
                    pstmt.setString(2, p.title)
                    pstmt.setString(3, p.category)
                    pstmt.setDouble(4, p.price)
                    pstmt.setInt(5, p.stock)
                    pstmt.executeUpdate()
                }
                sqliteConn.commit()
                sqliteConn.autoCommit = true
            }
        }
        val sqliteTxMs = sqliteTxNanos / 1_000_000.0

        val koreTxNanos = measureNanoTime {
            runBlocking {
                for (p in txProducts) {
                    koreDb.transaction { tx ->
                        val txCol = tx.binaryCollection<Product>("products")
                        txCol.insert(p.id, p)
                    }
                }
            }
        }
        val koreTxMs = koreTxNanos / 1_000_000.0
        val txWinner = if (koreTxMs < sqliteTxMs) "KoreDB (${String.format("%.1fx", sqliteTxMs / koreTxMs)})" else "SQLite"

        println(String.format("%-32s | %13.2f ms | %13.2f ms | %-12s", "1,000 Discrete Transactions", sqliteTxMs, koreTxMs, txWinner))

        // --------------------------------------------------------------------
        // WORKLOAD 7: Storage Footprint on Disk
        // --------------------------------------------------------------------
        koreDb.compact()
        koreDb.close()
        sqliteConn.close()



        val sqliteTotalBytes = sqliteDir.walkTopDown().filter { it.isFile }.sumOf { it.length() }
        val koreTotalBytes = koreDir.walkTopDown().filter { it.isFile }.sumOf { it.length() }

        val sqliteKb = sqliteTotalBytes / 1024.0
        val koreKb = koreTotalBytes / 1024.0
        val diskWinner = if (koreKb < sqliteKb) "KoreDB (${String.format("%.1fx smaller", sqliteKb / koreKb)})" else "SQLite"

        println("---------------------------------+------------------+------------------+-------------")
        println(String.format("%-32s | %13.1f KB | %13.1f KB | %-12s", "Storage Footprint (Disk)", sqliteKb, koreKb, diskWinner))
        println("=========================================================================================\n")

        assertTrue("Benchmark completed successfully", koreTotalBytes > 0 && sqliteTotalBytes > 0)
    }
}
