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

import com.pankaj.koredb.db.KoreDatabase
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

@Serializable
data class InventoryItem(
    val id: String,
    val name: String,
    val quantity: Int,
    val price: Double
)

class DataExportImportTest {

    private lateinit var testDir: File
    private lateinit var db: KoreDatabase

    @Before
    fun setup() {
        testDir = File("build/tmp/test_export_${UUID.randomUUID()}").apply { mkdirs() }
        db = KoreDatabase(testDir)
    }

    @After
    fun tearDown() {
        db.close()
        testDir.deleteRecursively()
    }

    @Test
    fun testJsonExportAndImport() = runBlocking {
        val inventory = db.collection<InventoryItem>("inventory")
        for (i in 1..25) {
            inventory.insert("item_$i", InventoryItem("item_$i", "Widget $i", i * 10, i * 9.99))
        }
        assertEquals(25, inventory.count())

        val exportFile = File(testDir, "inventory_export.json")
        val exportStats = inventory.exportToJson(exportFile)

        assertEquals(25L, exportStats.totalRecords)
        assertTrue(exportFile.exists())
        assertTrue(exportFile.length() > 0)

        // Wipe the collection
        inventory.deleteAll()
        assertEquals(0, inventory.count())

        // Import back from JSON
        val importStats = inventory.importFromJson(exportFile)
        assertEquals(25L, importStats.totalRecords)
        assertEquals(25, inventory.count())

        val item12 = inventory.getById("item_12")
        assertNotNull(item12)
        assertEquals("Widget 12", item12?.name)
        assertEquals(120, item12?.quantity)
        assertEquals(119.88, item12!!.price, 0.01)
    }

    @Test
    fun testCsvExportAndImport() = runBlocking {
        val inventory = db.collection<InventoryItem>("inventory")
        inventory.insert("item_1", InventoryItem("item_1", "Screw, 10mm \"Pro\"", 100, 0.99))
        inventory.insert("item_2", InventoryItem("item_2", "Hammer", 10, 19.99))

        val exportFile = File(testDir, "inventory_export.csv")
        val exportStats = inventory.exportToCsv(
            outputFile = exportFile,
            headers = listOf("id", "name", "quantity", "price"),
            rowMapper = { listOf(it.name, it.quantity.toString(), it.price.toString()) }
        )

        assertEquals(2L, exportStats.totalRecords)
        assertTrue(exportFile.exists())

        // Wipe collection
        inventory.deleteAll()
        assertEquals(0, inventory.count())

        // Import from CSV
        val importStats = inventory.importFromCsv(
            inputFile = exportFile,
            hasHeader = true,
            rowParser = { tokens ->
                val id = tokens[0]
                val name = tokens[1]
                val qty = tokens[2].toInt()
                val price = tokens[3].toDouble()
                Pair(id, InventoryItem(id, name, qty, price))
            }
        )

        assertEquals(2L, importStats.totalRecords)
        assertEquals(2, inventory.count())

        val item1 = inventory.getById("item_1")
        assertNotNull(item1)
        assertEquals("Screw, 10mm \"Pro\"", item1?.name)
        assertEquals(100, item1?.quantity)
    }
}
