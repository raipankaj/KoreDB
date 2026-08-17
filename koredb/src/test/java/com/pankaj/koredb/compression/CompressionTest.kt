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

package com.pankaj.koredb.compression

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
data class LargeArticle(
    val id: String,
    val title: String,
    val body: String
)

class CompressionTest {

    private lateinit var uncompressedDir: File
    private lateinit var compressedDir: File

    @Before
    fun setup() {
        uncompressedDir = File("build/tmp/test_uncompressed_${UUID.randomUUID()}").apply { mkdirs() }
        compressedDir = File("build/tmp/test_compressed_${UUID.randomUUID()}").apply { mkdirs() }
    }

    @After
    fun tearDown() {
        uncompressedDir.deleteRecursively()
        compressedDir.deleteRecursively()
    }

    @Test
    fun testDirectCodecs() {
        val sampleText = "KoreDB is an embedded database. ".repeat(100).toByteArray(Charsets.UTF_8)

        // 1. Deflate Codec
        val deflateCodec = DeflateCompressionCodec()
        val deflated = deflateCodec.compress(sampleText)
        assertTrue("Deflated size should be significantly smaller", deflated.size < sampleText.size / 2)
        val decompressedDeflate = deflateCodec.decompress(deflated)
        assertArrayEquals(sampleText, decompressedDeflate)

        // 2. Gzip Codec
        val gzipCodec = GzipCompressionCodec
        val gzipped = gzipCodec.compress(sampleText)
        assertTrue("Gzipped size should be significantly smaller", gzipped.size < sampleText.size / 2)
        val decompressedGzip = gzipCodec.decompress(gzipped)
        assertArrayEquals(sampleText, decompressedGzip)
    }

    @Test
    fun testSSTableCompressionReducesDiskFootprintAndMaintainsFidelity() = runBlocking {
        val repetitiveBody = "Log-Structured Merge-tree architecture optimized for mobile devices. ".repeat(50)

        // 1. Write to Uncompressed DB
        val dbUncompressed = KoreDatabase(uncompressedDir, compressionCodec = NoOpCompressionCodec)
        val collUncompressed = dbUncompressed.collection<LargeArticle>("articles")
        for (i in 0 until 50) {
            collUncompressed.insert("art_$i", LargeArticle("art_$i", "Title $i", repetitiveBody))
        }
        dbUncompressed.engine.flushMemTableInternal()
        val uncompressedSst = uncompressedDir.listFiles { _, name -> name.endsWith(".sst") }!!.first()
        val uncompressedSize = uncompressedSst.length()
        dbUncompressed.close()

        // 2. Write to Compressed DB (Deflate)
        val dbCompressed = KoreDatabase(compressedDir, compressionCodec = DeflateCompressionCodec())
        val collCompressed = dbCompressed.collection<LargeArticle>("articles")
        for (i in 0 until 50) {
            collCompressed.insert("art_$i", LargeArticle("art_$i", "Title $i", repetitiveBody))
        }
        dbCompressed.engine.flushMemTableInternal()
        val compressedSst = compressedDir.listFiles { _, name -> name.endsWith(".sst") }!!.first()
        val compressedSize = compressedSst.length()

        // Assert size reduction
        assertTrue(
            "Compressed SSTable ($compressedSize bytes) should be substantially smaller than uncompressed ($uncompressedSize bytes)",
            compressedSize < uncompressedSize / 2
        )

        // 3. Verify Read Fidelity
        for (i in 0 until 50) {
            val article = collCompressed.getById("art_$i")
            assertNotNull(article)
            assertEquals("Title $i", article?.title)
            assertEquals(repetitiveBody, article?.body)
        }

        // 4. Verify Prefix Scan
        val allArticles = collCompressed.getByIdPrefix("art_")
        assertEquals(50, allArticles.size)

        dbCompressed.close()
    }
}
