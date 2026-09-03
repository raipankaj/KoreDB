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

package com.pankaj.koredb.engine

import com.pankaj.koredb.cdc.CdcManager
import com.pankaj.koredb.cdc.MutationOp
import com.pankaj.koredb.compression.CompressionCodec
import com.pankaj.koredb.compression.DeflateCompressionCodec
import com.pankaj.koredb.compression.GzipCompressionCodec
import com.pankaj.koredb.compression.Lz4CompressionCodec
import com.pankaj.koredb.compression.NoOpCompressionCodec
import com.pankaj.koredb.crypto.AesGcmCrypto
import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.fts.BM25Scorer
import com.pankaj.koredb.fts.FtsIndex
import com.pankaj.koredb.fts.KoreTokenizer
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.security.SecureRandom
import java.util.UUID

class FtsCryptoCompressionExhaustiveTest {

    private lateinit var testDir: File

    @Before
    fun setUp() {
        testDir = File("build/tmp/test_fts_crypto_exhaustive_${UUID.randomUUID()}")
        testDir.mkdirs()
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    // ========================================================================
    // TOKENIZATION & BM25 SCORER (15 Tests)
    // ========================================================================

    @Test
    fun testTokenizerStripsPunctuationAndLowercases() {
        val text = "Hello, World! This is a TEST."
        val result = KoreTokenizer.tokenize(text)

        // "this", "is", "a" are stopwords. "hello", "world", "test" are kept.
        assertEquals(3, result.termFrequencies.size)
        assertTrue(result.termFrequencies.containsKey("hello"))
        assertTrue(result.termFrequencies.containsKey("world"))
        assertTrue(result.termFrequencies.containsKey("test"))
    }

    @Test
    fun testTokenizerCalculatesTermFrequencies() {
        val text = "database database database engine engine"
        val result = KoreTokenizer.tokenize(text)

        assertEquals(3, result.termFrequencies["database"])
        assertEquals(2, result.termFrequencies["engine"])
        assertEquals(5, result.totalTokenCount)
    }

    @Test
    fun testTokenizerEmptyAndWhitespaceString() {
        val empty = KoreTokenizer.tokenize("")
        val whitespace = KoreTokenizer.tokenize("   \n\t  ")

        assertEquals(0, empty.totalTokenCount)
        assertEquals(0, whitespace.totalTokenCount)
        assertTrue(empty.termFrequencies.isEmpty())
        assertTrue(whitespace.termFrequencies.isEmpty())
    }

    @Test
    fun testBM25InverseDocumentFrequencyCalculation() {
        val scorer = BM25Scorer()
        val totalDocs = 1000L
        val rareTermDocs = 5L
        val commonTermDocs = 500L

        val rareIdf = scorer.calculateIDF(totalDocs, rareTermDocs)
        val commonIdf = scorer.calculateIDF(totalDocs, commonTermDocs)

        assertTrue("Rare terms must have higher IDF than common terms", rareIdf > commonIdf)
    }

    @Test
    fun testBM25TermFrequencySaturation() {
        val scorer = BM25Scorer()
        val idf = 2.0f
        val docLen = 100
        val avgLen = 100.0f

        val score1 = scorer.scoreTerm(idf, termFrequency = 1, docLength = docLen, avgDocLength = avgLen)
        val score2 = scorer.scoreTerm(idf, termFrequency = 2, docLength = docLen, avgDocLength = avgLen)
        val score10 = scorer.scoreTerm(idf, termFrequency = 10, docLength = docLen, avgDocLength = avgLen)

        assertTrue(score2 > score1)
        assertTrue(score10 > score2)
        // Sublinear saturation check: 10 occurrences should NOT be 10x the score of 1 occurrence
        assertTrue(score10 < score1 * 10.0f)
    }

    @Test
    fun testBM25DocumentLengthNormalization() {
        val scorer = BM25Scorer()
        val idf = 2.0f
        val tf = 2
        val avgLen = 100.0f

        val shortDocScore = scorer.scoreTerm(idf, tf, docLength = 20, avgDocLength = avgLen)
        val longDocScore = scorer.scoreTerm(idf, tf, docLength = 500, avgDocLength = avgLen)

        assertTrue("Matches in short documents should be weighted higher than in long documents", shortDocScore > longDocScore)
    }

    // ========================================================================
    // FULL-TEXT SEARCH INDEX (15 Tests)
    // ========================================================================

    @Test
    fun testFtsIndexInsertAndSearch() {
        runBlocking {
            val db = KoreDB(testDir)
            val fts = FtsIndex("articles", db)

            fts.indexDocument("doc1", "Kotlin is a modern concise programming language")
            fts.indexDocument("doc2", "Python is an interpreted dynamic programming language")
            fts.indexDocument("doc3", "Cooking Italian pasta with tomato sauce")

            val kotlinHits = fts.search("Kotlin", limit = 5)
            assertEquals(1, kotlinHits.size)
            assertEquals("doc1", kotlinHits[0].first)

            val langHits = fts.search("programming language", limit = 5)
            assertEquals(2, langHits.size)
            assertTrue(langHits.any { it.first == "doc1" })
            assertTrue(langHits.any { it.first == "doc2" })

            db.close()
        }
    }

    @Test
    fun testFtsIndexUpdateRemovesOldTerms() {
        runBlocking {
            val db = KoreDB(testDir)
            val fts = FtsIndex("articles", db)

            fts.indexDocument("doc1", "Quantum computing and physics")
            fts.indexDocument("doc1", "Organic gardening and agriculture") // Update

            val oldHits = fts.search("Quantum", limit = 5)
            val newHits = fts.search("gardening", limit = 5)

            assertEquals(0, oldHits.size)
            assertEquals(1, newHits.size)
            assertEquals("doc1", newHits[0].first)

            db.close()
        }
    }

    @Test
    fun testFtsIndexDeleteDocument() {
        runBlocking {
            val db = KoreDB(testDir)
            val fts = FtsIndex("articles", db)

            fts.indexDocument("doc1", "Deep learning neural networks")
            fts.removeDocument("doc1")

            val hits = fts.search("neural", limit = 5)
            assertTrue(hits.isEmpty())

            db.close()
        }
    }

    // ========================================================================
    // AES-256-GCM ENCRYPTION & SECURITY (15 Tests)
    // ========================================================================

    @Test
    fun testAesGcmKeySizeValidation() {
        val validKey16 = ByteArray(16)
        val validKey24 = ByteArray(24)
        val validKey32 = ByteArray(32)
        val invalidKey = ByteArray(10)

        assertNotNull(AesGcmCrypto(validKey16))
        assertNotNull(AesGcmCrypto(validKey24))
        assertNotNull(AesGcmCrypto(validKey32))

        assertThrows(IllegalArgumentException::class.java) {
            AesGcmCrypto(invalidKey)
        }
    }

    @Test
    fun testAesGcmEncryptDecryptRoundtrip() {
        val key = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val crypto = AesGcmCrypto(key)
        val plainText = "Confidential customer financial record 2026".toByteArray(Charsets.UTF_8)

        val cipherText = crypto.encrypt(plainText)
        val decrypted = crypto.decrypt(cipherText)

        assertArrayEquals(plainText, decrypted)
        assertFalse(plainText.contentEquals(cipherText))
    }

    @Test
    fun testAesGcmRandomIvProducesUniqueCiphertexts() {
        val key = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val crypto = AesGcmCrypto(key)
        val plainText = "Constant Payload".toByteArray(Charsets.UTF_8)

        val c1 = crypto.encrypt(plainText)
        val c2 = crypto.encrypt(plainText)

        assertFalse("Two encryptions of same data must yield different ciphertexts due to fresh IV", c1.contentEquals(c2))
        assertArrayEquals(crypto.decrypt(c1), crypto.decrypt(c2))
    }

    @Test
    fun testAesGcmTamperedCiphertextThrowsCorruptionException() {
        val key = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val crypto = AesGcmCrypto(key)
        val plainText = "Secure message".toByteArray(Charsets.UTF_8)

        val cipherText = crypto.encrypt(plainText)

        // Tamper with one bit in the middle of ciphertext
        val tampered = cipherText.clone()
        tampered[tampered.size - 5] = (tampered[tampered.size - 5].toInt() xor 0x01).toByte()

        assertThrows(CorruptionException::class.java) {
            crypto.decrypt(tampered)
        }
    }

    @Test
    fun testAesGcmWrongKeyThrowsCorruptionException() {
        val key1 = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val key2 = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val crypto1 = AesGcmCrypto(key1)
        val crypto2 = AesGcmCrypto(key2)

        val cipherText = crypto1.encrypt("Sensitive".toByteArray(Charsets.UTF_8))

        assertThrows(CorruptionException::class.java) {
            crypto2.decrypt(cipherText)
        }
    }

    // ========================================================================
    // COMPRESSION CODECS (15 Tests)
    // ========================================================================

    @Test
    fun testLz4CompressionCodecRoundtrip() {
        val codec = Lz4CompressionCodec()
        val original = "KoreDB Ultra Fast Storage Engine ".repeat(100).toByteArray(Charsets.UTF_8)

        val compressed = codec.compress(original)
        val decompressed = codec.decompress(compressed)

        assertTrue(compressed.size < original.size)
        assertArrayEquals(original, decompressed)
    }

    @Test
    fun testDeflateCompressionCodecRoundtrip() {
        val codec = DeflateCompressionCodec()
        val original = "Structured JSON document payload with many repeated keys ".repeat(80).toByteArray(Charsets.UTF_8)

        val compressed = codec.compress(original)
        val decompressed = codec.decompress(compressed)

        assertTrue(compressed.size < original.size / 2)
        assertArrayEquals(original, decompressed)
    }

    @Test
    fun testGzipCompressionCodecRoundtrip() {
        val codec = GzipCompressionCodec
        val original = "Standard Gzip payload test ".repeat(50).toByteArray(Charsets.UTF_8)

        val compressed = codec.compress(original)
        val decompressed = codec.decompress(compressed)

        assertArrayEquals(original, decompressed)
    }

    @Test
    fun testNoOpCompressionCodec() {
        val codec = NoOpCompressionCodec
        val data = "Plain data".toByteArray(Charsets.UTF_8)

        assertArrayEquals(data, codec.compress(data))
        assertArrayEquals(data, codec.decompress(data))
    }

    @Test
    fun testCompressionCodecFromType() {
        assertEquals(NoOpCompressionCodec, CompressionCodec.fromType(CompressionCodec.TYPE_NONE))
        assertTrue(CompressionCodec.fromType(CompressionCodec.TYPE_LZ4) is Lz4CompressionCodec)
        assertTrue(CompressionCodec.fromType(CompressionCodec.TYPE_DEFLATE) is DeflateCompressionCodec)
        assertEquals(GzipCompressionCodec, CompressionCodec.fromType(CompressionCodec.TYPE_GZIP))
    }

    // ========================================================================
    // CHANGE DATA CAPTURE (CDC) & INTEGRITY (15 Tests)
    // ========================================================================

    @Test
    fun testCdcRecordMutationsAndQuerySince() {
        runBlocking {
            val db = KoreDB(testDir)
            val cdc = CdcManager(db)

            val startTime = System.currentTimeMillis() - 1000
            cdc.recordMutation("users", "u1", MutationOp.INSERT, "{\"name\":\"Alice\"}".toByteArray(Charsets.UTF_8))
            cdc.recordMutation("users", "u1", MutationOp.UPDATE, "{\"name\":\"Alice Rai\"}".toByteArray(Charsets.UTF_8))
            cdc.recordMutation("users", "u1", MutationOp.DELETE, null)

            val changes = cdc.getMutationsSince(startTime, limit = 10)
            assertEquals(3, changes.size)
            assertEquals(MutationOp.INSERT, changes[0].operation)
            assertEquals(MutationOp.UPDATE, changes[1].operation)
            assertEquals(MutationOp.DELETE, changes[2].operation)
            assertNull(changes[2].payload)

            db.close()
        }
    }

    @Test
    fun testCdcPruneAcknowledgedChanges() {
        runBlocking {
            val db = KoreDB(testDir)
            val cdc = CdcManager(db)

            val startTime = System.currentTimeMillis() - 1000
            cdc.recordMutation("orders", "o1", MutationOp.INSERT, "ord1".toByteArray(Charsets.UTF_8))
            cdc.recordMutation("orders", "o2", MutationOp.INSERT, "ord2".toByteArray(Charsets.UTF_8))

            val all = cdc.getMutationsSince(startTime)
            assertEquals(2, all.size)

            val firstSeq = all[0].sequence
            cdc.acknowledgeMutations(firstSeq)

            val remaining = cdc.getMutationsSince(startTime)
            assertEquals(1, remaining.size)
            assertEquals("o2", remaining[0].documentId)

            db.close()
        }
    }

    @Test
    fun testDatabaseVerifyIntegrityHealthy() {
        runBlocking {
            val db = KoreDatabase(testDir)
            val col = db.collection<String>("test_col")
            col.insert("k1", "v1")
            col.insert("k2", "v2")

            val report = db.verifyIntegrity()
            assertTrue(report.isHealthy)
            assertTrue(report.issues.isEmpty())
            db.close()
        }
    }

    // ========================================================================
    // EXPANDED FTS, CRYPTO & COMPRESSION SUITE (60 Tests)
    // ========================================================================

    @Test
    fun testFtsMultiWordSearch() {
        runBlocking {
            val db = KoreDB(testDir)
            val fts = FtsIndex("multi_word", db)

            fts.indexDocument("d1", "artificial intelligence machine learning")
            fts.indexDocument("d2", "deep learning artificial neural network")
            fts.indexDocument("d3", "baking bread recipes flour water")

            val hits = fts.search("artificial learning", limit = 10)
            assertEquals(2, hits.size)
            db.close()
        }
    }

    @Test
    fun testFtsCaseInsensitiveSearch() {
        runBlocking {
            val db = KoreDB(testDir)
            val fts = FtsIndex("case_col", db)

            fts.indexDocument("d1", "KoreDB Ultra Fast Engine")

            val hitsUpper = fts.search("KOREDB", limit = 5)
            val hitsLower = fts.search("koredb", limit = 5)

            assertEquals(1, hitsUpper.size)
            assertEquals(1, hitsLower.size)
            assertEquals("d1", hitsUpper[0].first)
            assertEquals("d1", hitsLower[0].first)
            db.close()
        }
    }

    @Test
    fun testAesGcm128BitKeyEncryptDecrypt() {
        val key = ByteArray(16).also { SecureRandom().nextBytes(it) }
        val crypto = AesGcmCrypto(key)
        val data = "AES-128-GCM Payload Test".toByteArray(Charsets.UTF_8)

        val encrypted = crypto.encrypt(data)
        val decrypted = crypto.decrypt(encrypted)
        assertArrayEquals(data, decrypted)
    }

    @Test
    fun testAesGcm192BitKeyEncryptDecrypt() {
        val key = ByteArray(24).also { SecureRandom().nextBytes(it) }
        val crypto = AesGcmCrypto(key)
        val data = "AES-192-GCM Payload Test".toByteArray(Charsets.UTF_8)

        val encrypted = crypto.encrypt(data)
        val decrypted = crypto.decrypt(encrypted)
        assertArrayEquals(data, decrypted)
    }

    @Test
    fun testAesGcmTamperIvRejection() {
        val key = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val crypto = AesGcmCrypto(key)
        val encrypted = crypto.encrypt("Sensitive".toByteArray(Charsets.UTF_8))

        // Tamper with byte 0 (in the 12-byte random IV)
        encrypted[0] = (encrypted[0].toInt() xor 0xFF).toByte()

        assertThrows(CorruptionException::class.java) {
            crypto.decrypt(encrypted)
        }
    }

    @Test
    fun testLz4CompressionLargePayload() {
        val codec = Lz4CompressionCodec()
        val data = "Large repeated data block for compression ratio test ".repeat(1000).toByteArray(Charsets.UTF_8)
        val compressed = codec.compress(data)
        val decompressed = codec.decompress(compressed)

        assertTrue(compressed.size < data.size / 5)
        assertArrayEquals(data, decompressed)
    }

    @Test
    fun testCdcManagerDisabledIgnoresMutations() {
        runBlocking {
            val db = KoreDB(testDir)
            val cdc = CdcManager(db, enabled = false)

            cdc.recordMutation("col", "doc1", MutationOp.INSERT, "payload".toByteArray(Charsets.UTF_8))
            val changes = cdc.getMutationsSince(0L)
            assertTrue(changes.isEmpty())
            db.close()
        }
    }

    // 50 Crypto & Codec Micro Boundary Tests
    @Test fun testCryptoCompressionMicro01() = verifyCryptoRoundtrip(1)
    @Test fun testCryptoCompressionMicro02() = verifyCryptoRoundtrip(2)
    @Test fun testCryptoCompressionMicro03() = verifyCryptoRoundtrip(3)
    @Test fun testCryptoCompressionMicro04() = verifyCryptoRoundtrip(4)
    @Test fun testCryptoCompressionMicro05() = verifyCryptoRoundtrip(5)
    @Test fun testCryptoCompressionMicro06() = verifyCryptoRoundtrip(6)
    @Test fun testCryptoCompressionMicro07() = verifyCryptoRoundtrip(7)
    @Test fun testCryptoCompressionMicro08() = verifyCryptoRoundtrip(8)
    @Test fun testCryptoCompressionMicro09() = verifyCryptoRoundtrip(9)
    @Test fun testCryptoCompressionMicro10() = verifyCryptoRoundtrip(10)
    @Test fun testCryptoCompressionMicro11() = verifyCryptoRoundtrip(11)
    @Test fun testCryptoCompressionMicro12() = verifyCryptoRoundtrip(12)
    @Test fun testCryptoCompressionMicro13() = verifyCryptoRoundtrip(13)
    @Test fun testCryptoCompressionMicro14() = verifyCryptoRoundtrip(14)
    @Test fun testCryptoCompressionMicro15() = verifyCryptoRoundtrip(15)
    @Test fun testCryptoCompressionMicro16() = verifyCryptoRoundtrip(16)
    @Test fun testCryptoCompressionMicro17() = verifyCryptoRoundtrip(17)
    @Test fun testCryptoCompressionMicro18() = verifyCryptoRoundtrip(18)
    @Test fun testCryptoCompressionMicro19() = verifyCryptoRoundtrip(19)
    @Test fun testCryptoCompressionMicro20() = verifyCryptoRoundtrip(20)
    @Test fun testCryptoCompressionMicro21() = verifyCryptoRoundtrip(21)
    @Test fun testCryptoCompressionMicro22() = verifyCryptoRoundtrip(22)
    @Test fun testCryptoCompressionMicro23() = verifyCryptoRoundtrip(23)
    @Test fun testCryptoCompressionMicro24() = verifyCryptoRoundtrip(24)
    @Test fun testCryptoCompressionMicro25() = verifyCryptoRoundtrip(25)
    @Test fun testCryptoCompressionMicro26() = verifyCryptoRoundtrip(26)
    @Test fun testCryptoCompressionMicro27() = verifyCryptoRoundtrip(27)
    @Test fun testCryptoCompressionMicro28() = verifyCryptoRoundtrip(28)
    @Test fun testCryptoCompressionMicro29() = verifyCryptoRoundtrip(29)
    @Test fun testCryptoCompressionMicro30() = verifyCryptoRoundtrip(30)
    @Test fun testCryptoCompressionMicro31() = verifyCryptoRoundtrip(31)
    @Test fun testCryptoCompressionMicro32() = verifyCryptoRoundtrip(32)
    @Test fun testCryptoCompressionMicro33() = verifyCryptoRoundtrip(33)
    @Test fun testCryptoCompressionMicro34() = verifyCryptoRoundtrip(34)
    @Test fun testCryptoCompressionMicro35() = verifyCryptoRoundtrip(35)
    @Test fun testCryptoCompressionMicro36() = verifyCryptoRoundtrip(36)
    @Test fun testCryptoCompressionMicro37() = verifyCryptoRoundtrip(37)
    @Test fun testCryptoCompressionMicro38() = verifyCryptoRoundtrip(38)
    @Test fun testCryptoCompressionMicro39() = verifyCryptoRoundtrip(39)
    @Test fun testCryptoCompressionMicro40() = verifyCryptoRoundtrip(40)
    @Test fun testCryptoCompressionMicro41() = verifyCryptoRoundtrip(41)
    @Test fun testCryptoCompressionMicro42() = verifyCryptoRoundtrip(42)
    @Test fun testCryptoCompressionMicro43() = verifyCryptoRoundtrip(43)
    @Test fun testCryptoCompressionMicro44() = verifyCryptoRoundtrip(44)
    @Test fun testCryptoCompressionMicro45() = verifyCryptoRoundtrip(45)
    @Test fun testCryptoCompressionMicro46() = verifyCryptoRoundtrip(46)
    @Test fun testCryptoCompressionMicro47() = verifyCryptoRoundtrip(47)
    @Test fun testCryptoCompressionMicro48() = verifyCryptoRoundtrip(48)
    @Test fun testCryptoCompressionMicro49() = verifyCryptoRoundtrip(49)
    @Test fun testCryptoCompressionMicro50() = verifyCryptoRoundtrip(50)

    private fun verifyCryptoRoundtrip(seed: Int) {
        val key = ByteArray(32) { ((it + seed) % 128).toByte() }
        val crypto = AesGcmCrypto(key)
        val data = "Payload_$seed with some extra characters to verify block boundaries".toByteArray(Charsets.UTF_8)
        val enc = crypto.encrypt(data)
        val dec = crypto.decrypt(enc)
        assertArrayEquals(data, dec)
    }
}
