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

package com.pankaj.koredb.crypto

import com.pankaj.koredb.db.KoreDatabase
import com.pankaj.koredb.engine.CorruptionException
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.UUID

@Serializable
data class SecretNote(
    val id: String,
    val title: String,
    val secretContent: String
)

class EncryptionTest {

    private lateinit var testDir: File
    private lateinit var key: ByteArray
    private lateinit var crypto: AesGcmCrypto

    @Before
    fun setup() {
        testDir = File("build/tmp/test_encryption_${UUID.randomUUID()}")
        testDir.mkdirs()
        key = AesGcmCrypto.generateKey()
        crypto = AesGcmCrypto(key)
    }

    @After
    fun tearDown() {
        testDir.deleteRecursively()
    }

    @Test
    fun testDirectEncryptionAndDecryption() {
        val plaintext = "KoreDB Enterprise Security".toByteArray(Charsets.UTF_8)
        val aad = "note:123".toByteArray(Charsets.UTF_8)

        val encrypted = crypto.encrypt(plaintext, aad)
        assertFalse("Ciphertext should differ from plaintext", plaintext.contentEquals(encrypted))

        val decrypted = crypto.decrypt(encrypted, aad)
        assertArrayEquals(plaintext, decrypted)
    }

    @Test
    fun testDirectDecryptionWithWrongAADThrows() {
        val plaintext = "Sensitive Financial Data".toByteArray(Charsets.UTF_8)
        val aad = "account:1001".toByteArray(Charsets.UTF_8)
        val wrongAad = "account:1002".toByteArray(Charsets.UTF_8)

        val encrypted = crypto.encrypt(plaintext, aad)

        try {
            crypto.decrypt(encrypted, wrongAad)
            fail("Expected CorruptionException due to AAD mismatch")
        } catch (e: CorruptionException) {
            assertTrue(e.message!!.contains("Decryption / AEAD authentication failed"))
        }
    }

    @Test
    fun testDatabaseEncryptionAtRest() = runBlocking {
        val db = KoreDatabase(testDir, crypto = crypto)
        val notes = db.collection<SecretNote>("secrets")

        val note = SecretNote("note_1", "Password", "super_secret_classified_token_xyz")
        notes.insert("note_1", note)

        // Force flush to SSTable
        db.engine.flushMemTableInternal()

        // 1. Read through database: should decrypt seamlessly
        val retrieved = notes.getById("note_1")
        assertNotNull(retrieved)
        assertEquals("super_secret_classified_token_xyz", retrieved?.secretContent)

        // 2. Read raw SSTable on disk: plaintext should NOT appear anywhere
        val sstFiles = testDir.listFiles { _, name -> name.endsWith(".sst") }
        assertNotNull(sstFiles)
        assertTrue(sstFiles!!.isNotEmpty())

        val allBytesOnDisk = sstFiles.flatMap { it.readBytes().toList() }.toByteArray()
        val diskContentString = String(allBytesOnDisk, Charsets.ISO_8859_1)
        assertFalse(
            "Disk SSTable should not contain secret plaintext",
            diskContentString.contains("super_secret_classified_token_xyz")
        )

        db.close()
    }

    @Test
    fun testWrongKeyCannotReadEncryptedDatabase() = runBlocking {
        val db1 = KoreDatabase(testDir, crypto = crypto)
        val notes1 = db1.collection<SecretNote>("secrets")
        notes1.insert("note_1", SecretNote("note_1", "Secret", "classified_info"))
        db1.engine.flushMemTableInternal()
        db1.close()

        // Attempt reading with a different key
        val wrongKey = AesGcmCrypto.generateKey()
        val wrongCrypto = AesGcmCrypto(wrongKey)
        val db2 = KoreDatabase(testDir, crypto = wrongCrypto)
        val notes2 = db2.collection<SecretNote>("secrets")

        try {
            notes2.getById("note_1")
            fail("Expected CorruptionException when reading with wrong key")
        } catch (e: CorruptionException) {
            assertTrue(e.message!!.contains("Decryption / AEAD authentication failed"))
        } finally {
            db2.close()
        }
    }
}
