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

import com.pankaj.koredb.engine.CorruptionException
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * Interface for database encryption at rest.
 */
interface KoreCrypto {
    /**
     * Encrypts plaintext bytes with optional associated authenticated data (AAD).
     */
    fun encrypt(plainText: ByteArray, aad: ByteArray? = null): ByteArray

    /**
     * Decrypts ciphertext bytes with optional associated authenticated data (AAD).
     * @throws CorruptionException if ciphertext is tampered or authentication fails.
     */
    fun decrypt(cipherText: ByteArray, aad: ByteArray? = null): ByteArray
}

/**
 * High-performance Enterprise AES-256-GCM encryption provider.
 *
 * Optimizations:
 * - Reuses [Cipher] instances per thread via [ThreadLocal] to eliminate JCA provider lookup overhead.
 * - Uses zero-copy offset encryption/decryption to eliminate intermediate byte array allocations.
 *
 * Format of encrypted payload:
 * [12-byte random IV] [Ciphertext + 16-byte GCM authentication tag]
 */
class AesGcmCrypto(
    private val key: ByteArray
) : KoreCrypto {

    init {
        require(key.size == 16 || key.size == 24 || key.size == 32) {
            "AES key must be 16, 24, or 32 bytes (128, 192, or 256 bits). Provided: ${key.size} bytes."
        }
    }

    private val secretKeySpec = SecretKeySpec(key, "AES")
    private val secureRandom = SecureRandom()

    private val cipherThreadLocal = ThreadLocal.withInitial {
        Cipher.getInstance(AES_GCM_TRANSFORMATION)
    }

    override fun encrypt(plainText: ByteArray, aad: ByteArray?): ByteArray {
        val iv = ByteArray(GCM_IV_LENGTH_BYTES)
        secureRandom.nextBytes(iv)

        val cipher = cipherThreadLocal.get()
        val spec = GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv)
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, spec)

        if (aad != null && aad.isNotEmpty()) {
            cipher.updateAAD(aad)
        }

        // Direct single-allocation payload: [IV (12B)] [Ciphertext + Tag (plainText.size + 16B)]
        val outputSize = GCM_IV_LENGTH_BYTES + cipher.getOutputSize(plainText.size)
        val result = ByteArray(outputSize)
        System.arraycopy(iv, 0, result, 0, GCM_IV_LENGTH_BYTES)

        val written = cipher.doFinal(plainText, 0, plainText.size, result, GCM_IV_LENGTH_BYTES)
        return if (GCM_IV_LENGTH_BYTES + written == result.size) {
            result
        } else {
            result.copyOf(GCM_IV_LENGTH_BYTES + written)
        }
    }

    override fun decrypt(cipherText: ByteArray, aad: ByteArray?): ByteArray {
        if (cipherText.size < GCM_IV_LENGTH_BYTES + GCM_TAG_LENGTH_BYTES) {
            throw CorruptionException("Encrypted payload too short: ${cipherText.size} bytes")
        }

        val cipher = cipherThreadLocal.get()
        val spec = GCMParameterSpec(GCM_TAG_LENGTH_BITS, cipherText, 0, GCM_IV_LENGTH_BYTES)

        try {
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, spec)

            if (aad != null && aad.isNotEmpty()) {
                cipher.updateAAD(aad)
            }

            // Direct offset decryption without allocating an intermediate cipherData ByteArray
            val cipherDataLength = cipherText.size - GCM_IV_LENGTH_BYTES
            return cipher.doFinal(cipherText, GCM_IV_LENGTH_BYTES, cipherDataLength)
        } catch (e: Exception) {
            throw CorruptionException("Decryption / AEAD authentication failed. Data may be corrupted or tampered.", e)
        }
    }

    companion object {
        private const val AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding"
        private const val GCM_IV_LENGTH_BYTES = 12
        private const val GCM_TAG_LENGTH_BITS = 128
        private const val GCM_TAG_LENGTH_BYTES = 16

        /**
         * Helper to generate a cryptographically secure 256-bit (32-byte) key.
         */
        fun generateKey(): ByteArray {
            val key = ByteArray(32)
            SecureRandom().nextBytes(key)
            return key
        }
    }
}
