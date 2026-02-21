/*
 * Copyright 2024 KoreDB Authors
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

package com.pankaj.koredb.foundation

import java.util.BitSet
import kotlin.math.abs

/**
 * A probabilistic data structure used to test whether an element is a member of a set.
 *
 * In KoreDB, the Bloom Filter is used to optimize read operations by quickly determining
 * if a key might exist in an SSTable. False positive matches are possible, but false
 * negatives are not. This reduces unnecessary disk I/O for keys that definitely do not exist.
 *
 * @property bitSize The number of bits in the filter.
 * @property hashFunctions The number of hash functions to use.
 * @property bitSet The underlying storage for the bits.
 */
class BloomFilter(
    val bitSize: Int,
    val hashFunctions: Int,
    private val bitSet: BitSet = BitSet(bitSize)
) {

    /**
     * Adds a key to the Bloom Filter.
     *
     * @param key The byte array representing the key to add.
     */
    fun add(key: ByteArray) {
        val hashes = getHashes(key)
        for (hash in hashes) {
            bitSet.set(hash)
        }
    }

    /**
     * Checks if a key might be present in the filter.
     *
     * @param key The byte array representing the key to check.
     * @return true if the key might be present, false if it definitely is not.
     */
    fun mightContain(key: ByteArray): Boolean {
        val hashes = getHashes(key)
        for (hash in hashes) {
            if (!bitSet.get(hash)) {
                return false
            }
        }
        return true
    }

    /**
     * Generates multiple hash indices for a given key using double hashing.
     */
    private fun getHashes(key: ByteArray): IntArray {
        val result = IntArray(hashFunctions)
        var hash1 = 5381
        var hash2 = 0

        // DJB2-like hashing for hash1 and a simple shift-based hash for hash2
        for (b in key) {
            hash1 = ((hash1 shl 5) + hash1) xor b.toInt()
            hash2 = hash2 * 31 + b.toInt()
        }

        for (i in 0 until hashFunctions) {
            val combinedHash = abs(hash1 + (i * hash2))
            result[i] = combinedHash % bitSize
        }
        return result
    }

    /**
     * Serializes the Bloom Filter's bit set to a raw [ByteArray].
     *
     * @return The serialized byte representation of the bit set.
     */
    fun toByteArray(): ByteArray {
        return bitSet.toByteArray()
    }

    companion object {
        /**
         * Reconstructs a [BloomFilter] from a raw byte array.
         *
         * @param bitSize The original bit size used when creating the filter.
         * @param hashFunctions The original number of hash functions used.
         * @param bytes The serialized byte array.
         * @return A reconstructed [BloomFilter] instance.
         */
        fun fromByteArray(bitSize: Int, hashFunctions: Int, bytes: ByteArray): BloomFilter {
            return BloomFilter(bitSize, hashFunctions, BitSet.valueOf(bytes))
        }
    }
}
