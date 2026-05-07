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

package com.pankaj.koredb.foundation

import java.util.Comparator

/**
 * A highly optimized comparator for sorting [ByteArray] keys.
 *
 * This comparator performs an unsigned lexicographical comparison using
 * 8-byte-at-a-time Long reads with an XOR-based fast path. For keys sharing
 * long common prefixes (common in KoreDB's key schema like "doc:collection:"),
 * this skips matching bytes 8x faster than byte-by-byte comparison.
 */
object ByteArrayComparator : Comparator<ByteArray> {
    
    /**
     * Compares two byte arrays lexicographically using unsigned byte values.
     *
     * Uses a word-at-a-time strategy: reads 8 bytes as a Long, XORs them,
     * and uses [Long.numberOfLeadingZeros] to locate the first differing byte.
     *
     * @param left The first byte array to compare.
     * @param right The second byte array to compare.
     * @return A negative integer, zero, or a positive integer as the first argument
     *         is less than, equal to, or greater than the second.
     */
    override fun compare(left: ByteArray, right: ByteArray): Int {
        val minLength = minOf(left.size, right.size)

        // Fast path: compare 8 bytes at a time for keys with long shared prefixes
        var i = 0
        val longLimit = minLength - 7
        while (i < longLimit) {
            val lv = readLong(left, i)
            val rv = readLong(right, i)
            if (lv != rv) {
                // XOR reveals differing bits; leading zeros locate the first differing byte
                val xor = lv xor rv
                val diffByteIndex = java.lang.Long.numberOfLeadingZeros(xor) ushr 3
                val a = left[i + diffByteIndex].toInt() and 0xFF
                val b = right[i + diffByteIndex].toInt() and 0xFF
                return a - b
            }
            i += 8
        }

        // Remaining bytes: compare one by one
        while (i < minLength) {
            val a = left[i].toInt() and 0xFF
            val b = right[i].toInt() and 0xFF
            if (a != b) return a - b
            i++
        }
        return left.size - right.size
    }

    /**
     * Reads 8 bytes from [data] at [offset] as a big-endian Long.
     */
    private fun readLong(data: ByteArray, offset: Int): Long {
        return ((data[offset].toLong() and 0xFF) shl 56) or
               ((data[offset + 1].toLong() and 0xFF) shl 48) or
               ((data[offset + 2].toLong() and 0xFF) shl 40) or
               ((data[offset + 3].toLong() and 0xFF) shl 32) or
               ((data[offset + 4].toLong() and 0xFF) shl 24) or
               ((data[offset + 5].toLong() and 0xFF) shl 16) or
               ((data[offset + 6].toLong() and 0xFF) shl 8) or
               (data[offset + 7].toLong() and 0xFF)
    }
}
