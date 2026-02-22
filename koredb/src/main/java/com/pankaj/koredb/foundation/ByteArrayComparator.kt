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
 * This comparator performs an unsigned lexicographical comparison, which is
 * essential for maintaining strict key order in the [MemTable] and SSTables.
 * This ordering ensures that range scans and merges work correctly.
 */
object ByteArrayComparator : Comparator<ByteArray> {
    
    /**
     * Compares two byte arrays lexicographically using unsigned byte values.
     *
     * @param left The first byte array to compare.
     * @param right The second byte array to compare.
     * @return A negative integer, zero, or a positive integer as the first argument
     *         is less than, equal to, or greater than the second.
     */
    override fun compare(left: ByteArray, right: ByteArray): Int {
        val minLength = minOf(left.size, right.size)
        for (i in 0 until minLength) {
            // Convert to unsigned int for proper lexicographical byte comparison
            val a = left[i].toInt() and 0xFF
            val b = right[i].toInt() and 0xFF
            if (a != b) {
                return a.compareTo(b)
            }
        }
        return left.size.compareTo(right.size)
    }
}
