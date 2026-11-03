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

import org.junit.Assert.*
import org.junit.Test
import java.util.Random

/**
 * Adversarial stress and edge-case verification for [OrderPreservingEncoder].
 *
 * Rigorously validates:
 * - Numerical boundary conditions: Long.MIN_VALUE, Long.MAX_VALUE, Int.MIN_VALUE, Int.MAX_VALUE
 * - Floating point anomalies: -0.0 vs +0.0, subnormals (MIN_VALUE), POSITIVE_INFINITY,
 *   NEGATIVE_INFINITY, canonical and non-canonical NaNs
 * - Strict monotonicity matching natural IEEE 754 and primitive integer ordering
 * - Round-trip encoding and decoding fidelity with offset safety
 * - Randomized property-based order invariance
 */
class OrderPreservingEncoderTest {

    @Test
    fun `test Long Boundary and Extreme Values Roundtrip and Order`() {
        val testLongs = longArrayOf(
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            Long.MIN_VALUE + 100,
            -1_000_000_000_000L,
            -1_000_000L,
            -100L,
            -2L,
            -1L,
            0L,
            1L,
            2L,
            100L,
            1_000_000L,
            1_000_000_000_000L,
            Long.MAX_VALUE - 100,
            Long.MAX_VALUE - 1,
            Long.MAX_VALUE
        )

        // 1. Roundtrip fidelity
        for (value in testLongs) {
            val encoded = OrderPreservingEncoder.encodeLong(value)
            assertEquals("Encoded long must be exactly 8 bytes", 8, encoded.size)
            val decoded = OrderPreservingEncoder.decodeLong(encoded)
            assertEquals("Roundtrip decoding failed for $value", value, decoded)
        }

        // 2. Pairwise monotonic ordering
        for (i in 0 until testLongs.size - 1) {
            val a = testLongs[i]
            val b = testLongs[i + 1]
            val encA = OrderPreservingEncoder.encodeLong(a)
            val encB = OrderPreservingEncoder.encodeLong(b)

            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Expected encoded($a) < encoded($b), but got cmp=$cmp", cmp < 0)
        }
    }

    @Test
    fun `test Double Numerical Edge Cases - Subnormals, Signs, Infinities, NaNs`() {
        // Strict IEEE 754 ordering:
        // -Inf < -MAX_VALUE < -1.0 < -MIN_VALUE (subnormal) < -0.0 < +0.0 < MIN_VALUE (subnormal) < 1.0 < MAX_VALUE < +Inf < NaN
        val orderedDoubles = doubleArrayOf(
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            -1.0e100,
            -1.0e10,
            -1.0,
            -1.0e-100,
            -java.lang.Double.MIN_NORMAL,
            -Double.MIN_VALUE,      // Smallest negative subnormal
            -0.0,
            0.0,
            Double.MIN_VALUE,       // Smallest positive subnormal (4.9E-324)
            java.lang.Double.MIN_NORMAL,      // Smallest positive normal
            1.0e-100,
            1.0,
            1.0e10,
            1.0e100,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NaN
        )

        // 1. Roundtrip verification
        for (value in orderedDoubles) {
            val encoded = OrderPreservingEncoder.encodeDouble(value)
            assertEquals("Encoded double must be exactly 8 bytes", 8, encoded.size)
            val decoded = OrderPreservingEncoder.decodeDouble(encoded)

            if (value.isNaN()) {
                assertTrue("Expected decoded NaN, but got $decoded", decoded.isNaN())
            } else if (value == 0.0) {
                // Verify signed zero preservation
                val expectedIsNeg = 1.0 / value == Double.NEGATIVE_INFINITY
                val decodedIsNeg = 1.0 / decoded == Double.NEGATIVE_INFINITY
                assertEquals("Signed zero sign mismatch for $value", expectedIsNeg, decodedIsNeg)
            } else {
                assertEquals("Roundtrip double failed for $value", java.lang.Double.doubleToLongBits(value), java.lang.Double.doubleToLongBits(decoded))
            }
        }

        // 2. Strict monotonicity check
        for (i in 0 until orderedDoubles.size - 1) {
            val a = orderedDoubles[i]
            val b = orderedDoubles[i + 1]
            val encA = OrderPreservingEncoder.encodeDouble(a)
            val encB = OrderPreservingEncoder.encodeDouble(b)

            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Expected encoded($a) < encoded($b), but got cmp=$cmp", cmp < 0)
        }

        // 3. Explicit check: -0.0 strictly precedes +0.0 in encoded bytes
        val encNegZero = OrderPreservingEncoder.encodeDouble(-0.0)
        val encPosZero = OrderPreservingEncoder.encodeDouble(0.0)
        assertTrue("-0.0 must be lexicographically smaller than 0.0",
            ByteArrayComparator.compare(encNegZero, encPosZero) < 0
        )

        // 4. Multiple NaN bit patterns must collapse to equal or canonical order
        val nan1 = Double.NaN
        val nan2 = java.lang.Double.longBitsToDouble(0x7ff0000000000001L) // Signaling/custom NaN
        val nan3 = java.lang.Double.longBitsToDouble(0xfff8000000000000UL.toLong()) // Negative NaN
        val encNan1 = OrderPreservingEncoder.encodeDouble(nan1)
        val encNan2 = OrderPreservingEncoder.encodeDouble(nan2)
        val encNan3 = OrderPreservingEncoder.encodeDouble(nan3)

        assertArrayEquals("All NaNs should produce identical canonical encodings", encNan1, encNan2)
        assertArrayEquals("Negative NaN should produce identical canonical encodings", encNan1, encNan3)

        // 5. Positive Infinity < NaN
        val encPosInf = OrderPreservingEncoder.encodeDouble(Double.POSITIVE_INFINITY)
        assertTrue("+Infinity must be strictly less than NaN",
            ByteArrayComparator.compare(encPosInf, encNan1) < 0
        )
    }

    @Test
    fun `test Float Numerical Edge Cases - Subnormals, Signs, Infinities, NaNs`() {
        val orderedFloats = floatArrayOf(
            Float.NEGATIVE_INFINITY,
            -Float.MAX_VALUE,
            -1000.0f,
            -1.0f,
            -Float.MIN_VALUE,      // Smallest negative subnormal
            -0.0f,
            0.0f,
            Float.MIN_VALUE,       // Smallest positive subnormal
            1.0f,
            1000.0f,
            Float.MAX_VALUE,
            Float.POSITIVE_INFINITY,
            Float.NaN
        )

        // 1. Roundtrip verification
        for (value in orderedFloats) {
            val encoded = OrderPreservingEncoder.encodeFloat(value)
            assertEquals("Encoded float must be 4 bytes", 4, encoded.size)
            val decoded = OrderPreservingEncoder.decodeFloat(encoded)

            if (value.isNaN()) {
                assertTrue("Expected decoded NaN, got $decoded", decoded.isNaN())
            } else if (value == 0.0f) {
                val expectedIsNeg = 1.0f / value == Float.NEGATIVE_INFINITY
                val decodedIsNeg = 1.0f / decoded == Float.NEGATIVE_INFINITY
                assertEquals("Signed zero mismatch for float $value", expectedIsNeg, decodedIsNeg)
            } else {
                assertEquals("Roundtrip float failed for $value", java.lang.Float.floatToIntBits(value), java.lang.Float.floatToIntBits(decoded))
            }
        }

        // 2. Strict monotonicity
        for (i in 0 until orderedFloats.size - 1) {
            val a = orderedFloats[i]
            val b = orderedFloats[i + 1]
            val encA = OrderPreservingEncoder.encodeFloat(a)
            val encB = OrderPreservingEncoder.encodeFloat(b)

            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Expected encoded($a) < encoded($b), but got cmp=$cmp", cmp < 0)
        }

        // 3. -0.0f < 0.0f
        val encNegZero = OrderPreservingEncoder.encodeFloat(-0.0f)
        val encPosZero = OrderPreservingEncoder.encodeFloat(0.0f)
        assertTrue("-0.0f must be less than 0.0f",
            ByteArrayComparator.compare(encNegZero, encPosZero) < 0
        )
    }

    @Test
    fun `test Int Boundary and Extreme Values Roundtrip and Order`() {
        val testInts = intArrayOf(
            Int.MIN_VALUE,
            Int.MIN_VALUE + 1,
            -100_000,
            -1,
            0,
            1,
            100_000,
            Int.MAX_VALUE - 1,
            Int.MAX_VALUE
        )

        for (value in testInts) {
            val encoded = OrderPreservingEncoder.encodeInt(value)
            assertEquals("Encoded int must be 4 bytes", 4, encoded.size)
            val decoded = OrderPreservingEncoder.decodeInt(encoded)
            assertEquals("Roundtrip int failed for $value", value, decoded)
        }

        for (i in 0 until testInts.size - 1) {
            val a = testInts[i]
            val b = testInts[i + 1]
            val encA = OrderPreservingEncoder.encodeInt(a)
            val encB = OrderPreservingEncoder.encodeInt(b)

            val cmp = ByteArrayComparator.compare(encA, encB)
            assertTrue("Expected encoded($a) < encoded($b), got cmp=$cmp", cmp < 0)
        }
    }

    @Test
    fun `test Offset Decoding Safety`() {
        val prefix = byteArrayOf(0x01, 0x02, 0x03, 0x04)
        val suffix = byteArrayOf(0x09, 0x0A)

        // Test Long with offset
        val testLong = -12345678901234567L
        val encLong = OrderPreservingEncoder.encodeLong(testLong)
        val bufferLong = prefix + encLong + suffix
        val decodedLong = OrderPreservingEncoder.decodeLong(bufferLong, offset = prefix.size)
        assertEquals("Offset decodeLong mismatch", testLong, decodedLong)

        // Test Double with offset
        val testDouble = -987.654321
        val encDouble = OrderPreservingEncoder.encodeDouble(testDouble)
        val bufferDouble = prefix + encDouble + suffix
        val decodedDouble = OrderPreservingEncoder.decodeDouble(bufferDouble, offset = prefix.size)
        assertEquals("Offset decodeDouble mismatch", testDouble, decodedDouble, 0.0)

        // Test Int with offset
        val testInt = -54321
        val encInt = OrderPreservingEncoder.encodeInt(testInt)
        val bufferInt = prefix + encInt + suffix
        val decodedInt = OrderPreservingEncoder.decodeInt(bufferInt, offset = prefix.size)
        assertEquals("Offset decodeInt mismatch", testInt, decodedInt)

        // Test Float with offset
        val testFloat = -43.21f
        val encFloat = OrderPreservingEncoder.encodeFloat(testFloat)
        val bufferFloat = prefix + encFloat + suffix
        val decodedFloat = OrderPreservingEncoder.decodeFloat(bufferFloat, offset = prefix.size)
        assertEquals("Offset decodeFloat mismatch", testFloat, decodedFloat, 0.0f)
    }

    @Test
    fun `test Property Based Randomized Invariant for Longs`() {
        val random = Random(42)
        val count = 5000
        val original = LongArray(count) { random.nextLong() }

        // Sort originals
        val sortedOriginals = original.clone()
        sortedOriginals.sort()

        // Encode and sort by ByteArrayComparator
        val encodedList = original.map { OrderPreservingEncoder.encodeLong(it) }
        val sortedEncoded = encodedList.sortedWith(ByteArrayComparator)

        // Decode back
        val decodedBack = sortedEncoded.map { OrderPreservingEncoder.decodeLong(it) }

        for (i in 0 until count) {
            assertEquals("Order mismatch at index $i", sortedOriginals[i], decodedBack[i])
        }
    }

    @Test
    fun `test Property Based Randomized Invariant for Doubles`() {
        val random = Random(1337)
        val count = 5000
        val original = DoubleArray(count) {
            when (random.nextInt(10)) {
                0 -> Double.NaN
                1 -> Double.POSITIVE_INFINITY
                2 -> Double.NEGATIVE_INFINITY
                3 -> 0.0
                4 -> -0.0
                5 -> Double.MIN_VALUE
                6 -> -Double.MIN_VALUE
                7 -> random.nextDouble() * 1e200
                8 -> -random.nextDouble() * 1e200
                else -> (random.nextDouble() - 0.5) * 1000.0
            }
        }

        // Sort using Java Double.compare
        val sortedOriginals = original.toTypedArray()
        sortedOriginals.sortWith { a, b -> java.lang.Double.compare(a, b) }

        // Encode and sort by ByteArrayComparator
        val encodedList = original.map { OrderPreservingEncoder.encodeDouble(it) }
        val sortedEncoded = encodedList.sortedWith(ByteArrayComparator)

        // Decode back
        val decodedBack = sortedEncoded.map { OrderPreservingEncoder.decodeDouble(it) }

        for (i in 0 until count) {
            val expected = sortedOriginals[i]
            val actual = decodedBack[i]
            val cmp = java.lang.Double.compare(expected, actual)
            assertEquals("Double order mismatch at index $i (expected=$expected, actual=$actual)", 0, cmp)
        }
    }
}
