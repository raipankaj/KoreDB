package com.pankaj.koredb.foundation

/**
 * Utility for encoding numeric primitives (Int, Long, Float, Double) into raw byte arrays
 * such that the natural lexicographical byte comparison order (as performed by [ByteArrayComparator])
 * exactly mirrors the numerical order.
 *
 * This enables O(log N) binary range queries directly on LSM-Tree and SSTable indices.
 */
object OrderPreservingEncoder {

    // ========================================================================
    // INT (32-bit signed integer)
    // ========================================================================

    fun encodeInt(value: Int): ByteArray {
        val flipped = value xor Int.MIN_VALUE
        return byteArrayOf(
            (flipped ushr 24).toByte(),
            (flipped ushr 16).toByte(),
            (flipped ushr 8).toByte(),
            flipped.toByte()
        )
    }

    fun decodeInt(bytes: ByteArray, offset: Int = 0): Int {
        val raw = ((bytes[offset].toInt() and 0xFF) shl 24) or
                ((bytes[offset + 1].toInt() and 0xFF) shl 16) or
                ((bytes[offset + 2].toInt() and 0xFF) shl 8) or
                (bytes[offset + 3].toInt() and 0xFF)
        return raw xor Int.MIN_VALUE
    }

    // ========================================================================
    // LONG (64-bit signed integer)
    // ========================================================================

    fun encodeLong(value: Long): ByteArray {
        val flipped = value xor Long.MIN_VALUE
        return byteArrayOf(
            (flipped ushr 56).toByte(),
            (flipped ushr 48).toByte(),
            (flipped ushr 40).toByte(),
            (flipped ushr 32).toByte(),
            (flipped ushr 24).toByte(),
            (flipped ushr 16).toByte(),
            (flipped ushr 8).toByte(),
            flipped.toByte()
        )
    }

    fun decodeLong(bytes: ByteArray, offset: Int = 0): Long {
        val raw = ((bytes[offset].toLong() and 0xFFL) shl 56) or
                ((bytes[offset + 1].toLong() and 0xFFL) shl 48) or
                ((bytes[offset + 2].toLong() and 0xFFL) shl 40) or
                ((bytes[offset + 3].toLong() and 0xFFL) shl 32) or
                ((bytes[offset + 4].toLong() and 0xFFL) shl 24) or
                ((bytes[offset + 5].toLong() and 0xFFL) shl 16) or
                ((bytes[offset + 6].toLong() and 0xFFL) shl 8) or
                (bytes[offset + 7].toLong() and 0xFFL)
        return raw xor Long.MIN_VALUE
    }

    // ========================================================================
    // FLOAT (32-bit IEEE 754 floating point)
    // ========================================================================

    fun encodeFloat(value: Float): ByteArray {
        val bits = java.lang.Float.floatToIntBits(value)
        val mask = if (bits < 0) -1 else Int.MIN_VALUE
        val encoded = bits xor mask
        return byteArrayOf(
            (encoded ushr 24).toByte(),
            (encoded ushr 16).toByte(),
            (encoded ushr 8).toByte(),
            encoded.toByte()
        )
    }

    fun decodeFloat(bytes: ByteArray, offset: Int = 0): Float {
        val encoded = ((bytes[offset].toInt() and 0xFF) shl 24) or
                ((bytes[offset + 1].toInt() and 0xFF) shl 16) or
                ((bytes[offset + 2].toInt() and 0xFF) shl 8) or
                (bytes[offset + 3].toInt() and 0xFF)
        val mask = if ((encoded and Int.MIN_VALUE) != 0) Int.MIN_VALUE else -1
        val bits = encoded xor mask
        return java.lang.Float.intBitsToFloat(bits)
    }

    // ========================================================================
    // DOUBLE (64-bit IEEE 754 floating point)
    // ========================================================================

    fun encodeDouble(value: Double): ByteArray {
        val bits = java.lang.Double.doubleToLongBits(value)
        val mask = if (bits < 0) -1L else Long.MIN_VALUE
        val encoded = bits xor mask
        return byteArrayOf(
            (encoded ushr 56).toByte(),
            (encoded ushr 48).toByte(),
            (encoded ushr 40).toByte(),
            (encoded ushr 32).toByte(),
            (encoded ushr 24).toByte(),
            (encoded ushr 16).toByte(),
            (encoded ushr 8).toByte(),
            encoded.toByte()
        )
    }

    fun decodeDouble(bytes: ByteArray, offset: Int = 0): Double {
        val encoded = ((bytes[offset].toLong() and 0xFFL) shl 56) or
                ((bytes[offset + 1].toLong() and 0xFFL) shl 48) or
                ((bytes[offset + 2].toLong() and 0xFFL) shl 40) or
                ((bytes[offset + 3].toLong() and 0xFFL) shl 32) or
                ((bytes[offset + 4].toLong() and 0xFFL) shl 24) or
                ((bytes[offset + 5].toLong() and 0xFFL) shl 16) or
                ((bytes[offset + 6].toLong() and 0xFFL) shl 8) or
                (bytes[offset + 7].toLong() and 0xFFL)
        val mask = if ((encoded and Long.MIN_VALUE) != 0L) Long.MIN_VALUE else -1L
        val bits = encoded xor mask
        return java.lang.Double.longBitsToDouble(bits)
    }
}
