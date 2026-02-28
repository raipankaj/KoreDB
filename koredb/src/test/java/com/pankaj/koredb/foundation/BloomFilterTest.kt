package com.pankaj.koredb.foundation

import org.junit.Assert.*
import org.junit.Test

class BloomFilterTest {

    @Test
    fun `test Bloom Filter Membership`() {
        val filter = BloomFilter(1000, 3)
        
        val key1 = "user:1".toByteArray()
        val key2 = "user:2".toByteArray()
        val key3 = "user:3".toByteArray() // Not added

        filter.add(key1)
        filter.add(key2)

        assertTrue(filter.mightContain(key1))
        assertTrue(filter.mightContain(key2))
        
        // Bloom Filters can have false positives, but for small sets, it's often accurate.
        // We can't strictly assert assertFalse for key3 unless we know hash collisions won't happen.
        // But for testing purposes, likely false.
    }

    @Test
    fun `test Serialization and Deserialization`() {
        val original = BloomFilter(100, 5)
        original.add("test".toByteArray())

        val bytes = original.toByteArray()
        
        // Reconstruct
        val restored = BloomFilter.fromByteArray(original.bitSize, original.hashFunctions, bytes)

        assertTrue("Restored filter should contain key", restored.mightContain("test".toByteArray()))
        assertEquals("Bit size mismatch", original.bitSize, restored.bitSize)
        assertEquals("Hash function count mismatch", original.hashFunctions, restored.hashFunctions)
    }
}
