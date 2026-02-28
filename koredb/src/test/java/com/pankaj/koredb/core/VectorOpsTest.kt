package com.pankaj.koredb.core

import org.junit.Assert.*
import org.junit.Test
import java.nio.ByteBuffer

class VectorOpsTest {

    @Test
    fun `test Magnitude Calculation`() {
        // [3, 4] -> Magnitude = sqrt(9 + 16) = 5
        val vec = floatArrayOf(3f, 4f)
        val mag = VectorMath.getMagnitude(vec)
        assertEquals(5f, mag, 0.0001f)
    }

    @Test
    fun `test Serialization Integrity`() {
        val original = floatArrayOf(1.23f, -4.56f, 0.0f, Float.MAX_VALUE)
        val bytes = VectorSerializer.toByteArray(original)
        val restored = VectorSerializer.fromByteArray(bytes)

        assertEquals(original.size, restored.size)
        assertArrayEquals(original, restored, 0.0001f)
    }

    @Test
    fun `test Cosine Similarity Calculation`() {
        val vA = floatArrayOf(1f, 0f)
        val vB = floatArrayOf(0f, 1f)
        
        val magA = VectorMath.getMagnitude(vA)
        
        // Wrap vB in ByteBuffer to simulate storage
        val buffer = ByteBuffer.allocate(8)
        buffer.putFloat(0f)
        buffer.putFloat(1f)
        buffer.flip()

        val score = VectorMath.cosineSimilarity(vA, magA, buffer, 0, 2) // length 2
        assertEquals(0f, score, 0.0001f)
    }
}
