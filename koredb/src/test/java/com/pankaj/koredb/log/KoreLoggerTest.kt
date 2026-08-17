package com.pankaj.koredb.log

import com.pankaj.koredb.engine.CorruptionException
import com.pankaj.koredb.engine.KoreDBException
import org.junit.Assert.*
import org.junit.Test

class KoreLoggerTest {

    @Test
    fun `test Custom Logger Delegation`() {
        val capturedLogs = mutableListOf<String>()
        val customLogger = object : KoreLogger {
            override fun debug(message: String) { capturedLogs.add("DEBUG: $message") }
            override fun info(message: String) { capturedLogs.add("INFO: $message") }
            override fun warn(message: String, throwable: Throwable?) { capturedLogs.add("WARN: $message") }
            override fun error(message: String, throwable: Throwable?) { capturedLogs.add("ERROR: $message") }
        }

        val originalFactory = KoreLogger.factory
        try {
            KoreLogger.factory = { customLogger }
            val logger = KoreLogger.getLogger("TestTag")

            logger.info("Database initialized")
            logger.warn("Slow query detected")
            logger.error("Failed to write segment", RuntimeException("Disk full"))

            assertEquals(3, capturedLogs.size)
            assertEquals("INFO: Database initialized", capturedLogs[0])
            assertEquals("WARN: Slow query detected", capturedLogs[1])
            assertEquals("ERROR: Failed to write segment", capturedLogs[2])
        } finally {
            KoreLogger.factory = originalFactory
        }
    }

    @Test
    fun `test Exception Hierarchy`() {
        val ex: Exception = CorruptionException("SSTable corrupted")
        assertTrue(ex is KoreDBException)
        assertEquals("SSTable corrupted", ex.message)
    }
}
