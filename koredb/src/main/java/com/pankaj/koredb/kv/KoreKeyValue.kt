package com.pankaj.koredb.kv

import com.pankaj.koredb.engine.KoreDB
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * A fast, persistent Key-Value store built directly on top of the KoreDB LSM-Tree.
 * This bypasses the document serializer and JSON parsing, offering the highest
 * possible performance for caching primitive data, sessions, or byte arrays.
 */
class KoreKeyValue(
    private val name: String,
    private val engine: KoreDB
) {
    private val prefix = "kv_$name:".toByteArray(Charsets.UTF_8)

    private fun escape(value: String): String {
        return value.replace("%", "%25").replace(":", "%3A")
    }

    private fun constructKey(key: String): ByteArray {
        val keyBytes = escape(key).toByteArray(Charsets.UTF_8)
        val fullKey = ByteArray(prefix.size + keyBytes.size)
        System.arraycopy(prefix, 0, fullKey, 0, prefix.size)
        System.arraycopy(keyBytes, 0, fullKey, prefix.size, keyBytes.size)
        return fullKey
    }

    /**
     * Persists a raw byte array.
     */
    suspend fun put(key: String, value: ByteArray) = withContext(Dispatchers.IO) {
        engine.putRaw(constructKey(key), value)
    }

    /**
     * Persists a UTF-8 String.
     */
    suspend fun putString(key: String, value: String) {
        put(key, value.toByteArray(Charsets.UTF_8))
    }

    /**
     * Retrieves a raw byte array, or null if it does not exist.
     */
    fun get(key: String): ByteArray? {
        return engine.getRaw(constructKey(key))
    }

    /**
     * Retrieves a UTF-8 String, or null if it does not exist.
     */
    fun getString(key: String): String? {
        return get(key)?.toString(Charsets.UTF_8)
    }

    /**
     * Deletes the key-value pair.
     */
    suspend fun delete(key: String) = withContext(Dispatchers.IO) {
        engine.deleteRaw(constructKey(key))
    }
}
