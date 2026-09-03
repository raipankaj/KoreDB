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

package com.pankaj.koredb.compression

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.Deflater
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.Inflater

/**
 * Interface for pluggable block and payload compression in SSTables and storage engines.
 */
interface CompressionCodec {
    /**
     * Unique 1-byte codec identifier stored in binary SSTable headers.
     */
    val type: Byte

    /**
     * Compresses the input byte array.
     */
    fun compress(data: ByteArray): ByteArray

    /**
     * Decompresses the compressed byte array back to original plaintext.
     */
    fun decompress(data: ByteArray): ByteArray

    companion object {
        const val TYPE_NONE: Byte = 0
        const val TYPE_DEFLATE: Byte = 1
        const val TYPE_GZIP: Byte = 2
        const val TYPE_LZ4: Byte = 3

        fun fromType(type: Byte): CompressionCodec {
            return when (type) {
                TYPE_NONE -> NoOpCompressionCodec
                TYPE_DEFLATE -> DeflateCompressionCodec()
                TYPE_GZIP -> GzipCompressionCodec
                TYPE_LZ4 -> Lz4CompressionCodec()
                else -> NoOpCompressionCodec
            }
        }
    }
}

/**
 * Passthrough uncompressed codec with zero overhead.
 */
object NoOpCompressionCodec : CompressionCodec {
    override val type: Byte = CompressionCodec.TYPE_NONE
    override fun compress(data: ByteArray): ByteArray = data
    override fun decompress(data: ByteArray): ByteArray = data
}

/**
 * High-performance DEFLATE compression codec with ThreadLocal zlib reuse.
 */
class DeflateCompressionCodec(
    private val level: Int = Deflater.DEFAULT_COMPRESSION
) : CompressionCodec {

    override val type: Byte = CompressionCodec.TYPE_DEFLATE

    private val deflaterThreadLocal = ThreadLocal.withInitial { Deflater(level) }
    private val inflaterThreadLocal = ThreadLocal.withInitial { Inflater() }

    override fun compress(data: ByteArray): ByteArray {
        if (data.isEmpty()) return data
        val deflater = deflaterThreadLocal.get()
        deflater.reset()
        deflater.setInput(data)
        deflater.finish()

        val outputStream = ByteArrayOutputStream(maxOf(64, data.size / 2))
        val buffer = ByteArray(4096)
        while (!deflater.finished()) {
            val count = deflater.deflate(buffer)
            outputStream.write(buffer, 0, count)
        }
        return outputStream.toByteArray()
    }

    override fun decompress(data: ByteArray): ByteArray {
        if (data.isEmpty()) return data
        val inflater = inflaterThreadLocal.get()
        inflater.reset()
        inflater.setInput(data)

        val outputStream = ByteArrayOutputStream(maxOf(64, data.size * 2))
        val buffer = ByteArray(4096)
        while (!inflater.finished()) {
            val count = inflater.inflate(buffer)
            if (count == 0 && inflater.needsInput()) break
            outputStream.write(buffer, 0, count)
        }
        return outputStream.toByteArray()
    }
}

/**
 * Standard GZIP compression codec.
 */
object GzipCompressionCodec : CompressionCodec {
    override val type: Byte = CompressionCodec.TYPE_GZIP

    override fun compress(data: ByteArray): ByteArray {
        if (data.isEmpty()) return data
        val bos = ByteArrayOutputStream(data.size)
        GZIPOutputStream(bos).use { gzip ->
            gzip.write(data)
        }
        return bos.toByteArray()
    }

    override fun decompress(data: ByteArray): ByteArray {
        if (data.isEmpty()) return data
        val bis = ByteArrayInputStream(data)
        val bos = ByteArrayOutputStream(data.size * 2)
        GZIPInputStream(bis).use { gzip ->
            val buffer = ByteArray(4096)
            var len: Int
            while (gzip.read(buffer).also { len = it } != -1) {
                bos.write(buffer, 0, len)
            }
        }
        return bos.toByteArray()
    }
}
