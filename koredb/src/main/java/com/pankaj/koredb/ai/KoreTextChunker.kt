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

package com.pankaj.koredb.ai

/**
 * Represents a boundary-aligned text chunk produced by [KoreTextChunker].
 *
 * @property index 0-based sequential position of the chunk within the parent document.
 * @property text The chunk text content.
 * @property startOffset Inclusive character start index in the normalized source document.
 * @property endOffset Exclusive character end index in the normalized source document.
 * @property estimatedTokens Estimated subword token count for on-device SLM context budgeting.
 */
data class TextChunk(
    val index: Int,
    val text: String,
    val startOffset: Int,
    val endOffset: Int,
    val estimatedTokens: Int
)

/**
 * Boundary-aware recursive text chunker tailored for On-Device RAG and Small Language Models (SLMs).
 *
 * Splits long documents into overlapping chunks while respecting natural semantic boundaries
 * in priority order:
 * 1. Paragraph breaks (`\n\n`)
 * 2. Line breaks (`\n`)
 * 3. Sentence endings (`. `, `! `, `? `, `.\n`)
 * 4. Clause/word boundaries (`, `, `; `, ` `)
 * 5. Hard character boundary fallback
 */
object KoreTextChunker {

    /**
     * Fast, allocation-free token estimator for English/multilingual BPE/SentencePiece tokenizers
     * (e.g., Gemini Nano / Gemma / Llama tokenizers average ~3.8–4.0 chars per token, or ~1.3 tokens per word).
     */
    fun estimateTokens(text: String): Int {
        if (text.isEmpty()) return 0
        var wordCount = 0
        var inWord = false
        for (i in 0 until text.length) {
            val c = text[i]
            if (c.isWhitespace()) {
                inWord = false
            } else if (!inWord) {
                inWord = true
                wordCount++
            }
        }
        val charBasedEstimate = (text.length + 3) / 4
        val wordBasedEstimate = ((wordCount * 13) + 9) / 10
        return maxOf(1, maxOf(charBasedEstimate, wordBasedEstimate))
    }

    /**
     * Splits [text] into overlapping, boundary-aware [TextChunk]s.
     *
     * @param text Source document text.
     * @param maxChunkChars Maximum characters per chunk (default 512, ~128 tokens).
     * @param overlapChars Number of trailing characters to overlap into the next chunk (default 64).
     */
    fun chunk(
        text: String,
        maxChunkChars: Int = 512,
        overlapChars: Int = 64
    ): List<TextChunk> {
        require(maxChunkChars > 0) { "maxChunkChars must be positive, got $maxChunkChars" }
        val safeOverlap = overlapChars.coerceIn(0, (maxChunkChars - 1).coerceAtLeast(0))
        val trimmed = text.trim()
        if (trimmed.isEmpty()) return emptyList()

        if (trimmed.length <= maxChunkChars) {
            return listOf(
                TextChunk(
                    index = 0,
                    text = trimmed,
                    startOffset = 0,
                    endOffset = trimmed.length,
                    estimatedTokens = estimateTokens(trimmed)
                )
            )
        }

        val chunks = ArrayList<TextChunk>()
        var cursor = 0
        val length = trimmed.length

        while (cursor < length) {
            // Skip leading whitespace for the chunk
            while (cursor < length && trimmed[cursor].isWhitespace()) {
                cursor++
            }
            if (cursor >= length) break

            val maxEnd = (cursor + maxChunkChars).coerceAtMost(length)
            val end = if (maxEnd == length) {
                length
            } else {
                findBestSplitPoint(trimmed, cursor, maxEnd)
            }

            val rawSlice = trimmed.substring(cursor, end).trim()
            if (rawSlice.isNotEmpty()) {
                chunks.add(
                    TextChunk(
                        index = chunks.size,
                        text = rawSlice,
                        startOffset = cursor,
                        endOffset = end,
                        estimatedTokens = estimateTokens(rawSlice)
                    )
                )
            }

            if (end >= length) break

            // Advance cursor with overlap, snapping to a clean word boundary if possible
            val rawNextCursor = (end - safeOverlap).coerceAtLeast(cursor + 1)
            cursor = snapToForwardWordBoundary(trimmed, rawNextCursor, end)
        }

        return chunks
    }

    /**
     * Splits [text] using a target token budget per chunk rather than raw character count.
     */
    fun chunkByTokens(
        text: String,
        maxTokensPerChunk: Int = 128,
        overlapTokens: Int = 16
    ): List<TextChunk> {
        require(maxTokensPerChunk > 0) { "maxTokensPerChunk must be positive" }
        return chunk(
            text = text,
            maxChunkChars = maxTokensPerChunk * 4,
            overlapChars = overlapTokens.coerceAtLeast(0) * 4
        )
    }

    private fun findBestSplitPoint(text: String, start: Int, maxEnd: Int): Int {
        // Search only in the second half of the window so chunks stay reasonably full
        val minSearch = start + ((maxEnd - start) / 2).coerceAtLeast(1)

        // 1. Paragraph boundary (\n\n)
        val paraIdx = text.lastIndexOf("\n\n", startIndex = maxEnd - 1)
        if (paraIdx >= minSearch) return paraIdx + 2

        // 2. Single newline (\n)
        val lineIdx = text.lastIndexOf('\n', startIndex = maxEnd - 1)
        if (lineIdx >= minSearch) return lineIdx + 1

        // 3. Sentence boundary (. / ! / ?) followed by whitespace
        for (i in (maxEnd - 1) downTo minSearch) {
            val c = text[i]
            if ((c == '.' || c == '!' || c == '?') && (i + 1 >= text.length || text[i + 1].isWhitespace())) {
                return i + 1
            }
        }

        // 4. Clause or word boundary (whitespace)
        for (i in (maxEnd - 1) downTo minSearch) {
            if (text[i].isWhitespace()) {
                return i + 1
            }
        }

        // 5. Fallback: hard split at maxEnd
        return maxEnd
    }

    private fun snapToForwardWordBoundary(text: String, candidateStart: Int, currentEnd: Int): Int {
        if (candidateStart <= 0 || text[candidateStart - 1].isWhitespace() || text[candidateStart].isWhitespace()) {
            return candidateStart
        }
        var i = candidateStart
        while (i < currentEnd && !text[i].isWhitespace()) {
            i++
        }
        return if (i < currentEnd) i + 1 else candidateStart
    }
}
