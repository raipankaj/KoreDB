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

package com.pankaj.koredb.fts

/**
 * Result of tokenizing a text document.
 *
 * @property termFrequencies A map of unique lowercased terms to their frequency within the document.
 * @property totalTokenCount The total number of non-stopword tokens in the document.
 */
data class TokenizationResult(
    val termFrequencies: Map<String, Int>,
    val totalTokenCount: Int
)

/**
 * Fast, allocation-conscious tokenizer for full-text search.
 */
object KoreTokenizer {

    private val STOP_WORDS = hashSetOf(
        "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
        "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
        "below", "between", "both", "but", "by", "can", "can't", "cannot", "could",
        "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
        "during", "each", "few", "for", "from", "further", "had", "hadn't", "has",
        "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her",
        "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's",
        "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it",
        "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my",
        "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or",
        "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same",
        "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so",
        "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
        "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll",
        "they're", "they've", "this", "those", "through", "to", "too", "under", "until",
        "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've",
        "were", "weren't", "what", "what's", "when", "when's", "where", "where's",
        "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't",
        "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
        "yours", "yourself", "yourselves"
    )

    /**
     * Tokenizes raw text into clean term frequencies and total token count.
     *
     * Splits on non-alphanumeric characters, converts to lowercase, and filters stopwords.
     *
     * @param text The input text string to tokenize.
     * @param filterStopwords Whether to remove common English stopwords (default true).
     * @return [TokenizationResult] containing frequency map and total length.
     */
    fun tokenize(text: String, filterStopwords: Boolean = true): TokenizationResult {
        if (text.isEmpty()) {
            return TokenizationResult(emptyMap(), 0)
        }

        val frequencies = HashMap<String, Int>()
        var totalTokens = 0

        val sb = StringBuilder()
        for (i in 0 until text.length) {
            val c = text[i]
            if (c.isLetterOrDigit()) {
                sb.append(c.lowercaseChar())
            } else if (sb.isNotEmpty()) {
                val term = sb.toString()
                sb.clear()
                if (!filterStopwords || !STOP_WORDS.contains(term)) {
                    frequencies[term] = (frequencies[term] ?: 0) + 1
                    totalTokens++
                }
            }
        }

        if (sb.isNotEmpty()) {
            val term = sb.toString()
            if (!filterStopwords || !STOP_WORDS.contains(term)) {
                frequencies[term] = (frequencies[term] ?: 0) + 1
                totalTokens++
            }
        }

        return TokenizationResult(frequencies, totalTokens)
    }

    /**
     * Tokenizes a query string into a list of search terms.
     */
    fun tokenizeQuery(query: String): List<String> {
        val result = mutableListOf<String>()
        val sb = StringBuilder()
        for (i in 0 until query.length) {
            val c = query[i]
            if (c.isLetterOrDigit()) {
                sb.append(c.lowercaseChar())
            } else if (sb.isNotEmpty()) {
                val term = sb.toString()
                sb.clear()
                if (!STOP_WORDS.contains(term)) {
                    result.add(term)
                }
            }
        }
        if (sb.isNotEmpty()) {
            val term = sb.toString()
            if (!STOP_WORDS.contains(term)) {
                result.add(term)
            }
        }
        return result
    }
}
