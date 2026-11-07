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
 * Lightweight, zero-dependency on-device Entity & Keyphrase Extractor for GraphRAG ingestion.
 *
 * Identifies shared semantic anchors across documents so that [com.pankaj.koredb.bridge.GraphVectorBridge]
 * can automatically link chunks from separate documents via shared `Entity` graph nodes:
 * 1. **Multi-word Proper Nouns** (e.g., `"Nova Engine"`, `"Project Apollo"`, `"DeepSystems Research"`)
 * 2. **Technical / CamelCase Identifiers & Acronyms** (e.g., `"GraphRAG"`, `"KoreDB"`, `"HNSW"`, `"MVCC"`)
 * 3. **Explicit `#hashtags` and `@mentions`**
 */
object KoreEntityExtractor {

    /**
     * Represents an extracted entity mention from a document or text chunk.
     *
     * @property id Normalized canonical identifier used as the graph node ID (`"entity:<normalized>"`).
     * @property name Display name of the entity as it appeared in the text.
     * @property type Entity category (`"NamedEntity"`, `"Concept"`, `"Tag"`, `"Mention"`).
     */
    data class EntityMention(
        val id: String,
        val name: String,
        val type: String = "NamedEntity"
    )

    private val SENTENCE_STARTER_STOPWORDS = setOf(
        "the", "this", "that", "these", "those", "it", "its", "we", "they", "you", "he", "she",
        "for", "in", "on", "at", "by", "with", "from", "to", "of", "as", "into", "during",
        "after", "before", "when", "where", "while", "if", "unless", "although", "because",
        "however", "therefore", "furthermore", "moreover", "meanwhile", "additionally", "also",
        "some", "many", "most", "all", "each", "every", "both", "other", "another", "such"
    )

    private val HASHTAG_REGEX = Regex("""#([A-Za-z][A-Za-z0-9_-]{1,40})""")
    private val MENTION_REGEX = Regex("""@([A-Za-z][A-Za-z0-9_.-]{1,40})""")
    // Matches 2 to 4 consecutive capitalized words (e.g. "Project Apollo", "Nova Engine", "San Francisco")
    private val MULTI_WORD_PROPER_REGEX = Regex("""\b([A-Z][a-z0-9]+(?:\s+[A-Z][a-zA-Z0-9]+){1,3})\b""")
    // Matches CamelCase (e.g. "KoreDB", "GraphRAG", "VectorEngine") or 3+ uppercase acronyms (e.g. "HNSW", "MVCC", "LSM")
    private val TECHNICAL_CONCEPT_REGEX = Regex("""\b([A-Z][a-z0-9]+[A-Z][A-Za-z0-9]*|[A-Z]{3,10})\b""")

    /**
     * Extracts deduplicated [EntityMention]s from [text].
     */
    fun extract(text: String, maxEntities: Int = 16): List<EntityMention> {
        if (text.isBlank() || maxEntities <= 0) return emptyList()
        val mentions = LinkedHashMap<String, EntityMention>()

        // 1. Hashtags
        for (match in HASHTAG_REGEX.findAll(text)) {
            val raw = match.groupValues[1]
            val canonical = "entity:" + normalizeId(raw)
            mentions.putIfAbsent(canonical, EntityMention(canonical, raw, "Tag"))
            if (mentions.size >= maxEntities) return mentions.values.toList()
        }

        // 2. @Mentions
        for (match in MENTION_REGEX.findAll(text)) {
            val raw = match.groupValues[1]
            val canonical = "entity:" + normalizeId(raw)
            mentions.putIfAbsent(canonical, EntityMention(canonical, raw, "Mention"))
            if (mentions.size >= maxEntities) return mentions.values.toList()
        }

        // 3. Multi-word Proper Nouns (trimming leading sentence-starter stopwords like "For Project Apollo")
        for (match in MULTI_WORD_PROPER_REGEX.findAll(text)) {
            val words = match.groupValues[1].split(Regex("""\s+"""))
                .dropWhile { it.lowercase() in SENTENCE_STARTER_STOPWORDS }
            if (words.size >= 2) {
                val phrase = words.joinToString(" ")
                val canonical = "entity:" + normalizeId(phrase)
                mentions.putIfAbsent(canonical, EntityMention(canonical, phrase, "NamedEntity"))
                if (mentions.size >= maxEntities) return mentions.values.toList()
            }
        }

        // 4. CamelCase & Acronym Technical Concepts
        for (match in TECHNICAL_CONCEPT_REGEX.findAll(text)) {
            val token = match.groupValues[1]
            if (token.lowercase() in SENTENCE_STARTER_STOPWORDS) continue
            val canonical = "entity:" + normalizeId(token)
            mentions.putIfAbsent(canonical, EntityMention(canonical, token, "Concept"))
            if (mentions.size >= maxEntities) return mentions.values.toList()
        }

        return mentions.values.toList()
    }

    private fun normalizeId(raw: String): String {
        val sb = StringBuilder(raw.length)
        var lastUnderscore = false
        for (i in 0 until raw.length) {
            val c = raw[i].lowercaseChar()
            if (c in 'a'..'z' || c in '0'..'9') {
                sb.append(c)
                lastUnderscore = false
            } else if (!lastUnderscore && sb.isNotEmpty()) {
                sb.append('_')
                lastUnderscore = true
            }
        }
        return sb.toString().trimEnd('_')
    }
}
