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

package com.pankaj.koredb.core

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialInfo
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

/**
 * Declares a secondary equality index on a `@Serializable` document property without requiring KSP.
 *
 * Supports both scalar properties (`String`, `Int`, `Enum`, etc.) and multi-value collection
 * properties (`List<String>`, `Set<String>`, `List<Enum>`, etc.), which automatically fan out
 * each element into an inverted secondary index entry.
 *
 * When a collection is opened via `db.collection<T>(name)` or `db.binaryCollection<T>(name)`,
 * KoreDB inspects the compile-time `SerialDescriptor` and automatically registers the index.
 *
 * @property name Optional custom index name. Defaults to the property's serial name.
 */
@OptIn(ExperimentalSerializationApi::class)
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.BINARY)
annotation class KoreIndex(val name: String = "")

/**
 * Declares a composite multi-field secondary index on a `@Serializable` document class without KSP.
 *
 * Example:
 * ```kotlin
 * @Serializable
 * @KoreCompositeIndex(fields = ["category", "tier"])
 * data class Article(
 *     val id: String,
 *     val category: String,
 *     val tier: String
 * )
 * ```
 *
 * Queries filtering on all constituent fields via `whereEq` automatically push down
 * a single composite LSM prefix lookup.
 *
 * @property fields Ordered array of property names forming the composite key.
 * @property name Optional custom index name. Defaults to `fields.joinToString("+")`.
 */
@OptIn(ExperimentalSerializationApi::class)
@SerialInfo
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.BINARY)
annotation class KoreCompositeIndex(
    val fields: Array<String>,
    val name: String = ""
)

/**
 * Container annotation for declaring multiple [KoreCompositeIndex] definitions on a single `@Serializable` class.
 */
@OptIn(ExperimentalSerializationApi::class)
@SerialInfo
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.BINARY)
annotation class KoreCompositeIndexes(
    vararg val value: KoreCompositeIndex
)

/**
 * Declares an order-preserving IEEE-754 numeric secondary index on a `@Serializable` numeric property
 * (`Int`, `Long`, `Float`, `Double`, `Short`, `Byte`) without requiring KSP.
 *
 * Enables $O(\log N)$ range queries (`whereBetween`, `whereGt`, `whereGte`, `whereLt`, `whereLte`).
 *
 * @property name Optional custom index name. Defaults to the property's serial name.
 */
@OptIn(ExperimentalSerializationApi::class)
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.BINARY)
annotation class KoreNumericIndex(val name: String = "")

/**
 * Marks a `@Serializable` string property for automatic Okapi BM25 Full-Text Search indexing.
 */
@OptIn(ExperimentalSerializationApi::class)
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.BINARY)
annotation class KoreSearchable

/**
 * Registers a `@Serializable` property for Query DSL filtering, sorting, and aggregation
 * without creating an on-disk secondary index.
 *
 * @property name Optional custom property name. Defaults to the property's serial name.
 */
@OptIn(ExperimentalSerializationApi::class)
@SerialInfo
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.BINARY)
annotation class KoreProperty(val name: String = "")

/**
 * Zero-reflection, zero-KSP index auto-configurator powered by `kotlinx.serialization` descriptors.
 */
@OptIn(ExperimentalSerializationApi::class)
internal object KoreAnnotationIndexer {

    fun <T : Any> autoConfigure(collection: KoreCollection<T>, serializer: KSerializer<T>) {
        val descriptor = serializer.descriptor
        val count = descriptor.elementsCount
        if (count <= 0) return

        val elementIndexByName = HashMap<String, Int>(count)

        for (elemIdx in 0 until count) {
            val defaultName = descriptor.getElementName(elemIdx)
            elementIndexByName[defaultName] = elemIdx

            val annotations = descriptor.getElementAnnotations(elemIdx)
            if (annotations.isEmpty()) continue

            val elemKind = descriptor.getElementDescriptor(elemIdx).kind

            for (ann in annotations) {
                when (ann) {
                    is KoreIndex -> {
                        val idxName = ann.name.ifEmpty { defaultName }
                        if (elemKind == StructureKind.LIST) {
                            collection.createMultiValueIndex(idxName) { doc ->
                                extractFieldAsStringList(serializer, doc, elemIdx)
                            }
                        } else {
                            collection.createIndex(idxName) { doc ->
                                extractFieldAsString(serializer, doc, elemIdx)
                            }
                        }
                    }
                    is KoreNumericIndex -> {
                        val idxName = ann.name.ifEmpty { defaultName }
                        collection.createNumericIndex(idxName) { doc ->
                            extractFieldAsDouble(serializer, doc, elemIdx)
                        }
                    }
                    is KoreSearchable -> {
                        collection.searchableFields({ doc ->
                            extractFieldAsString(serializer, doc, elemIdx)
                        })
                        if (!collection.hasIndex(defaultName) && !collection.hasNumericIndex(defaultName)) {
                            collection.registerProperty(defaultName) { doc ->
                                extractFieldAsString(serializer, doc, elemIdx)
                            }
                        }
                    }
                    is KoreProperty -> {
                        val propName = ann.name.ifEmpty { defaultName }
                        collection.registerProperty(propName) { doc ->
                            extractFieldAsString(serializer, doc, elemIdx)
                        }
                    }
                }
            }
        }

        // Process class-level @KoreCompositeIndex and @KoreCompositeIndexes annotations
        val compositeAnnotations = mutableListOf<KoreCompositeIndex>()
        for (classAnn in descriptor.annotations) {
            when (classAnn) {
                is KoreCompositeIndex -> compositeAnnotations.add(classAnn)
                is KoreCompositeIndexes -> compositeAnnotations.addAll(classAnn.value)
            }
        }

        for (comp in compositeAnnotations) {
            val fieldNames = comp.fields.toList()
            if (fieldNames.size < 2) continue

            // Ensure every field participating in the composite index has a registered property extractor
            for (fieldName in fieldNames) {
                if (!collection.hasPropertyExtractor(fieldName)) {
                    val elemIdx = elementIndexByName[fieldName] ?: continue
                    collection.registerProperty(fieldName) { doc ->
                        extractFieldAsString(serializer, doc, elemIdx)
                    }
                }
            }

            val compName = comp.name.ifEmpty { fieldNames.joinToString("+") }
            collection.createCompositeIndex(fieldNames, compName)
        }
    }

    private val threadLocalEncoder = object : ThreadLocal<SingleFieldExtractorEncoder>() {
        override fun initialValue(): SingleFieldExtractorEncoder = SingleFieldExtractorEncoder()
    }

    private fun <T> extractFieldAsString(serializer: KSerializer<T>, doc: T, targetIndex: Int): String {
        val encoder = threadLocalEncoder.get()!!.apply { reset(targetIndex) }
        serializer.serialize(encoder, doc)
        return encoder.stringValue
    }

    private fun <T> extractFieldAsDouble(serializer: KSerializer<T>, doc: T, targetIndex: Int): Double {
        val encoder = threadLocalEncoder.get()!!.apply { reset(targetIndex) }
        serializer.serialize(encoder, doc)
        return encoder.doubleValue
    }

    private fun <T> extractFieldAsStringList(serializer: KSerializer<T>, doc: T, targetIndex: Int): List<String> {
        val encoder = threadLocalEncoder.get()!!.apply { reset(targetIndex) }
        serializer.serialize(encoder, doc)
        return if (encoder.listValues.isEmpty()) emptyList() else ArrayList(encoder.listValues)
    }

    /**
     * Ultra-lightweight `AbstractEncoder` that captures a single top-level element (or top-level
     * list/set of elements) by index and skips serializing all other nested objects/collections.
     * Reused via [ThreadLocal] for zero heap allocations on hot insert/query paths.
     */
    private class SingleFieldExtractorEncoder : AbstractEncoder() {
        override val serializersModule: SerializersModule = EmptySerializersModule()

        private var targetIndex: Int = 0
        private var depth = 0
        private var isTargetActive = false

        var stringValue: String = ""
        var doubleValue: Double = 0.0
        val listValues = ArrayList<String>(8)

        fun reset(newTargetIndex: Int) {
            targetIndex = newTargetIndex
            depth = 0
            isTargetActive = false
            stringValue = ""
            doubleValue = 0.0
            listValues.clear()
        }

        override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
            depth++
            return this
        }

        override fun endStructure(descriptor: SerialDescriptor) {
            depth--
        }

        override fun shouldEncodeElementDefault(descriptor: SerialDescriptor, index: Int): Boolean {
            return (depth == 1 && index == targetIndex) || (isTargetActive && depth == 2)
        }

        override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
            if (depth == 1) {
                isTargetActive = (index == targetIndex)
                return isTargetActive
            }
            return isTargetActive && depth == 2
        }

        override fun <T> encodeSerializableElement(
            descriptor: SerialDescriptor,
            index: Int,
            serializer: SerializationStrategy<T>,
            value: T
        ) {
            if (depth == 1 && index == targetIndex) {
                isTargetActive = true
                stringValue = value?.toString() ?: ""
                doubleValue = (value as? Number)?.toDouble() ?: stringValue.toDoubleOrNull() ?: 0.0
                serializer.serialize(this, value)
                isTargetActive = false
            } else if (isTargetActive && depth == 2) {
                val beforeSize = listValues.size
                serializer.serialize(this, value)
                if (listValues.size == beforeSize && value != null) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun <T : Any> encodeNullableSerializableElement(
            descriptor: SerialDescriptor,
            index: Int,
            serializer: SerializationStrategy<T>,
            value: T?
        ) {
            if (depth == 1 && index == targetIndex) {
                isTargetActive = true
                if (value == null) {
                    stringValue = ""
                    doubleValue = 0.0
                } else {
                    stringValue = value.toString()
                    doubleValue = (value as? Number)?.toDouble() ?: stringValue.toDoubleOrNull() ?: 0.0
                    serializer.serialize(this, value)
                }
                isTargetActive = false
            } else if (isTargetActive && depth == 2 && value != null) {
                val beforeSize = listValues.size
                serializer.serialize(this, value)
                if (listValues.size == beforeSize) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeString(value: String) {
            if (isTargetActive) {
                if (depth == 1) {
                    stringValue = value
                    doubleValue = value.toDoubleOrNull() ?: 0.0
                } else if (depth == 2) {
                    listValues.add(value)
                }
            }
        }

        override fun encodeInt(value: Int) {
            if (isTargetActive) {
                if (depth == 1) {
                    doubleValue = value.toDouble()
                    stringValue = value.toString()
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeLong(value: Long) {
            if (isTargetActive) {
                if (depth == 1) {
                    doubleValue = value.toDouble()
                    stringValue = value.toString()
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeDouble(value: Double) {
            if (isTargetActive) {
                if (depth == 1) {
                    doubleValue = value
                    stringValue = value.toString()
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeFloat(value: Float) {
            if (isTargetActive) {
                if (depth == 1) {
                    doubleValue = value.toDouble()
                    stringValue = value.toString()
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeShort(value: Short) {
            if (isTargetActive) {
                if (depth == 1) {
                    doubleValue = value.toDouble()
                    stringValue = value.toString()
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeByte(value: Byte) {
            if (isTargetActive) {
                if (depth == 1) {
                    doubleValue = value.toDouble()
                    stringValue = value.toString()
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeBoolean(value: Boolean) {
            if (isTargetActive) {
                if (depth == 1) {
                    stringValue = value.toString()
                    doubleValue = if (value) 1.0 else 0.0
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeChar(value: Char) {
            if (isTargetActive) {
                if (depth == 1) {
                    stringValue = value.toString()
                } else if (depth == 2) {
                    listValues.add(value.toString())
                }
            }
        }

        override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
            if (isTargetActive) {
                val enumName = enumDescriptor.getElementName(index)
                if (depth == 1) {
                    stringValue = enumName
                    doubleValue = index.toDouble()
                } else if (depth == 2) {
                    listValues.add(enumName)
                }
            }
        }

        override fun encodeNull() {
            if (isTargetActive && depth == 1) {
                stringValue = ""
                doubleValue = 0.0
            }
        }
    }
}
