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

package com.pankaj.koredb.engine.mvcc

import com.pankaj.koredb.core.KoreCollection

/**
 * A typed collection interface bound to an active [MvccTransaction].
 *
 * All operations execute within the transaction's Snapshot Isolation context.
 * Reads observe mutations made by this transaction as well as the snapshot state.
 * Writes are accumulated in the transaction buffer and committed atomically when
 * the transaction finishes without conflict.
 */
class MvccCollection<T : Any>(
    val tx: MvccTransaction,
    val collection: KoreCollection<T>
) {
    /**
     * Reads a document by ID within the transaction's isolated snapshot.
     *
     * @param id The document ID.
     * @return The document if found, or null.
     */
    fun getById(id: String): T? {
        val key = collection.makeDocKey(id)
        val raw = tx.getRaw(key) ?: return null
        return collection.internalSerializer.deserialize(raw)
    }

    /**
     * Inserts or replaces a document in the transaction's private write buffer.
     *
     * @param id Unique identifier for the document.
     * @param document The document instance to store.
     */
    fun insert(id: String, document: T) {
        val key = collection.makeDocKey(id)
        val bytes = collection.internalSerializer.serialize(document)
        tx.putRaw(key, bytes)
    }

    /**
     * Updates an existing document atomically within the transaction.
     *
     * @param id Document identifier.
     * @param transform Function to apply to the existing document.
     * @return The updated document instance.
     * @throws NoSuchElementException if the document does not exist.
     */
    fun update(id: String, transform: (T) -> T): T {
        val existing = getById(id)
            ?: throw NoSuchElementException("Document with ID '$id' does not exist in collection '${collection.name}'.")
        val updated = transform(existing)
        insert(id, updated)
        return updated
    }

    /**
     * Deletes a document by ID within the transaction's private write buffer.
     *
     * @param id The document ID to delete.
     */
    fun delete(id: String) {
        val key = collection.makeDocKey(id)
        tx.deleteRaw(key)
    }
}
