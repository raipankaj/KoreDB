package com.pankaj.koredb

import androidx.room.*

@Dao
interface NoteDao {
    @Query("SELECT * FROM notes")
    suspend fun getAll(): List<Note>

    @Query("SELECT * FROM notes WHERE id = :id")
    suspend fun getById(id: String): Note?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insert(note: Note)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAll(notes: List<Note>)

    @Delete
    suspend fun delete(note: Note)

    @Query("DELETE FROM notes")
    suspend fun deleteAll()

    @Query("DELETE FROM notes WHERE id = :id")
    suspend fun deleteById(id: String)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertEdge(edge: EdgeEntity)

    @Query("SELECT * FROM notes WHERE id LIKE :prefix || '%'")
    fun getByPrefix(prefix: String): List<Note>

    /**
     * 2-Hop Traversal in SQL: Alice -> FOLLOWS -> ? -> FOLLOWS -> Result
     */
    @Query("""
        SELECT e2.toId FROM edges e1 
        JOIN edges e2 ON e1.toId = e2.fromId 
        WHERE e1.fromId = :startId 
        AND e1.relation = :rel 
        AND e2.relation = :rel
    """)
    suspend fun getTwoHopNodes(startId: String, rel: String): List<String>

    @Query("DELETE FROM edges")
    suspend fun clearEdges()
}
