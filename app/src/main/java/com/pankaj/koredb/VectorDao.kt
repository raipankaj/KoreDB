package com.pankaj.koredb

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.Update

@Dao
interface VectorDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAll(vectors: List<VectorEntity>)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insert(vector: VectorEntity)

    @Query("SELECT * FROM vectors")
    suspend fun getAll(): List<VectorEntity>

    @Query("SELECT * FROM vectors WHERE id = :id")
    suspend fun getById(id: String): VectorEntity?

    @Query("SELECT * FROM vectors WHERE category = :category")
    suspend fun getByCategory(category: String): List<VectorEntity>

    @Query("DELETE FROM vectors WHERE id = :id")
    suspend fun deleteById(id: String)

    @Query("DELETE FROM vectors")
    suspend fun deleteAll()

    @Query("SELECT COUNT(*) FROM vectors")
    suspend fun count(): Int

    @Update
    suspend fun update(vector: VectorEntity)
}