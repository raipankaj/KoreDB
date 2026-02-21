package com.pankaj.koredb

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query

@Dao
interface VectorDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAll(vectors: List<VectorEntity>)

    @Query("SELECT * FROM vectors")
    suspend fun getAll(): List<VectorEntity>
}