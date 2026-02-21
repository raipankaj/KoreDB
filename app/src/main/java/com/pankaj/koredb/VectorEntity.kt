package com.pankaj.koredb

import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "vectors")
data class VectorEntity(
    @PrimaryKey val id: String,
    val blob: ByteArray
)