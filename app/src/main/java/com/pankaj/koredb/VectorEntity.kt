package com.pankaj.koredb

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
    tableName = "vectors",
    indices = [Index(value = ["category"])]
)
data class VectorEntity(
    @PrimaryKey val id: String,
    val blob: ByteArray,
    val category: String = "",
    val price: Double = 0.0,
    val label: String = ""
)