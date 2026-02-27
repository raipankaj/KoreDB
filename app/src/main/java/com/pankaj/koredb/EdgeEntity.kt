package com.pankaj.koredb

import androidx.room.Entity

@Entity(tableName = "edges", primaryKeys = ["fromId", "toId", "relation"])
data class EdgeEntity(
    val fromId: String,
    val toId: String,
    val relation: String
)