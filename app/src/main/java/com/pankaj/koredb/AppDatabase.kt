package com.pankaj.koredb

import androidx.room.Database
import androidx.room.RoomDatabase

@Database(entities = [Note::class, VectorEntity::class, EdgeEntity::class], version = 2, exportSchema = false)
abstract class AppDatabase : RoomDatabase() {
    abstract fun noteDao(): NoteDao
    abstract fun vectorDao(): VectorDao
}
