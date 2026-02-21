package com.pankaj.koredb

import android.app.Application
import androidx.room.Room
import com.pankaj.koredb.db.KoreAndroid
import com.pankaj.koredb.db.KoreDatabase

class MyApplication: Application() {

    // Global Singleton Instance
    lateinit var database: KoreDatabase
    lateinit var roomDatabase: AppDatabase

    override fun onCreate() {
        super.onCreate()

        // Initialize the engine ONCE
        database = KoreAndroid.create(this, "my_notes_db")

        // Initialize Room
        roomDatabase = Room.databaseBuilder(
            applicationContext,
            AppDatabase::class.java, "room_notes_db"
        ).build()
    }

    override fun onTerminate() {
        super.onTerminate()
        // Close safely when the OS kills the process (optional but good practice)
        database.close()
        roomDatabase.close()
    }

}
