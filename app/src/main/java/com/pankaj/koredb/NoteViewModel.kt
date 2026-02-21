package com.pankaj.koredb

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.pankaj.koredb.core.KoreCollection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import java.util.UUID
import kotlin.system.measureTimeMillis

class NoteViewModel(application: Application) : AndroidViewModel(application) {

    // 1. Get Singleton DBs
    private val koreDb = (application as MyApplication).database
    private val roomDb = (application as MyApplication).roomDatabase
    private val noteDao = roomDb.noteDao()

    // 2. Collections: One for Notes
    private val notesCollection: KoreCollection<Note> = koreDb.collection("notes")

    // 3. UI State
    private val _notes = MutableStateFlow<List<Note>>(emptyList())
    val notes = _notes.asStateFlow()

    private val _readBenchmarkText = MutableStateFlow("Read: --")
    val readBenchmarkText = _readBenchmarkText.asStateFlow()

    private val _writeBenchmarkText = MutableStateFlow("Write: --")
    val writeBenchmarkText = _writeBenchmarkText.asStateFlow()

    private val _benchmarkText = MutableStateFlow("Ready")
    val benchmarkText = _benchmarkText.asStateFlow()

    init {
        loadNotes()
    }

    /**
     * Loads the Master Index, then fetches all Notes in parallel.
     * Showcases the read speed.
     */
    private fun loadNotes() {
        viewModelScope.launch(Dispatchers.IO) {
            var koreTime: Long
            var roomTime: Long
            var notesList: List<Note>

            koreTime = measureTimeMillis {
                notesList = notesCollection.getAll().sortedByDescending { it.isPinned }
            }
            _notes.value = notesList

            roomTime = measureTimeMillis {
                noteDao.getAll()
            }

            _readBenchmarkText.value = "Read: KoreDB ${koreTime}ms | Room ${roomTime}ms"
            updateCombinedBenchmark()
        }
    }

    private fun updateCombinedBenchmark() {
        _benchmarkText.value = "${_readBenchmarkText.value}\n${_writeBenchmarkText.value}"
    }

    fun addNote(title: String, content: String, isPinned: Boolean = false) {
        viewModelScope.launch(Dispatchers.IO) {
            val newId = UUID.randomUUID().toString()
            val newNote = Note(newId, title, content, isPinned)

            val koreTime = measureTimeMillis {
                // 1. Save the Note to KoreDB
                notesCollection.insert(newId, newNote)
            }

            val roomTime = measureTimeMillis {
                // 2. Save the Note to Room
                noteDao.insert(newNote)
            }

            // 3. Refresh UI
            refreshLocalList(newNote)

            _writeBenchmarkText.value = "Write: KoreDB ${koreTime}ms | Room ${roomTime}ms"
            updateCombinedBenchmark()
        }
    }

    /**
     * Showcases Bulk Insert Speed (Batching)
     */
    fun generateBulkNotes() {
        viewModelScope.launch(Dispatchers.IO) {
            val newNotes = (1..50).map { i ->
                Note(UUID.randomUUID().toString(), "Speed Test #$i", "Performance testing content.", false)
            }

            val koreTime = measureTimeMillis {
                // 1. Convert List to Map (This is an O(N) RAM operation, basically 0ms)
                val batchMap = newNotes.associateBy { it.id }
                // 2. Insert all 50 notes in ONE single disk operation
                notesCollection.insertBatch(batchMap)
            }

            val roomTime = measureTimeMillis {
                // Insert all notes to Room
                noteDao.insertAll(newNotes)
            }

            _writeBenchmarkText.value = "Bulk Write (50): KoreDB ${koreTime}ms | Room ${roomTime}ms"
            loadNotes() // Reload full list and updateCombinedBenchmark is called inside
        }
    }

    fun deleteNote(id: String) {
        viewModelScope.launch(Dispatchers.IO) {
            val koreTime = measureTimeMillis {
                // 1. Tombstone the note in KoreDB
                notesCollection.delete(id)
            }

            val roomTime = measureTimeMillis {
                // 2. Delete from Room
                noteDao.deleteById(id)
            }

            // 3. UI Update
            _notes.value = _notes.value.filter { it.id != id }

            _writeBenchmarkText.value = "Delete: KoreDB ${koreTime}ms | Room ${roomTime}ms"
            updateCombinedBenchmark()
        }
    }

    fun nukeAll() {
        viewModelScope.launch(Dispatchers.IO) {
            notesCollection.deleteAll()
            noteDao.deleteAll()
            _notes.value = emptyList()
            _readBenchmarkText.value = "Read: --"
            _writeBenchmarkText.value = "Write: --"
            _benchmarkText.value = "Nuked Both Databases"
        }
    }

    private fun refreshLocalList(newNote: Note) {
        // Optimistic UI update
        val current = _notes.value.toMutableList()
        current.add(0, newNote)
        _notes.value = current
    }
}
