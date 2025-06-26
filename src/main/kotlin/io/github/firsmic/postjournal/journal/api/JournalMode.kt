package io.github.firsmic.postjournal.journal.api

sealed interface JournalMode {
    data class Read(
        // Inclusive upper bound: data is read while offset <= maxOffsetToRead.
        val maxOffsetToRead: Offset? = null
    ) : JournalMode

    data object ReadAndWrite : JournalMode
}

val JournalMode.isReadOnly get() = this is JournalMode.Read
