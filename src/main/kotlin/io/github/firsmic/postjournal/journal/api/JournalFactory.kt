package io.github.firsmic.postjournal.journal.api

interface JournalFactory {
    fun createJournal(): Journal
}