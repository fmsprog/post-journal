package io.github.firsmic.postjournal.journal.state

interface TimeSource {
    fun nanoTime(): Long
    fun currentTimeMillis(): Long
}