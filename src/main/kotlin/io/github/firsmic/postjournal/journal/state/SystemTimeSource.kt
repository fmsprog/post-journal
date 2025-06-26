package io.github.firsmic.postjournal.journal.state

object SystemTimeSource : TimeSource {

    // wall time
    override fun nanoTime() = System.nanoTime()

    // UTC time
    override fun currentTimeMillis() = System.currentTimeMillis()
}