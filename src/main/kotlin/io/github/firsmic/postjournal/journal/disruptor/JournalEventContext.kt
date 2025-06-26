package io.github.firsmic.postjournal.journal.disruptor

class JournalEventContext {
    var journalInputEvent: JournalInputEvent? = null
    var outputEvent: JournalOutputEvent? = null
        private set

    fun cleanup() {
        journalInputEvent = null
        outputEvent = null
    }

    fun addOutputEvent(event: JournalOutputEvent) {
        val previousEvent = outputEvent
        outputEvent = if (previousEvent == null) {
            event
        } else {
            JournalOutputEvent.CompositeEvent(
                listOf(
                    previousEvent,
                    event
                )
            )
        }
    }

    operator fun plusAssign(event: JournalOutputEvent) {
        addOutputEvent(event)
    }
}
