package io.github.firsmic.postjournal.journal.disruptor

import io.github.firsmic.postjournal.app.core.topology.BaseEventHandler

class ContextCleaner : BaseEventHandler<JournalEventContext> {
    override fun onEvent(
        event: JournalEventContext?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        event?.cleanup()
    }
}