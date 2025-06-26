package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalOutputEvent

internal class ShutdownWriterState(
    sharedState: SharedState,
    currentEpoch: Epoch,
    nextTxnId: TxnId,
) : TerminalState(
    sharedState,
    currentEpoch,
    nextTxnId
) {
    fun onShutdown(context: JournalEventContext) {
        context += JournalOutputEvent.WriterDeactivatedEvent
    }
}