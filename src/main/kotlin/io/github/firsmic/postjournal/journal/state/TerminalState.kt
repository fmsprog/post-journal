package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent

internal abstract class TerminalState(
    sharedState: SharedState,
    currentEpoch: Epoch,
    nextTxnId: TxnId,
) : JournalState(
    sharedState,
    currentEpoch,
    nextTxnId
) {
    override val isReader: Boolean get() = false
    override val isWriter: Boolean get() = false

    override fun processTimerEvent(
        event: JournalInputEvent.TimerEvent,
        context: JournalEventContext
    ): JournalState {
        // Ignore
        return this
    }

    override fun processReaderLagEvent(event: JournalInputEvent.ReaderLagEvent, context: JournalEventContext): JournalState {
        // Ignore
        return this
    }

    override fun processMessageEvent(
        event: JournalInputEvent.InputDataMessageEvent,
        context: JournalEventContext
    ): JournalState {
        return this
    }

    override fun processDataWriteEvent(
        event: JournalInputEvent.DataWriteEvent,
        context: JournalEventContext
    ): JournalState {
        return this
    }

    override fun processLockReaderEvent(
        event: JournalInputEvent.LockReaderEvent,
        context: JournalEventContext
    ): JournalState {
        if (event.acquire) {
            event.result.complete(false)
        } else {
            event.result.completeExceptionally(
                IllegalStateException("Reader lock is not acquired")
            )
        }
        return this
    }
}