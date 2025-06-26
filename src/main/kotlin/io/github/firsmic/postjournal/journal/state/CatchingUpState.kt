package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent
import java.util.concurrent.TimeUnit

internal class CatchingUpState(
    sharedState: SharedState,
    currentEpoch: Epoch,
    nextTxnId: TxnId,
    recoveryFinished: Boolean,
    readerLockAcquired: Boolean,
) : RecoveringState(
    sharedState,
    currentEpoch,
    nextTxnId,
    recoveryFinished,
    readerLockAcquired
) {
    override fun processTimerEvent(event: JournalInputEvent.TimerEvent, context: JournalEventContext): JournalState {
        sendHeartbeatIfNeed(context)
        return this
    }

    override fun processReaderLagEvent(event: JournalInputEvent.ReaderLagEvent, context: JournalEventContext): JournalState {
        val lastMessageIntervalMs = TimeUnit.NANOSECONDS.toMillis (timeSource.nanoTime() - lastMessageNanos)
        if (event.lag < END_CATCH_UP_LAG && lastMessageIntervalMs < END_CATCH_UP_IDLE_TIMEOUT_MS) {
            return InSyncState(sharedState, currentEpoch, nextTxnId, recoveryFinished, readerLockAcquired)
        }
        return this
    }

    override fun processMessageEvent(event: JournalInputEvent.InputDataMessageEvent, context: JournalEventContext): JournalState {
        handleJournalMessage(event, context)
        return checkEndOfRecovery(event, context).state
    }
}