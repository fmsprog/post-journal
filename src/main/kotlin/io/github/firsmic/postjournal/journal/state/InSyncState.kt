package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.api.isReadOnly
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent
import java.util.concurrent.TimeUnit

internal class InSyncState(
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
    readerLockAcquired,
) {
    override fun processTimerEvent(event: JournalInputEvent.TimerEvent, context: JournalEventContext): JournalState {
        if (mode.isReadOnly) {
            // do not activate
            return this
        }
        val now = timeSource.nanoTime()
        val lastMessageIntervalMs = TimeUnit.NANOSECONDS.toMillis (now - lastMessageNanos)
        val lastLeaderMessageIntervalMs = TimeUnit.NANOSECONDS.toMillis (now - lastLeaderMessageNanos)
        return if (lastMessageIntervalMs > START_CATCH_UP_IDLE_TIMEOUT_MS) {
            // No messages for a long time, Kafka may be down - do not try to activate
            CatchingUpState(sharedState, currentEpoch, nextTxnId, recoveryFinished, readerLockAcquired)
        } else if (
            lastMessageIntervalMs < START_CATCH_UP_IDLE_TIMEOUT_MS/2 &&
            lastLeaderMessageIntervalMs > ACTIVATION_TIMEOUT_MS
        ) {
            if (readerLockAcquired) {
                // Activation is blocked
                this
            } else {
                // Kafka is alive, but no messages from leader for a long time - can try to activate
                ActivatingState(sharedState, currentEpoch, nextTxnId).apply {
                    onActivating(context)
                }
            }
        } else {
            sendHeartbeatIfNeed(context)
            this
        }
    }

    override fun processReaderLagEvent(event: JournalInputEvent.ReaderLagEvent, context: JournalEventContext): JournalState {
        return if (event.lag >= START_CATCH_UP_LAG) {
            CatchingUpState(sharedState, currentEpoch, nextTxnId, recoveryFinished, readerLockAcquired)
        } else {
            this
        }
    }

    override fun processMessageEvent(event: JournalInputEvent.InputDataMessageEvent, context: JournalEventContext): JournalState {
        val processed = handleJournalMessage(event, context)
        val result = checkEndOfRecovery(event, context)
        if (result.isEndOfRecovery) {
            return result.state
        }
        if (!processed) {
            return this
        }
        val lastMessageIntervalMs = TimeUnit.NANOSECONDS.toMillis (timeSource.nanoTime() - lastMessageNanos)
        if (lastMessageIntervalMs > START_CATCH_UP_IDLE_TIMEOUT_MS) {
            return CatchingUpState(sharedState, currentEpoch, nextTxnId, recoveryFinished, readerLockAcquired)
        }
        return this
    }
}