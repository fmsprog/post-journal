package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.JournalMode
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.api.isNotNull
import io.github.firsmic.postjournal.journal.api.isNotNullEpoch
import io.github.firsmic.postjournal.journal.api.isReadOnly
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent
import io.github.firsmic.postjournal.journal.disruptor.JournalOutputEvent
import io.github.firsmic.postjournal.journal.kafka.JournalInputMessage
import io.github.firsmic.postjournal.journal.kafka.OutputJournalMessage
import io.github.firsmic.postjournal.journal.kafka.toReaderRecord
import java.util.concurrent.TimeUnit

internal abstract class RecoveringState(
    sharedState: SharedState,
    currentEpoch: Epoch,
    nextTxnId: TxnId,
    protected var recoveryFinished: Boolean,
    protected var readerLockAcquired: Boolean
) : JournalState(
    sharedState,
    currentEpoch,
    nextTxnId
) {
    override val isReader: Boolean get() = true
    override val isWriter: Boolean get() = false

    protected var lastLeaderMessageNanos: Long = timeSource.nanoTime()
    protected var lastMessageNanos: Long = lastLeaderMessageNanos
    protected var lastSentHeartbeatNanos: Long = timeSource.nanoTime()

    protected fun handleJournalMessage(event: JournalInputEvent.InputDataMessageEvent, context: JournalEventContext): Boolean {
        lastMessageNanos = timeSource.nanoTime()
        val journalMessage = event.message
        val receivedEpoch = journalMessage.epoch
        if (currentEpoch.isNotNullEpoch && receivedEpoch < currentEpoch) {
            // ignore a message with older epoch
            return false
        }
        val prevEpoch = currentEpoch
        when (journalMessage) {
            // TODO - add validation that epoch changed only by NewEpoch message
            // validate recovery from arbitrary txn corner case
            //
            is JournalInputMessage.NewEpoch -> {
                lastLeaderMessageNanos = lastMessageNanos
                currentEpoch = journalMessage.epoch
            }

            is JournalInputMessage.Heartbeat -> {
                if (journalMessage.isLeader) {
                    lastLeaderMessageNanos = lastMessageNanos
                    currentEpoch = journalMessage.epoch
                }
            }

            is JournalInputMessage.Data -> {
                lastLeaderMessageNanos = lastMessageNanos
                currentEpoch = journalMessage.epoch
                applyIncomingData(journalMessage, context)
            }
        }
        if (prevEpoch != currentEpoch) {
            logState()
        }
        return true
    }

    protected fun checkEndOfRecovery(event: JournalInputEvent.InputDataMessageEvent, context: JournalEventContext): CheckEndOfRecoveryResult {
        val maxOffsetToRead = (mode as? JournalMode.Read)?.maxOffsetToRead
        return if (maxOffsetToRead != null && event.message.offset >= maxOffsetToRead) {
            context += JournalOutputEvent.EndOfRecoveryEvent(null)
            CheckEndOfRecoveryResult.EndOfRecovery(
                ShutdownReaderState(sharedState, currentEpoch, nextTxnId)
            )
        } else {
            CheckEndOfRecoveryResult.ContinueRecovery(this)
        }
    }

    sealed interface CheckEndOfRecoveryResult {
        val state: JournalState
        val isEndOfRecovery: Boolean get() = this is EndOfRecovery
        data class ContinueRecovery<T : RecoveringState>(override val state: T) : CheckEndOfRecoveryResult
        data class EndOfRecovery(override val state: ShutdownReaderState) : CheckEndOfRecoveryResult
    }

    protected open fun isRecoveryStarted(txnId: TxnId): Boolean {
        return true
    }

    protected fun applyIncomingData(journalMessage: JournalInputMessage.Data, context: JournalEventContext) {
        val txnId = journalMessage.txnId
        if (nextTxnId.isNotNull && txnId < nextTxnId) {
            // TODO - validate txn digest to ensure that it's duplicate
            // Ignore duplicate txn
        } else {
            validateCurrentTxnId(txnId)
            nextTxnId = txnId + 1
            // actually reading will finish later
            val recoveryStarted = isRecoveryStarted(journalMessage.txnId)
            if (recoveryStarted && !recoveryFinished) {
                context += JournalOutputEvent.DataReadEvent(
                    record = journalMessage.toReaderRecord()
                )
            }
        }
    }

    protected fun sendHeartbeatIfNeed(context: JournalEventContext): Boolean {
        if (mode.isReadOnly) {
            return false
        }
        val now = timeSource.nanoTime()
        val maxMsgTime = maxOf(lastSentHeartbeatNanos, lastMessageNanos)
        val lastHeartbeatIntervalMs = TimeUnit.NANOSECONDS.toMillis(now - maxMsgTime)
        return if (lastHeartbeatIntervalMs > SEND_FOLLOWER_HEARTBEAT_INTERVAL_MS) {
            lastSentHeartbeatNanos = now
            context += JournalOutputEvent.WriteJournalMessageEvent(
                OutputJournalMessage.Heartbeat(
                    epoch = currentEpoch,
                    replicaId = replicaId,
                    timestamp = timeSource.currentTimeMillis(),
                    isLeader = false
                )
            )
            true
        } else {
            false
        }
    }

    override fun processDataWriteEvent(event: JournalInputEvent.DataWriteEvent, context: JournalEventContext): JournalState {
        throw IllegalStateException("Inactive instance can't write data")
    }

    override fun processLockReaderEvent(
        event: JournalInputEvent.LockReaderEvent,
        context: JournalEventContext
    ): JournalState {
        if (event.acquire) {
            if (readerLockAcquired) {
                event.result.completeExceptionally(
                    IllegalStateException("Reader lock already acquired")
                )
            } else {
                readerLockAcquired = true
                event.result.complete(true)
            }
        } else {
            if (!readerLockAcquired) {
                event.result.completeExceptionally(
                    IllegalStateException("Reader lock already released")
                )
            } else {
                readerLockAcquired = false
                // result value is unused
                event.result.complete(false)
            }
        }
        return this
    }
}