package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.JournalMode
import io.github.firsmic.postjournal.journal.api.Offset
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.api.isNotNull
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent

internal abstract class JournalState(
    internal val sharedState: SharedState,
    var currentEpoch: Epoch,
    var nextTxnId: TxnId,
) {
    val timeSource: TimeSource get() = sharedState.timeSource
    val replicaId: String get() = sharedState.replicaId
    val mode: JournalMode get() = sharedState.mode

    companion object {
        val logger = JournalStateManager.logger
    }

    init {
        logState()
    }

    protected fun logState() {
        logger.info {
            "${javaClass.simpleName}: " +
                    "replicaId='$replicaId', " +
                    "currentEpoch='$currentEpoch', " +
                    "nextTxnId='$nextTxnId', " +
                    "offset='${sharedState.offset}'"
        }
    }


    abstract val isWriter: Boolean
    abstract val isReader: Boolean

    abstract fun processReaderLagEvent(event: JournalInputEvent.ReaderLagEvent, context: JournalEventContext): JournalState

    abstract fun processTimerEvent(event: JournalInputEvent.TimerEvent, context: JournalEventContext): JournalState

    abstract fun processMessageEvent(event: JournalInputEvent.InputDataMessageEvent, context: JournalEventContext): JournalState

    abstract fun processDataWriteEvent(event: JournalInputEvent.DataWriteEvent, context: JournalEventContext): JournalState

    abstract fun processLockReaderEvent(event: JournalInputEvent.LockReaderEvent, context: JournalEventContext): JournalState

    protected fun validateCurrentTxnId(txnId: TxnId) {
        if (nextTxnId.isNotNull && nextTxnId != txnId) {
            throw IllegalStateException("Journal inconsistency detected: expected txnId=$nextTxnId, received txnId=$txnId")
        }
    }
}

internal class SharedState(
    val timeSource: TimeSource,
    val replicaId: String,
    val mode: JournalMode,
    var offset: Offset,
)