package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.app.core.topology.BaseEventHandler
import io.github.firsmic.postjournal.journal.api.JournalMode
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent
import mu.KLogging

const val START_CATCH_UP_LAG = 1000
const val END_CATCH_UP_LAG = 100
const val START_CATCH_UP_IDLE_TIMEOUT_MS = 500
const val END_CATCH_UP_IDLE_TIMEOUT_MS = 150
const val ACTIVATION_TIMEOUT_MS = 1000
const val SEND_LEADER_HEARTBEAT_INTERVAL_MS = 50
const val SEND_FOLLOWER_HEARTBEAT_INTERVAL_MS = 200

class JournalStateManager(
    timeSource: TimeSource = SystemTimeSource,
    replicaId: String,
    startReaderFrom: JournalTxn,
    mode: JournalMode,
) : BaseEventHandler<JournalEventContext> {
    companion object : KLogging()
    
    private val sharedState = SharedState(
        timeSource = timeSource,
        replicaId = replicaId,
        mode = mode,
        offset = startReaderFrom.offset
    )

    private var state: JournalState = CatchingUpState(
        sharedState = sharedState,
        currentEpoch = startReaderFrom.epoch,
        nextTxnId = startReaderFrom.txnId,
        recoveryFinished = false,
        readerLockAcquired = false
    )
    
    @Volatile
    private var stateRef: JournalState = state

    val isWriter: Boolean get() = stateRef.isWriter

    val isReader: Boolean get() = stateRef.isReader

    override fun onEvent(
        eventContext: JournalEventContext?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        val context = eventContext!!
        val event = context.journalInputEvent ?: return
        val newState = when (event) {
            is JournalInputEvent.TimerEvent -> state.processTimerEvent(event, context)
            is JournalInputEvent.ReaderLagEvent -> state.processReaderLagEvent(event, context)
            is JournalInputEvent.InputDataMessageEvent -> {
                sharedState.offset = event.message.offset
                state.processMessageEvent(event, context)
            }
            is JournalInputEvent.DataWriteEvent -> state.processDataWriteEvent(event, context)
            is JournalInputEvent.LockReaderEvent -> state.processLockReaderEvent(event, context)
        }
        if (state.javaClass != newState.javaClass) {
            stateRef = newState
        }
        state = newState
    }
}
