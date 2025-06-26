package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.*
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent
import io.github.firsmic.postjournal.journal.disruptor.JournalOutputEvent
import io.github.firsmic.postjournal.journal.kafka.JournalInputMessage
import io.github.firsmic.postjournal.journal.kafka.OutputJournalMessage
import java.util.*
import java.util.concurrent.TimeUnit

internal class ActiveState(
    sharedState: SharedState,
    currentEpoch: Epoch,
    nextTxnId: TxnId,
) : JournalState(
    sharedState,
    currentEpoch,
    nextTxnId
) {
    override val isReader: Boolean get() = false
    override val isWriter: Boolean get() = true

    private var lastSentMessageNanos: Long = timeSource.nanoTime()
    private val inFlightAcknowledgments = LinkedList<TxnIdAndArg>()

    private class TxnIdAndArg(
        val txnId: TxnId,
        val clientCallbackArg: Any?
    )

    override fun processTimerEvent(event: JournalInputEvent.TimerEvent, context: JournalEventContext): JournalState {
        val pauseMs = TimeUnit.NANOSECONDS.toMillis(timeSource.nanoTime() - lastSentMessageNanos)
        if (pauseMs >= SEND_LEADER_HEARTBEAT_INTERVAL_MS) {
            context += JournalOutputEvent.WriteJournalMessageEvent(
                OutputJournalMessage.Heartbeat(
                    epoch = currentEpoch,
                    replicaId = replicaId,
                    timestamp = timeSource.currentTimeMillis(),
                    isLeader = true
                )
            )
            lastSentMessageNanos = timeSource.nanoTime()
        }
        return this
    }

    override fun processReaderLagEvent(event: JournalInputEvent.ReaderLagEvent, context: JournalEventContext): JournalState {
        return this
    }

    override fun processDataWriteEvent(event: JournalInputEvent.DataWriteEvent, context: JournalEventContext): JournalState {
        val record = event.writerRecord
        val txnId = record.txnId
        validateCurrentTxnId(txnId)
        inFlightAcknowledgments += TxnIdAndArg(txnId, event.clientCallbackArg)
        lastSentMessageNanos = timeSource.nanoTime()
        context += JournalOutputEvent.WriteJournalMessageEvent(
            OutputJournalMessage.Data(
                epoch = currentEpoch,
                txnId = txnId,
                payload = record.payload,
                replicaId = replicaId,
                timestamp = timeSource.currentTimeMillis(),
            )
        )
        nextTxnId = txnId + 1
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

    private fun validateReceivedDataMessage(message: JournalInputMessage.Data): Boolean {
        if (message.replicaId != replicaId) {
            logger.error {
                "Received invalid data message: ${message.toSensitiveSafeString()}. " +
                        "Current replica $replicaId has same epoch $currentEpoch. Ignore message."
            }
            return false
        }
        if (message.txnId >= nextTxnId) {
            logger.error {
                "Received invalid data message: ${message.toSensitiveSafeString()}. " +
                        "Received txnId is greater than last sent ${nextTxnId - 1}. Ignore message. "
            }
            return false
        }
        return true
    }

    private fun acknowledge(txn: JournalTxn, context: JournalEventContext) {
        if (inFlightAcknowledgments.isEmpty()) {
            // ignore
        } else {
            val first = inFlightAcknowledgments.first()
            when {
                txn.txnId == first.txnId -> {
                    context += JournalOutputEvent.AcknowledgementEvent(
                        txn = txn,
                        clientCallbackArg = first.clientCallbackArg,
                    )
                    inFlightAcknowledgments.removeFirst()
                }
                else -> {
                    logger.error {
                        "Out of order acknowledgement. Expected txnId = ${first.txnId} but received ${txn.txnId}"
                    }
                    null
                }
            }
        }
    }

    override fun processMessageEvent(event: JournalInputEvent.InputDataMessageEvent, context: JournalEventContext): JournalState {
        val journalMessage = event.message
        val epoch = journalMessage.epoch
        if (epoch < currentEpoch) {
            return this
        } else if (epoch == currentEpoch) {
            when (journalMessage) {
                is JournalInputMessage.Heartbeat -> {
                    // ignore
                }
                is JournalInputMessage.NewEpoch -> {
                    // ignore

                }
                is JournalInputMessage.Data -> {
                    if (!validateReceivedDataMessage(journalMessage)) {
                        return this
                    }
                    val journalTxn = JournalTxn(
                        epoch = journalMessage.epoch,
                        offset = journalMessage.offset,
                        txnId = journalMessage.txnId
                    )
                    acknowledge(journalTxn, context)
                }
            }
            return this
        } else {
            logger.info { "New leader '${journalMessage.replicaId}' started epoch $epoch. Shutdown current active node." }
            return ShutdownWriterState(sharedState, NullEpoch, TxnId.Companion.MAX).apply {
                onShutdown(context)
            }
        }
    }

    fun onActive(context: JournalEventContext) {
        context += JournalOutputEvent.WriterActivatedEvent
    }
}