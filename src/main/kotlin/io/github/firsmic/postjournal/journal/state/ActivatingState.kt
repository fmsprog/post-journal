package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.disruptor.JournalEventContext
import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent
import io.github.firsmic.postjournal.journal.disruptor.JournalOutputEvent
import io.github.firsmic.postjournal.journal.kafka.JournalInputMessage
import io.github.firsmic.postjournal.journal.kafka.OutputJournalMessage
import java.util.UUID

internal class ActivatingState(
    sharedState: SharedState,
    currentEpoch: Epoch,
    nextTxnId: TxnId,
) : RecoveringState(
    sharedState,
    currentEpoch,
    nextTxnId,
    false,
    false
) {
    private val proposedEpochNumber = currentEpoch.epochNumber + 1
    private val proposeEpochMessageId: String = replicaId + "-" + UUID.randomUUID().toString()

    fun onActivating(context: JournalEventContext) {
        context += JournalOutputEvent.WriteJournalMessageEvent(
            OutputJournalMessage.NewEpochProposal(
                epochNumber = proposedEpochNumber,
                messageId = proposeEpochMessageId,
                replicaId = replicaId,
                timestamp = timeSource.currentTimeMillis(),
            )
        )
    }

    override fun processTimerEvent(event: JournalInputEvent.TimerEvent, context: JournalEventContext): JournalState {
        // Ignore
        return this
    }

    override fun processReaderLagEvent(event: JournalInputEvent.ReaderLagEvent, context: JournalEventContext): JournalState {
        // Ignore
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

    private fun isMyEpochProposalResponse(message: JournalInputEvent.InputDataMessageEvent): Boolean {
        val msg = message.message
        return if (msg is JournalInputMessage.NewEpoch) {
            msg.messageId == proposeEpochMessageId && msg.epoch.epochNumber == proposedEpochNumber
        } else {
            false
        }
    }

    override fun processMessageEvent(event: JournalInputEvent.InputDataMessageEvent, context: JournalEventContext): JournalState {
        val journalMessage = event.message
        if (journalMessage.epoch < currentEpoch) {
            // it's possible on message lost and actually unexpected.
            return if (isMyEpochProposalResponse(event)) {
                logger.info { "Epoch proposal rejected: $event. New leader is '${journalMessage.replicaId}'." }
                InSyncState(sharedState, currentEpoch, nextTxnId, recoveryFinished, false)
            } else {
                // ignore a message with older epoch
                return this
            }
        }
        else if (journalMessage.epoch == currentEpoch) {
            val processed = handleJournalMessage(event, context)
            if (!processed) throw IllegalStateException(
                "Message at offset ${journalMessage.offset} was not processed"
            )
            return this
        } else {
            // The new epoch received.
            // Two options are possible:
            // - my epoch proposal accepted: I'm a leader now
            // - another epoch proposal accepted: another replica is leader
            currentEpoch = journalMessage.epoch
            when (journalMessage) {
                is JournalInputMessage.NewEpoch -> {
                    return if (isMyEpochProposalResponse(event)) {
                        // my request completed
                        ActiveState(sharedState, currentEpoch, nextTxnId).apply {
                            onActive(context)
                        }
                    } else {
                        logger.info { "Epoch proposal rejected: $event" }
                        InSyncState(sharedState, currentEpoch, nextTxnId, recoveryFinished, false)
                    }
                }
                is JournalInputMessage.Heartbeat -> {
                    // ignore
                }
                is JournalInputMessage.Data -> {
                    applyIncomingData(journalMessage, context)
                }
            }
            return InSyncState(sharedState, currentEpoch, nextTxnId, recoveryFinished, false)
        }
    }
}
