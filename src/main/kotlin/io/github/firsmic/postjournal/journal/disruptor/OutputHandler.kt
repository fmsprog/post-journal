package io.github.firsmic.postjournal.journal.disruptor

import io.github.firsmic.postjournal.app.core.topology.BaseEventHandler
import io.github.firsmic.postjournal.journal.api.JournalReaderCallback
import io.github.firsmic.postjournal.journal.api.JournalStabilityListener
import io.github.firsmic.postjournal.journal.api.JournalStateListener
import mu.KLogging

class OutputHandler(
    private val stateListener: JournalStateListener,
    private val stabilityListener: JournalStabilityListener?,
    private val callback: JournalReaderCallback
) : BaseEventHandler<JournalEventContext> {

    companion object : KLogging()

    override fun onEvent(
        context: JournalEventContext?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        val event = context?.outputEvent
        if (event != null) {
            processEvent(event)
        }
    }

    private fun processEvent(event: JournalOutputEvent) {
        when (event) {
            is JournalOutputEvent.CompositeEvent -> event.events.forEach { subEvent ->
                processEvent(subEvent)
            }
            is JournalOutputEvent.WriteJournalMessageEvent -> {
                // ignore
            }
            JournalOutputEvent.BecameStandbyEvent -> stateListener.onStandby()
            JournalOutputEvent.WriterActivatedEvent -> stateListener.onActive()
            JournalOutputEvent.WriterDeactivatedEvent -> stateListener.onShutdown()
            is JournalOutputEvent.AcknowledgementEvent -> {
                stabilityListener?.txnStable(event.txn, event.clientCallbackArg)
            }
            is JournalOutputEvent.DataReadEvent -> callback.onDataRead(event.record)
            is JournalOutputEvent.EndOfRecoveryEvent -> callback.onEndOfRecovery(event.exception)
            is JournalOutputEvent.EpochStarted -> {
                logger.info { "Epoch started: $event" }
            }
            is JournalOutputEvent.EpochFinished -> {
                logger.info { "Epoch finished: $event" }
            }
        }
    }
}