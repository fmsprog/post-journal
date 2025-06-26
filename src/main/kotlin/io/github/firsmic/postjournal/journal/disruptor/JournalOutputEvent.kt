package io.github.firsmic.postjournal.journal.disruptor

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.api.ReaderRecord
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.kafka.OutputJournalMessage

sealed interface JournalOutputEvent {

    @JvmInline
    value class WriteJournalMessageEvent(val message: OutputJournalMessage) : JournalOutputEvent

    @JvmInline
    value class CompositeEvent(val events: List<JournalOutputEvent>) : JournalOutputEvent

    data object BecameStandbyEvent : JournalOutputEvent

    data object WriterActivatedEvent : JournalOutputEvent

    data object WriterDeactivatedEvent : JournalOutputEvent

    class AcknowledgementEvent(
        val txn: JournalTxn,
        val clientCallbackArg: Any?
    ) : JournalOutputEvent

    @JvmInline
    value class DataReadEvent(val record: ReaderRecord) : JournalOutputEvent

    @JvmInline
    value class EndOfRecoveryEvent(val exception: Throwable?) : JournalOutputEvent

    data class EpochStarted(val epoch: Epoch, val firstTxnId: TxnId) : JournalOutputEvent

    data class EpochFinished(val epoch: Epoch, val lastTxnId: TxnId) : JournalOutputEvent
}