package io.github.firsmic.postjournal.journal.disruptor

import io.github.firsmic.postjournal.journal.api.WriterRecord
import io.github.firsmic.postjournal.journal.kafka.JournalInputMessage
import java.util.concurrent.CompletableFuture

sealed interface JournalInputEvent {

    @JvmInline
    value class InputDataMessageEvent(
        val message: JournalInputMessage,
    ) : JournalInputEvent

    @JvmInline
    value class ReaderLagEvent(
        val lag: Long
    ): JournalInputEvent

    @JvmInline
    value class TimerEvent(
        val nanoTime: Long
    ) : JournalInputEvent

    class DataWriteEvent(
        val writerRecord: WriterRecord,
        val clientCallbackArg: Any?
    ): JournalInputEvent

    class LockReaderEvent(
        val acquire: Boolean,
    ) : JournalInputEvent {
        val result = CompletableFuture<Boolean>()
    }
}