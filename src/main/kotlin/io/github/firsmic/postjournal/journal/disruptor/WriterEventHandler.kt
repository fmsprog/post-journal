package io.github.firsmic.postjournal.journal.disruptor

import io.github.firsmic.postjournal.app.core.topology.BaseEventHandler
import io.github.firsmic.postjournal.journal.kafka.KafkaJournalPublisher
import io.github.firsmic.postjournal.journal.kafka.OutputJournalMessage
import mu.KLogging

/**
 * Обработчик событий дизраптора, делегирующий отправку в KafkaPublisher
 */
class WriterEventHandler<T>(
    private val publisher: KafkaJournalPublisher<String, ByteArray, T>,
    private val payloadCreator: (event: JournalOutputEvent) -> List<T>
) : BaseEventHandler<JournalEventContext> {

    companion object : KLogging()

    override fun onEvent(
        event: JournalEventContext?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        val outputEvent = event?.outputEvent ?: return
        val payload = payloadCreator(outputEvent)
        payload.forEach {
            publisher.publish(it)
        }
    }

    fun start() {
        publisher.start()
    }

    fun shutdown() {
        publisher.destroy()
    }
}

fun journalDataMessagePayloadCreator(event: JournalOutputEvent): List<OutputJournalMessage> {
    fun processEvent(event: JournalOutputEvent, dst: MutableList<OutputJournalMessage>) {
        when (event) {
            is JournalOutputEvent.CompositeEvent -> {
                event.events.forEach { childEvent ->
                    processEvent(childEvent, dst)
                }
            }
            is JournalOutputEvent.WriteJournalMessageEvent -> {
                dst += event.message
            }
            else -> {
                // ignore
            }
        }
    }
    return buildList { processEvent(event, this) }
}
