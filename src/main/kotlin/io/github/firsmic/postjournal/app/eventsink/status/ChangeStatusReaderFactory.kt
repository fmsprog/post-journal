package io.github.firsmic.postjournal.app.eventsink.status

import io.github.firsmic.postjournal.journal.kafka.ConsumerFactory

class ChangeStatusReaderFactory(
    private val topic: String,
    private val consumerFactory: ConsumerFactory<String, String>
) {
    fun createChangeStatusReader(
        eventProcessor: (events: List<SinkStatusMessage>) -> Unit
    ) = ChangeStatusReader(
        topic = topic,
        consumerFactory = consumerFactory,
        eventProcessor = eventProcessor
    )
}