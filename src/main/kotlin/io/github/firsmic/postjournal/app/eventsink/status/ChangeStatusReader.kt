package io.github.firsmic.postjournal.app.eventsink.status

import io.github.firsmic.postjournal.journal.kafka.AbstractKafkaReader
import io.github.firsmic.postjournal.journal.kafka.ConsumerFactory
import org.apache.kafka.clients.consumer.ConsumerRecord

class ChangeStatusReader(
    topic: String,
    consumerFactory: ConsumerFactory<String, String>,
    eventProcessor: (events: List<SinkStatusMessage>) -> Unit
) : AbstractKafkaReader<String, String, SinkStatusMessage>(
    topic = topic,
    lagUpdateIntervalMs = Long.MAX_VALUE, // do not update lag
    consumerFactory = consumerFactory,
    eventProcessor = eventProcessor
) {
    override fun unmarshall(record: ConsumerRecord<String, String>): SinkStatusMessage {
        return SinkStatusProtocol.unmarshall(record)
    }
}