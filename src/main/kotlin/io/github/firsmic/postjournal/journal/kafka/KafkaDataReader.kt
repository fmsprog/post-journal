package io.github.firsmic.postjournal.journal.kafka

import io.github.firsmic.postjournal.journal.disruptor.JournalInputEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

class KafkaDataReader(
    topic: String,
    consumerFactory: ConsumerFactory<String, ByteArray>,
    eventProcessor: (events: List<JournalInputEvent>) -> Unit
) : AbstractKafkaReader<String, ByteArray, JournalInputEvent>(
    topic = topic,
    consumerFactory = consumerFactory,
    eventProcessor = eventProcessor
) {
    override fun processLag(lag: Map<TopicPartition, Long>) {
        val eventList = listOf(
            JournalInputEvent.ReaderLagEvent(lag.values.sum())
        )
        eventProcessor(eventList)
    }

    override fun unmarshall(record: ConsumerRecord<String, ByteArray>): JournalInputEvent {
        val msg = JournalDataKafkaProtocol.unmarshall(record)
        return JournalInputEvent.InputDataMessageEvent(msg)
    }
}
