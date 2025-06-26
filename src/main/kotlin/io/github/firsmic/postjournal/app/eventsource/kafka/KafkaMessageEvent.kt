package io.github.firsmic.postjournal.app.eventsource.kafka

import io.github.firsmic.postjournal.app.core.event.ApplicationEvent
import io.github.firsmic.postjournal.app.core.event.UnmarshallableEvent
import io.github.firsmic.postjournal.app.eventsource.EventSourceId
import io.github.firsmic.postjournal.journal.api.Offset
import io.github.firsmic.postjournal.journal.kafka.KafkaUnmarshaller
import kotlinx.serialization.Serializable
import org.apache.kafka.clients.consumer.ConsumerRecord

data class KafkaMessageEvent<K, V, T : Any>(
    val sourceId: EventSourceId,
    val record: ConsumerRecord<K, V>,
    val kafkaUnmarshaller: KafkaUnmarshaller<K, V, T>
) : ApplicationEvent(), UnmarshallableEvent {

    val messageInfo = KafkaMessageInfo(
        sourceId = sourceId,
        topicPartition = KafkaTopicPartition(
            topic = record.topic(),
            partition = record.partition()
        ),
        offset = Offset.of(record.offset())
    )

    lateinit var unmarshalledPayload: T

    override fun unmarshall() {
        unmarshalledPayload = kafkaUnmarshaller.unmarshall(record)
    }
}

@Serializable
data class KafkaMessageInfo(
    val sourceId: EventSourceId,
    val topicPartition: KafkaTopicPartition,
    val offset: Offset
)