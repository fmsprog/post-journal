package io.github.firsmic.postjournal.app.eventsource.kafka

import io.github.firsmic.postjournal.app.eventsource.EventSourceId
import io.github.firsmic.postjournal.journal.api.Offset
import kotlinx.serialization.Serializable
import org.apache.kafka.common.TopicPartition

@Serializable
data class KafkaSourceState(
    val sourceId: EventSourceId,
    val latestOffsets: Map<KafkaTopicPartition, Offset>
)

fun KafkaSourceState.getLatestOffset(topicPartition: TopicPartition): Offset? {
    val key = KafkaTopicPartition(topic = topicPartition.topic(), partition = topicPartition.partition())
    return latestOffsets[key]
}

@Serializable
data class KafkaTopicPartition(
    val topic: String,
    val partition: Int
) {
    override fun toString(): String {
        return "$topic-$partition"
    }
}