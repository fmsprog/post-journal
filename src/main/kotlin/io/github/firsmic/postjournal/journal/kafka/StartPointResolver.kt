package io.github.firsmic.postjournal.journal.kafka

import org.apache.kafka.common.TopicPartition

fun interface StartPointResolver {
    fun resolveSeekMode(topicPartition: TopicPartition): PositionSeekMode
}