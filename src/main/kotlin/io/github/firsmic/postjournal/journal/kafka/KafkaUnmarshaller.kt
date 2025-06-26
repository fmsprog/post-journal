package io.github.firsmic.postjournal.journal.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

interface KafkaUnmarshaller<K, V, T : Any> {

    fun unmarshall(record: ConsumerRecord<K, V>): T
}