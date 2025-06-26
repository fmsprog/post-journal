package io.github.firsmic.postjournal.journal.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer

fun interface ConsumerFactory<K, V> {
    fun createConsumer(): KafkaConsumer<K, V>
}
