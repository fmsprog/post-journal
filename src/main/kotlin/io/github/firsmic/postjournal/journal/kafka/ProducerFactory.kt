package io.github.firsmic.postjournal.journal.kafka

import org.apache.kafka.clients.producer.KafkaProducer

fun interface ProducerFactory<K, V> {
    fun createProducer(): KafkaProducer<K, V>
}