package io.github.firsmic.postjournal.app.core.publisher.kafka

import org.apache.kafka.clients.producer.ProducerRecord

fun interface KafkaNullRecordFactory<K, V> {
    fun nullPayloadRecord(topic: String, partition: Int): ProducerRecord<K, V>
}