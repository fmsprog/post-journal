package io.github.firsmic.postjournal.journal.kafka

import org.apache.kafka.clients.producer.ProducerRecord

fun interface KafkaMarshaller<K, V, T> {

    fun marshall(topic: String, partition: Int?, message: T): ProducerRecord<K, V>
}

