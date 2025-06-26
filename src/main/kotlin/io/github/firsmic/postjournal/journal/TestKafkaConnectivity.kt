package io.github.firsmic.postjournal.journal

import io.github.firsmic.postjournal.journal.kafka.DefaultKafkaConnectionProvider
import io.github.firsmic.postjournal.journal.kafka.KafkaDataReader
import io.github.firsmic.postjournal.journal.kafka.PositionSeekMode
import io.github.firsmic.postjournal.journal.kafka.createConsumerFactory
import io.github.firsmic.postjournal.journal.kafka.createProducerFactory
import org.apache.kafka.clients.producer.ProducerRecord

fun main() {
    val producerFactory = DefaultKafkaConnectionProvider.createProducerFactory<String, ByteArray>()
    val producer = producerFactory.createProducer()
    val sent = producer.send(
        ProducerRecord(
            "test-topic",
            0,
            null,
            "key-1",
            byteArrayOf()
        )
    )
    producer.flush()
    println(sent.get())

    KafkaDataReader(
        topic = "test-topic",
        consumerFactory = DefaultKafkaConnectionProvider.createConsumerFactory(),
        eventProcessor = {
            println("received $it")
        }
    ).start {
        PositionSeekMode.Earliest
    }
    Thread.currentThread().join()
}