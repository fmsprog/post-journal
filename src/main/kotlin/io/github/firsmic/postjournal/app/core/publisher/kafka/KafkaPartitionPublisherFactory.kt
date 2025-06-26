package io.github.firsmic.postjournal.app.core.publisher.kafka

import io.github.firsmic.postjournal.app.core.publisher.PublisherFactory
import io.github.firsmic.postjournal.journal.kafka.ConsumerFactory
import io.github.firsmic.postjournal.journal.kafka.KafkaMarshaller
import io.github.firsmic.postjournal.journal.kafka.ProducerFactory

class KafkaPartitionPublisherFactory<K : Any, V : Any, T>(
    private val producerFactory: ProducerFactory<K, V>,
    private val consumerFactory: ConsumerFactory<K, V>,
    private val marshaller: KafkaMarshaller<K, V, T>,
    private val nullRecordFactory: KafkaNullRecordFactory<K, V>,
    private val topic: String,
    private val partition: Int,
) : PublisherFactory<T> {

    override fun createPublisher() = KafkaPartitionPublisher(
        marshaller = marshaller,
        nullRecordFactory = nullRecordFactory,
        topic = topic,
        partition = partition,
        producerFactory = producerFactory,
        consumerFactory = consumerFactory
    )
}
