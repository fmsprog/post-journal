package io.github.firsmic.postjournal.app.core.publisher.kafka

import io.github.firsmic.postjournal.app.core.publisher.BinaryHeader
import io.github.firsmic.postjournal.app.core.publisher.OutMessage
import io.github.firsmic.postjournal.app.core.publisher.OutMessageWithCallbackArg
import io.github.firsmic.postjournal.app.core.publisher.Publisher
import io.github.firsmic.postjournal.app.core.publisher.PublisherStabilityListener
import io.github.firsmic.postjournal.journal.kafka.ConsumerFactory
import io.github.firsmic.postjournal.journal.kafka.KafkaMarshaller
import io.github.firsmic.postjournal.journal.kafka.ProducerFactory
import mu.KLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList

class KafkaPartitionPublisher<K, V, T>(
    private val marshaller: KafkaMarshaller<K, V, T>,
    private val nullRecordFactory: KafkaNullRecordFactory<K, V>,
    private val topic: String,
    private val partition: Int,
    private val producerFactory: ProducerFactory<K, V>,
    private val consumerFactory: ConsumerFactory<K, V>,
) : Publisher<T> {
    companion object : KLogging() {
        const val MSG_INFO_HEADER_NAME = "__msg_info__"
    }

    private var kafkaProducer: KafkaProducer<K, V>? = null
    private val stabilityListeners = CopyOnWriteArrayList<PublisherStabilityListener>()

    override fun start() {
        kafkaProducer = producerFactory.createProducer()
    }

    override fun destroy() {
        kafkaProducer?.close()
        kafkaProducer = null
    }

    override fun addStabilityListener(listener: PublisherStabilityListener) {
        stabilityListeners += listener
    }

    override fun removeStabilityListener(listener: PublisherStabilityListener) {
        stabilityListeners.remove(listener)
    }

    private fun fireStable(clientCallbackArg: Any) {
        stabilityListeners.forEach { listener ->
            try {
                listener.stable(clientCallbackArg)
            } catch (ex: Exception) {
                logger.error(ex) { "Exception in listener $listener" }
            }
        }
    }

    private fun marshall(payload: T?, header: BinaryHeader? = null): ProducerRecord<K, V> {
        val record = if (payload != null) {
            marshaller.marshall(topic, partition, payload)
        } else {
            nullRecordFactory.nullPayloadRecord(topic, partition)
        }
        if (header != null) {
            record.headers().add(MSG_INFO_HEADER_NAME, header.data)
        }
        return record
    }

    override fun publish(
        outMessage: OutMessage<T>,
        clientCallbackArg: Any?
    ) {
        val messageWithCallbackArg = OutMessageWithCallbackArg(outMessage, clientCallbackArg)
        publishBatch(listOf(messageWithCallbackArg))
    }

    override fun publishBatch(batch: List<OutMessageWithCallbackArg<T>>) {
        val producer = requireNotNull(kafkaProducer)
        val futuresToCallbackArg = batch.map { (outMessage, clientCallbackArg) ->
            val producerRecord = when (outMessage) {
                is OutMessage.Message -> marshall(outMessage.message)
                is OutMessage.MessageAndHeader -> marshall(outMessage.message, outMessage.header)
                is OutMessage.Header -> marshall(null, outMessage.header)
            }
            producer.send(producerRecord) to clientCallbackArg
        }
        producer.flush()
        futuresToCallbackArg.forEach { (future, clientCallbackArg) ->
            future.get()
            if (clientCallbackArg != null) {
                fireStable(clientCallbackArg)
            }
        }
    }

    override fun retrieveLastHeader(): BinaryHeader? {
        return consumerFactory.createConsumer().use { consumer ->
            val topicPartition = TopicPartition(topic, partition)

            consumer.assign(listOf(topicPartition))

            val endOffsets = consumer.endOffsets(listOf(topicPartition))
            val endOffset = endOffsets[topicPartition] ?: 0L

            if (endOffset == 0L) {
                logger.debug { "No messages in topic=$topic, partition=$partition" }
                return null
            }
            val lastMessageOffset = endOffset - 1
            consumer.seek(topicPartition, lastMessageOffset)
            val records = consumer.poll(Duration.ofSeconds(30))
            val lastRecord = records.records(topicPartition).lastOrNull()
            if (lastRecord == null) {
                return null
            }
            val headerData = lastRecord.headers().lastHeader(MSG_INFO_HEADER_NAME)?.value()
            if (headerData != null) {
                BinaryHeader(headerData)
            } else {
                null
            }
        }
    }
}
