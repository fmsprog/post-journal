package io.github.firsmic.postjournal.app.eventsource.kafka

import io.github.firsmic.postjournal.app.core.EventDispatcher
import io.github.firsmic.postjournal.app.eventsource.EventSource
import io.github.firsmic.postjournal.app.eventsource.EventSourceId
import io.github.firsmic.postjournal.journal.kafka.AbstractKafkaReader
import io.github.firsmic.postjournal.journal.kafka.ConsumerFactory
import io.github.firsmic.postjournal.journal.kafka.KafkaUnmarshaller
import io.github.firsmic.postjournal.journal.kafka.PositionSeekMode
import mu.KLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaEventSource<K, V, T : Any>(
    override val sourceId: EventSourceId,
    private val topic: String,
    private val consumerFactory: ConsumerFactory<K, V>,
    private val kafkaUnmarshaller: KafkaUnmarshaller<K, V, T>,
) : EventSource {
    companion object : KLogging()

    private lateinit var reader: KafkaMessageEventReader<K, V, T>
    private lateinit var eventDispatcher: EventDispatcher

    @Volatile
    override var isStarted: Boolean = false
        private set

    override fun registerEventDispatcher(eventDispatcher: EventDispatcher) {
        this.eventDispatcher = eventDispatcher
    }

    override fun start() {
        val query = KafkaSourceStateQuery(sourceId)
        eventDispatcher.processEvent(query)
        val state = query.result.join()
        logger.info { "Source state '$sourceId': $state" }

        reader = KafkaMessageEventReader(
            sourceId = sourceId,
            topic = topic,
            consumerFactory = consumerFactory,
            kafkaUnmarshaller = kafkaUnmarshaller,
            eventProcessor = { events ->
                events.forEach { event ->
                    eventDispatcher.processEvent(event)
                }
            }
        )

        reader.start { topicPartition ->
            val offset = state?.getLatestOffset(topicPartition)
            if (offset != null) {
                PositionSeekMode.AtOffset(offset + 1)
            } else {
                PositionSeekMode.Earliest
            }
        }
    }

    override fun stop() {
        reader.close()
    }
}

class KafkaMessageEventReader<K, V, T : Any>(
    val sourceId: EventSourceId,
    topic: String,
    consumerFactory: ConsumerFactory<K, V>,
    private val kafkaUnmarshaller: KafkaUnmarshaller<K, V, T>,
    eventProcessor: (events: List<KafkaMessageEvent<K, V, T>>) -> Unit
) : AbstractKafkaReader<K, V, KafkaMessageEvent<K, V, T>>(
    topic = topic,
    lagUpdateIntervalMs = 1000,
    consumerFactory = consumerFactory,
    eventProcessor = eventProcessor
) {
    override fun unmarshall(record: ConsumerRecord<K, V>): KafkaMessageEvent<K, V, T> {
        return KafkaMessageEvent(
            sourceId = sourceId,
            record = record,
            kafkaUnmarshaller = kafkaUnmarshaller
        )
    }
}