package io.github.firsmic.postjournal.sample.application.app


import io.github.firsmic.postjournal.app.ApplicationConfig
import io.github.firsmic.postjournal.app.PostJournalApplication
import io.github.firsmic.postjournal.app.core.publisher.kafka.KafkaPartitionPublisherFactory
import io.github.firsmic.postjournal.app.eventsink.ChangeSink
import io.github.firsmic.postjournal.app.eventsink.ChangeSinkId
import io.github.firsmic.postjournal.app.eventsink.publisher.PublisherChangeSink
import io.github.firsmic.postjournal.app.eventsource.EventSourceId
import io.github.firsmic.postjournal.app.eventsource.kafka.KafkaEventSource
import io.github.firsmic.postjournal.journal.kafka.*
import io.github.firsmic.postjournal.sample.application.SAMPLE_APP_INPUT_TOPIC
import org.apache.kafka.clients.producer.ProducerRecord

class SampleApp(
    val replicaId: String,
) {
    private val stateManagerFactory = SampleStateManagerFactory(
        stateUpdateMarshallerFactory = { SampleMarshaller }
    )

    val application = createApplication()

    fun start() {
        application.start()
    }

    fun shutdown() {
        application.shutdown()
    }

    private fun createApplication(): PostJournalApplication<SampleChanges> {
        return PostJournalApplication(
            config = ApplicationConfig(ringBufferSize = 4096),
            journalFactory = KafkaJournalFactory(
                config = KafkaJournalConfig(
                    replicaId = replicaId,
                    topicPrefix = "app.",
                    partition = 0
                )
            ),
            stateManagerFactory = stateManagerFactory,
            changesMarshallerFactory = { SampleMarshaller },
            eventSources = listOf(
                createKafkaSource(SAMPLE_APP_INPUT_TOPIC)
            ),
            changeSinks = listOf(
                createKafkaChangeSink("publisher-1"),
                createKafkaChangeSink("publisher-2"),
            ),
            kafkaConnectionProvider = DefaultKafkaConnectionProvider,
        )
    }


    private fun createKafkaChangeSink(sinkId: String): ChangeSink<SampleChanges> {
        val topic = "sample-app-output-$sinkId"
        return PublisherChangeSink(
            id = ChangeSinkId(sinkId),
            payloadCreator = { changes ->
                when (changes) {
                    is SampleChanges.AddKey -> listOf(
                        // message = Pair<String, String>
                        changes.key to "${sinkId}-${changes.txnId}-${changes.value}"
                    )
                    is SampleChanges.InitComplete -> emptyList()
                }
            },
            publisherFactory = KafkaPartitionPublisherFactory(
                producerFactory = DefaultKafkaConnectionProvider.createProducerFactory(),
                consumerFactory = DefaultKafkaConnectionProvider.createConsumerFactory(),
                marshaller = { topic, partition, message ->
                    ProducerRecord<String, String>(topic, partition, message.first, message.second)
                },
                nullRecordFactory = { topic, partition ->
                    ProducerRecord<String, String>(topic, partition, null, "")
                },
                topic = topic,
                partition = 0
            )
        )
    }

    private fun createKafkaSource(topic: String): KafkaEventSource<String, String, SampleAppMessage> {
        return KafkaEventSource(
            sourceId = EventSourceId("sample-source-$topic"),
            topic = topic,
            consumerFactory = DefaultKafkaConnectionProvider.createConsumerFactory(),
            kafkaUnmarshaller = SampleAppMessageProtocol,
        )
    }
}
