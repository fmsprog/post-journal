package io.github.firsmic.postjournal.app.eventsink.status

import io.github.firsmic.postjournal.journal.kafka.KafkaMarshaller
import io.github.firsmic.postjournal.journal.kafka.KafkaUnmarshaller
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

object SinkStatusProtocol :
    KafkaMarshaller<String, String, SinkStatusMessage>,
    KafkaUnmarshaller<String, String, SinkStatusMessage> {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    override fun marshall(topic: String, partition: Int?, message: SinkStatusMessage): ProducerRecord<String, String> {
        val jsonString = json.encodeToString(message)
        return ProducerRecord(topic, partition, null, jsonString)
    }

    override fun unmarshall(record: ConsumerRecord<String, String>): SinkStatusMessage {
        val jsonString = record.value()
        return json.decodeFromString<SinkStatusMessage>(jsonString)
    }
}

@Serializable
data class SinkStatusMessage(
    // May be used txn id without Epoch because we publish status of stable transactions here
    val latestStableTransactions: Map<String, Long>
)
