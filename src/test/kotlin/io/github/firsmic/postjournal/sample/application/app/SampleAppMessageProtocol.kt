package io.github.firsmic.postjournal.sample.application.app

import io.github.firsmic.postjournal.journal.kafka.KafkaMarshaller
import io.github.firsmic.postjournal.journal.kafka.KafkaUnmarshaller
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

object SampleAppMessageProtocol :
    KafkaMarshaller<String, String, SampleAppMessage>,
    KafkaUnmarshaller<String, String, SampleAppMessage> {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    override fun marshall(topic: String, partition: Int?, message: SampleAppMessage): ProducerRecord<String, String> {
        val jsonString = json.encodeToString(message)
        return ProducerRecord(topic, partition, null, jsonString)
    }

    override fun unmarshall(record: ConsumerRecord<String, String>): SampleAppMessage {
        val jsonString = record.value()
        return json.decodeFromString<SampleAppMessage>(jsonString)
    }
}