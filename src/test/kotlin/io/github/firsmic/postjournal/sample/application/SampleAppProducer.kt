package io.github.firsmic.postjournal.sample.application

import io.github.firsmic.postjournal.journal.kafka.DefaultKafkaConnectionProvider
import io.github.firsmic.postjournal.journal.kafka.createProducerFactory
import io.github.firsmic.postjournal.sample.application.app.SampleAppMessage
import io.github.firsmic.postjournal.sample.application.app.SampleAppMessageProtocol
import mu.KLogging

const val SAMPLE_APP_INPUT_TOPIC = "sample-app-source"

class SampleAppProducer {
    companion object : KLogging() {
        private val session = System.currentTimeMillis()
    }

    fun run() {
        val producerFactory = DefaultKafkaConnectionProvider.createProducerFactory<String, String>()
        val producer = producerFactory.createProducer()
        for (i in 1..1000000) {
            try {
                val key = "key-$i"
                val value = "value-$session-$i"
                val message = SampleAppMessage(
                    key = key,
                    value = value
                )
                val record = SampleAppMessageProtocol.marshall(SAMPLE_APP_INPUT_TOPIC, partition = 0, message)
                val sent = producer.send(record)
                producer.flush()
                sent.get()
                logger.info { "Publish message: $message" }
            } catch (ex: Exception) {
                logger.error(ex) { }
            }
            Thread.sleep(3000)
        }
    }
}

fun main() {
    println("Started sample app")
    SampleAppProducer().run()
}