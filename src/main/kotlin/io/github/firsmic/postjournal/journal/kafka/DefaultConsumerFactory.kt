package io.github.firsmic.postjournal.journal.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*
import kotlin.reflect.KClass

class DefaultConsumerFactory<K, V>(
    private val bootstrapServers: String,
    private val keyType: Class<K>,
    private val valueType: Class<V>,
) : ConsumerFactory<K, V> {

    override fun createConsumer(): KafkaConsumer<K, V> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getDeserializerClass(keyType).java.getName())
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getDeserializerClass(valueType).java.getName())

            // Critical low-latency settings
            put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) // Fetch immediately
            put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1) // Minimal wait time
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1024)
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000) // 5 minutes

            // Offset management
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

            // Network optimization
            put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 33554432) // 32MB buffer
            put(ConsumerConfig.SEND_BUFFER_CONFIG, 131072) // 128KB
            put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000)
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000)
            put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000)
        }
        return KafkaConsumer(props)
    }
}

@Suppress("UNCHECKED_CAST")
private fun <T> getDeserializerClass(type: Class<T>): KClass<out Deserializer<T>> {
    val deserializerClass = when (type) {
        String::class.java ->  StringDeserializer::class
        ByteArray::class.java -> ByteArrayDeserializer::class
        else -> throw IllegalArgumentException("Unsupported type: ${type.simpleName}")
    }
    return deserializerClass as KClass<out Deserializer<T>>
}