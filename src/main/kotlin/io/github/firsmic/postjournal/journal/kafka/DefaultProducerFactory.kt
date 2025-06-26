package io.github.firsmic.postjournal.journal.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import kotlin.reflect.KClass

class DefaultProducerFactory<K, V>(
    private val bootstrapServers: String,
    private val keyType: Class<K>,
    private val valueType: Class<V>,
) : ProducerFactory<K, V> {
    override fun createProducer(
    ): KafkaProducer<K, V> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializerClass(keyType).java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getSerializerClass(valueType).java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")

            // Critical low-latency settings
            put(ProducerConfig.RETRIES_CONFIG, 0); // No retries for lowest latency
            put(ProducerConfig.LINGER_MS_CONFIG, 0); // Send immediately
            put(ProducerConfig.BATCH_SIZE_CONFIG, 1); // Minimal batching
            put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer

            // Network optimization
            put(ProducerConfig.SEND_BUFFER_CONFIG, 131072); // 128KB
            put(ProducerConfig.RECEIVE_BUFFER_CONFIG, 32768); // 32KB
            put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000); // 1 second timeout
            put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000); // 2 seconds total

            // Compression (disable for lowest latency, enable for network-bound scenarios)
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

            // Advanced settings
            put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Ensure ordering
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // Disable for lowest latency
        }
        return KafkaProducer(props)
    }
}

@Suppress("UNCHECKED_CAST")
private fun <T> getSerializerClass(type: Class<T>): KClass<out Serializer<T>> {
    val deserializerClass = when (type) {
        String::class.java ->  StringSerializer::class
        ByteArray::class.java -> ByteArraySerializer::class
        else -> throw IllegalArgumentException("Unsupported type: ${type.simpleName}")
    }
    return deserializerClass as KClass<out Serializer<T>>
}