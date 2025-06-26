package io.github.firsmic.postjournal.journal.kafka

import kotlin.reflect.KClass

object DefaultKafkaConnectionProvider : KafkaConnectionProvider {
    private const val DOCKER_BOOTSTRAP_SERVERS = "localhost:9092"

    override fun <K : Any, V : Any> createConsumerFactory(
        keyType: KClass<K>,
        valueType: KClass<V>
    ): ConsumerFactory<K, V> {
        return createConsumerFactory(keyType.java, valueType.java)
    }

    override fun <K, V> createConsumerFactory(keyType: Class<K>, valueType: Class<V>): ConsumerFactory<K, V> {
        return DefaultConsumerFactory(DOCKER_BOOTSTRAP_SERVERS, keyType, valueType)
    }

    override fun <K, V> createProducerFactory(keyType: Class<K>, valueType: Class<V>): ProducerFactory<K, V> {
        return DefaultProducerFactory(DOCKER_BOOTSTRAP_SERVERS, keyType, valueType)
    }

    override fun <K : Any, V : Any> createProducerFactory(keyType: KClass<K>, valueType: KClass<V>): ProducerFactory<K, V> {
        return DefaultProducerFactory(DOCKER_BOOTSTRAP_SERVERS, keyType.java, valueType.java)
    }
}