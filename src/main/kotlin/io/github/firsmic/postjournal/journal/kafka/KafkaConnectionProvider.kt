package io.github.firsmic.postjournal.journal.kafka

import kotlin.reflect.KClass

interface KafkaConnectionProvider {

    fun <K : Any, V : Any> createConsumerFactory(
        keyType: KClass<K>,
        valueType: KClass<V>,
    ): ConsumerFactory<K, V>

    fun <K, V> createConsumerFactory(
        keyType: Class<K>,
        valueType: Class<V>,
    ): ConsumerFactory<K, V>

    fun <K, V> createProducerFactory(keyType: Class<K>, valueType: Class<V>): ProducerFactory<K, V>

    fun <K : Any, V : Any> createProducerFactory(keyType: KClass<K>, valueType: KClass<V>): ProducerFactory<K, V>
}

inline fun <reified K : Any, reified V : Any> KafkaConnectionProvider.createConsumerFactory(): ConsumerFactory<K, V> {
    return createConsumerFactory(K::class, V::class)
}

inline fun <reified K : Any, reified V : Any> KafkaConnectionProvider.createProducerFactory(): ProducerFactory<K, V> {
    return createProducerFactory(K::class, V::class)
}
