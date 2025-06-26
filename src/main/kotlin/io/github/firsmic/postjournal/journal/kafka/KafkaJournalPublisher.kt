package io.github.firsmic.postjournal.journal.kafka

import mu.KLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


class KafkaJournalPublisher<K, V, T>(
    private val marshaller: KafkaMarshaller<K, V, T>,
    private val topic: String,
    private val partition: Int? = null,
    private val producerFactory: ProducerFactory<K, V>,
    private val maxBatchSize: Int = 1000,
    private val flushIntervalMs: Long = 10,
) {
    companion object : KLogging()

    private val buffer = mutableListOf<ProducerRecord<K, V>>()
    private val lock = ReentrantLock()
    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    @Volatile
    private var kafkaProducer: KafkaProducer<K, V>? = null

    @Volatile
    private var running = true

    fun start() {
        kafkaProducer = producerFactory.createProducer()
        scheduler.scheduleAtFixedRate({
            try {
                flush()
            } catch (e: Exception) {
                logger.error(e) { "Publish error" }
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS)
    }

    fun destroy() {
        running = false
        scheduler.shutdown()
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow()
            }
        } catch (_: InterruptedException) {
            scheduler.shutdownNow()
        }

        kafkaProducer?.close()
        kafkaProducer = null
    }

    /**
     * Добавляет сообщение в буфер для отправки
     * @param message сообщение для отправки
     */
    fun publish(message: T) {
        if (!running) {
            return
        }
        val producerRecord = marshaller.marshall(topic, partition, message)
        lock.withLock {
            buffer.add(producerRecord)
            if (buffer.size >= maxBatchSize) {
                flushLocked()
            }
        }
    }

    /**
     * Принудительно отправляет все сообщения из буфера
     */
    fun flush() {
        lock.withLock {
            flushLocked()
        }
    }

    private fun flushLocked() {
        if (buffer.isEmpty()) return

        val batch = buffer.toList()
        buffer.clear()

        while (true) {
            try {
                kafkaProducer!!.let { producer ->
                    val futures = batch.map { producer.send(it) }
                    producer.flush()
                    futures.forEach { it.get() }
                }
                break
            } catch (ex: Throwable) {
                logger.error(ex) { "Failed to publish batch" }
                if (!running) {
                    throw IllegalStateException("Not running")
                }
                // TODO - exponential back-off
                Thread.sleep(1000)
            }
        }
    }
}
