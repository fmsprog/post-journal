package io.github.firsmic.postjournal.journal.kafka

import mu.KLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.lang.AutoCloseable
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

abstract class AbstractKafkaReader<K, V, T : Any>(
    private val topic: String,
    private val consumerFactory: ConsumerFactory<K, V>,
    private val lagUpdateIntervalMs: Long = LAG_UPDATE_INTERVAL_MS,
    protected val eventProcessor: (events: List<T>) -> Unit
) : AutoCloseable {
    companion object : KLogging() {
        private const val LAG_UPDATE_INTERVAL_MS = 500L
    }

    private val lock = ReentrantLock()
    private val stopped = AtomicBoolean()
    private var thread: Thread? = null
    private var kafkaConsumer: KafkaConsumer<K, V>? = null

    protected open fun processLag(lag: Map<TopicPartition, Long>) {
    }

    protected abstract fun unmarshall(record: ConsumerRecord<K, V>): T

    private fun dataReadTask(
        consumer: KafkaConsumer<K, V>,
        startPointResolver: StartPointResolver,
    ) {
        var lastLagUpdate = System.currentTimeMillis()
        try {
            val assignedTopicPartitions = seekToStartPosition(consumer, startPointResolver)

            while (!stopped.get()) {
                val records = consumer.poll(Duration.ofMillis(100))
                val currentTime = System.currentTimeMillis()
                if (currentTime - lastLagUpdate >= lagUpdateIntervalMs) {
                    processPartitionLagEvent(consumer, assignedTopicPartitions)
                    lastLagUpdate = currentTime
                }

                val batch = records.map { record ->
                    unmarshall(record)
                }
                if (batch.isNotEmpty()) {
                    eventProcessor(batch)
                }
            }
        } catch (_: WakeupException) {
            logger.info { "KafkaReader received wakeup signal [$topic]" }
        } catch (ex: Exception) {
            logger.error(ex) { "Error in KafkaReader loop [$topic]" }
        } finally {
            consumer.close()
            logger.info { "KafkaReader stopped [$topic]" }
        }
    }

    private fun seekToStartPosition(
        consumer: KafkaConsumer<K, V>,
        startPointResolver: StartPointResolver,
    ): List<TopicPartition> {
        val partitions = consumer.partitionsFor(topic).map { it.partition() }
        val assignedPartitions: Map<TopicPartition, PositionSeekMode> = partitions
            .map { TopicPartition(topic, it) }
            .associateWith { topicPartition -> startPointResolver.resolveSeekMode(topicPartition) }
            .filter { it.value !is PositionSeekMode.Skip }

        consumer.assign(assignedPartitions.keys)

        assignedPartitions.forEach { (topicPartition, seekMode) ->
            when (seekMode) {
                is PositionSeekMode.Skip -> {
                    // Already ignored
                }
                is PositionSeekMode.Earliest -> {
                    consumer.seekToBeginning(listOf(topicPartition))
                    val currentOffset = consumer.position(topicPartition)
                    if (currentOffset != 0L) {
                        throw IllegalStateException(
                            "Cannot start journal recovery: expected offset 0, but found $currentOffset"
                        )
                    }
                }

                is PositionSeekMode.AtOffset -> {
                    val nextOffset = seekMode.nextOffset.value
                    consumer.seek(topicPartition, nextOffset)
                    val currentOffset = consumer.position(topicPartition)
                    if (currentOffset != nextOffset) {
                        throw IllegalStateException(
                            "Invalid journal state after seek: expected offset $nextOffset, actual offset $currentOffset"
                        )
                    }
                    logger.info { "KafkaReader started on $topicPartition from offset $nextOffset" }
                }

                is PositionSeekMode.Latest -> {
                    consumer.seekToEnd(listOf(topicPartition))
                    val endOffset = consumer.position(topicPartition)

                    if (seekMode.backlogSize > 0UL) {
                        val backlogSize = seekMode.backlogSize.toLong()
                        val targetOffset = maxOf(0L, endOffset - backlogSize)
                        consumer.seek(topicPartition, targetOffset)
                        val currentOffset = consumer.position(topicPartition)

                        if (currentOffset != targetOffset) {
                            throw IllegalStateException(
                                "Invalid journal state after seek: expected offset $targetOffset, actual offset $currentOffset"
                            )
                        }
                        logger.info {
                            "KafkaReader started on $topicPartition from offset $targetOffset " +
                                    "(backlog: $backlogSize messages, end offset: $endOffset)"
                        }
                    } else {
                        // Read only new messages
                        logger.info {
                            "KafkaReader started on $topicPartition from latest offset $endOffset (new messages only)"
                        }
                    }
                }
            }
        }
        return assignedPartitions.keys.toList()
    }

    private fun processPartitionLagEvent(
        consumer: KafkaConsumer<*, *>,
        topicPartitions: List<TopicPartition>
    ) {
        try {
            val endOffsets = consumer.endOffsets(topicPartitions)
            val currentOffsets = topicPartitions.associateWith {
                consumer.position(it)
            }
            val lag: Map<TopicPartition, Long> = endOffsets.mapValues { (topicPartition, offset) ->
                val current = currentOffsets[topicPartition] ?: 0L
                offset - current
            }.mapKeys { requireNotNull(it.key) }

            processLag(lag)
        } catch (e: Exception) {
            logger.warn(e) { "Failed to update lag [$topic]" }
        }
    }

    fun start(startPointResolver: StartPointResolver) {
        lock.withLock {
            if (thread != null) {
                throw IllegalStateException("Already started [$topic]")
            }
            val consumer = consumerFactory.createConsumer()
            thread = thread(name = "kafka-reader-$topic") {
                dataReadTask(consumer, startPointResolver)
            }
            kafkaConsumer = consumer
        }
    }

    override fun close() {
        lock.withLock {
            kafkaConsumer?.wakeup()
            stopped.set(true)
            thread?.join()
            thread = null
            kafkaConsumer = null
        }
    }
}