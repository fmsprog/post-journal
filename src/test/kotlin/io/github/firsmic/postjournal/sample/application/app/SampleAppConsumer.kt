package io.github.firsmic.postjournal.sample.application.app

import io.github.firsmic.postjournal.journal.kafka.DefaultKafkaConnectionProvider
import io.github.firsmic.postjournal.journal.kafka.createConsumerFactory
import mu.KLogging
import org.apache.kafka.common.TopicPartition
import java.time.Duration

class SampleAppConsumer(
    val topic: String
) {
    companion object : KLogging()

    fun run() {
        val consumerFactory = DefaultKafkaConnectionProvider.createConsumerFactory<String, String>()
        val consumer = consumerFactory.createConsumer()

        try {
            // Get information about topic partitions
            val partitionInfos = consumer.partitionsFor(topic)
            val topicPartitions = partitionInfos.map { TopicPartition(topic, it.partition()) }

            // Assign partitions manually (without consumer group)
            consumer.assign(topicPartitions)

            // Move to the end of each partition
            consumer.seekToEnd(topicPartitions)

            logger.info { "Assigned to topic: $topic, partitions: ${topicPartitions.map { it.partition() }}" }
            logger.info { "Seeking to end of partitions, waiting for new messages..." }

            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))

                if (records.isEmpty) {
                    continue
                }

                for (record in records) {
                    val key = record.key()
                    val value = record.value()
                    val offset = record.offset()
                    logger.info { "offset=[$offset] key=[$key]: $value" }
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error while consuming messages" }
        } finally {
            consumer.close()
            logger.info { "Consumer closed" }
        }
    }
}