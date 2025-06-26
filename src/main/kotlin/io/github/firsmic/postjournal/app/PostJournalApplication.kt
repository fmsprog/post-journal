package io.github.firsmic.postjournal.app

import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.SleepingWaitStrategy
import io.github.firsmic.postjournal.app.core.EventDispatcher
import io.github.firsmic.postjournal.app.core.LifecycleManager
import io.github.firsmic.postjournal.app.core.event.SaveSnapshotCommand
import io.github.firsmic.postjournal.app.core.publisher.kafka.KafkaNullRecordFactory
import io.github.firsmic.postjournal.app.core.publisher.kafka.KafkaPartitionPublisherFactory
import io.github.firsmic.postjournal.app.core.topology.*
import io.github.firsmic.postjournal.app.eventsink.ChangeSink
import io.github.firsmic.postjournal.app.eventsink.status.ChangeStatusReaderFactory
import io.github.firsmic.postjournal.app.eventsink.status.SinkStatusProtocol
import io.github.firsmic.postjournal.app.eventsource.EventSource
import io.github.firsmic.postjournal.app.state.ApplicationStateManagerFactory
import io.github.firsmic.postjournal.app.state.MarshallerFactory
import io.github.firsmic.postjournal.app.state.SnapshotInfo
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.common.disruptor.DisruptorFactory
import io.github.firsmic.postjournal.journal.api.JournalFactory
import io.github.firsmic.postjournal.journal.kafka.ConsumerFactory
import io.github.firsmic.postjournal.journal.kafka.KafkaConnectionProvider
import io.github.firsmic.postjournal.journal.kafka.createConsumerFactory
import io.github.firsmic.postjournal.journal.kafka.createProducerFactory
import mu.KLogging

class PostJournalApplication<CHANGES : TxnChanges>(
    val config: ApplicationConfig,
    val journalFactory: JournalFactory,
    val stateManagerFactory: ApplicationStateManagerFactory<CHANGES>,
    val changesMarshallerFactory: MarshallerFactory<CHANGES>,
    val eventSources: List<EventSource>,
    val changeSinks: List<ChangeSink<CHANGES>>,
    val kafkaConnectionProvider: KafkaConnectionProvider,
) {
    companion object : KLogging()

    private val journal = journalFactory.createJournal()
    private val waitStrategy = SleepingWaitStrategy(10, 1000000L)

    private val unmarshallerEventHandler = UnmarshallerEventHandler()
    private val businessLogicHandler = BusinessLogicHandler<CHANGES>()
    private val marshallerHandler = MarshallerEventHandler(
        marshaller = changesMarshallerFactory.createMarshaller()
    )

    private val journalStabilityBarrier = TxnSequenceBarrier(
        waitStrategy = waitStrategy
    )

    private val journalWriterHandler = JournalWriterHandler<CHANGES>(
        journal = journal,
        journalStabilityBarrier = journalStabilityBarrier
    )

    private val journalAckWaiter = JournalAckWaiter(
        journalStabilityBarrier = journalStabilityBarrier
    )

    private val sinkEventHandlers = changeSinks.associate { sink ->
        sink.id to SinkEventHandler(
            sink = sink,
            standbyPacingBarrier = TxnSequenceBarrier(waitStrategy)
        )
    }

    private val cleanupHandler = CleanupHandler()

    private val topology = listOf(
        listOf(unmarshallerEventHandler),
        listOf(businessLogicHandler),
        listOf(marshallerHandler),
        listOf(journalWriterHandler),
        listOf(journalAckWaiter),
        sinkEventHandlers.values.toList(),
        listOf(cleanupHandler)
    )

    @Suppress("UNCHECKED_CAST")
    private val disruptorFactory = DisruptorFactory<AppEventContext<CHANGES>>(
        name = "Application",
        ringBufferSize = config.ringBufferSize,
        waitStrategy = waitStrategy,
        eventFactory = { AppEventContext() },
        topologyFactory = {
            topology as List<List<EventHandler<AppEventContext<CHANGES>>>>
        }
    )

    private val disruptor = disruptorFactory.createDisruptor()

    private val sinkStatusPublisherFactory = KafkaPartitionPublisherFactory(
        marshaller = SinkStatusProtocol,
        topic = "standby.pacing",
        partition = 0,
        producerFactory = kafkaConnectionProvider.createProducerFactory(),
        consumerFactory = ConsumerFactory { throw UnsupportedOperationException() },
        nullRecordFactory = KafkaNullRecordFactory { t, p -> throw UnsupportedOperationException() },
    )

    private val sinkStatusReaderFactory = ChangeStatusReaderFactory(
        topic = "standby.pacing",
        consumerFactory = kafkaConnectionProvider.createConsumerFactory()
    )

    val eventDispatcher: EventDispatcher = RingBufferEventDispatcher(disruptor)

    fun takeSnapshot(): SnapshotInfo? {
        if (!journal.acquireReaderLock()) {
            logger.info("Can't take snapshot because current instance is not standby")
            return null
        }
        try {
            val command = SaveSnapshotCommand()
            eventDispatcher.processEvent(command)
            return command.result.join()
        } finally {
            try {
                journal.releaseReaderLock()
            } catch (ex: Throwable) {
                logger.error(ex) { "Exception on releaseStandbyLock" }
            }
        }
    }

    private val lifecycleManager = LifecycleManager(
        eventDispatcher = eventDispatcher,
        journal = journal,
        sinkEventHandlers = sinkEventHandlers.values,
        eventSources = eventSources,
        stateManagerFactory = stateManagerFactory,
        sinkStatusPublisherFactory = sinkStatusPublisherFactory,
        sinkStatusReaderFactory = sinkStatusReaderFactory
    )

    fun start() {
        disruptor.start()
        lifecycleManager.start()
    }

    fun shutdown() {
        lifecycleManager.shutdown()
        // TODO - when to stop disruptor?
        disruptor.shutdown()
    }
}

data class ApplicationConfig(
    val ringBufferSize: Int,
)