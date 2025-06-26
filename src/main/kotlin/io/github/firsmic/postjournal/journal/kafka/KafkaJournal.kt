package io.github.firsmic.postjournal.journal.kafka

import com.lmax.disruptor.SleepingWaitStrategy
import io.github.firsmic.postjournal.common.disruptor.DisruptorFactory
import io.github.firsmic.postjournal.journal.api.*
import io.github.firsmic.postjournal.journal.disruptor.*
import io.github.firsmic.postjournal.journal.state.JournalStateManager
import io.github.firsmic.postjournal.journal.state.SystemTimeSource
import io.github.firsmic.postjournal.journal.state.TimeSource
import mu.KLogging
import java.lang.AutoCloseable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class KafkaJournalFactory(
    private val config: KafkaJournalConfig
) : JournalFactory {
    override fun createJournal() = KafkaJournal(config)
}

class KafkaJournal(
    private val config: KafkaJournalConfig,
) : Journal {
    companion object : KLogging()

    private lateinit var stateListener: JournalStateListener
    private var stabilityListener: JournalStabilityListener? = null

    @Volatile
    private var handler: JournalHandler? = null

    override val replicaId: String get() = config.replicaId

    override val isWriter: Boolean get() = handler?.isWriter ?: false

    override val isReader: Boolean get() = handler?.isReader ?: false

    override fun setStateListener(stateListener: JournalStateListener) {
        this.stateListener = stateListener
    }

    override fun setStabilityListener(stabilityListener: JournalStabilityListener) {
        this.stabilityListener = stabilityListener
    }

    override fun write(record: WriterRecord, clientCallbackArg: Any?) {
        handler?.write(record, clientCallbackArg) ?: error("Not writer")
    }

    override fun start(
        mode: JournalMode,
        startReaderFrom: JournalTxn,
        callback: JournalReaderCallback
    ) {
        check(handler == null)
        logger.info { "Start reader from $startReaderFrom" }
        handler = JournalHandler(
            name = "journal",
            config = config,
            mode = mode,
            startReaderFrom = startReaderFrom,
            stateListener = stateListener,
            stabilityListener = stabilityListener,
            callback = callback
        ).apply {
            start()
        }
    }

    override fun acquireReaderLock(): Boolean {
        val h = requireNotNull(handler) {
            "Journal is not started"
        }
        return h.acquireReaderLock()
    }

    override fun releaseReaderLock() {
        val h = requireNotNull(handler) {
            "Journal is not started"
        }
        h.releaseReaderLock()
    }

    override fun destroy() {
        handler?.close()
        handler = null
    }
}

data class KafkaJournalConfig(
    val replicaId: String,
    val topicPrefix: String,
    val partition: Int,
    val connectionProvider: KafkaConnectionProvider = DefaultKafkaConnectionProvider,
    val timeSource: TimeSource = SystemTimeSource,
    val ringBufferSize: Int = 4096,
) {
    val dataTopic: String = "${topicPrefix}journal"
}

private class JournalHandler(
    val name: String,
    val config: KafkaJournalConfig,
    val mode: JournalMode,
    val startReaderFrom: JournalTxn,
    val stateListener: JournalStateListener = object : JournalStateListener {
        override fun onStandby() {}
        override fun onActive() {}
        override fun onShutdown() {}
    },
    val stabilityListener: JournalStabilityListener? = null,
    val callback: JournalReaderCallback
) : AutoCloseable {
    companion object : KLogging()

    private val journalStateManager = JournalStateManager(
        replicaId = config.replicaId,
        timeSource = config.timeSource,
        startReaderFrom = startReaderFrom,
        mode = mode
    )

    private val journalDataWriter = WriterEventHandler(
        publisher = KafkaJournalPublisher(
            marshaller = JournalDataKafkaProtocol,
            producerFactory = config.connectionProvider.createProducerFactory(),
            topic = config.dataTopic,
            partition = config.partition,
        ),
        payloadCreator = ::journalDataMessagePayloadCreator,
    )

    private val kafkaDataReader = KafkaDataReader(
        topic = config.dataTopic,
        consumerFactory = config.connectionProvider.createConsumerFactory(),
        eventProcessor = ::processInputEvent,
    )

    private val dispatcher = OutputHandler(stateListener, stabilityListener, callback)
    private val cleaner = ContextCleaner()

    private val scheduledExecutor = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "[$name] Journal Scheduler")
    }

    private val topology = listOf(
        listOf(journalStateManager),
        listOf(journalDataWriter, dispatcher),
        listOf(cleaner)
    )

    private val disruptorFactory = DisruptorFactory(
        name = "Journal",
        ringBufferSize = config.ringBufferSize,
        waitStrategy = SleepingWaitStrategy(10, 1000000L),
        eventFactory = { JournalEventContext() },
        topologyFactory = { topology }
    )

    private val disruptor = disruptorFactory.createDisruptor()
    private val ringBuffer = disruptor.ringBuffer

    @Volatile
    private var started = false

    val isWriter: Boolean get() = journalStateManager.isWriter
    val isReader: Boolean get() = journalStateManager.isReader

    fun start() {
        check(!started) { "Already started" }
        started = true
        disruptor.start()
        journalDataWriter.start()

        submit { context -> context += JournalOutputEvent.BecameStandbyEvent }

        scheduledExecutor.scheduleWithFixedDelay(
            { submitTimerEventSafe() },
            50,
            50,
            TimeUnit.MILLISECONDS
        )

        kafkaDataReader.start { topicPartition ->
            if (topicPartition.partition() != config.partition) {
                PositionSeekMode.Skip
            }
            val offset = startReaderFrom.offset
            if (offset >= 0) {
                PositionSeekMode.AtOffset(offset)
            } else {
                PositionSeekMode.Earliest
            }
        }
    }

    private inline fun submit(block: (JournalEventContext) -> Unit) {
        val sequence = ringBuffer.next()
        try {
            val context = ringBuffer[sequence]
            block(context)
        } finally {
            ringBuffer.publish(sequence)
        }
    }

    private fun submitTimerEventSafe() {
        try {
            submit { context ->
                context.journalInputEvent = JournalInputEvent.TimerEvent(
                    nanoTime = config.timeSource.nanoTime()
                )
            }
        } catch (ex: Throwable) {
            logger.error(ex) { "Timer task failed" }
        }
    }

    fun write(record: WriterRecord, clientCallbackArg: Any?) {
        if (!isWriter) {
            throw IllegalStateException("Not writer")
        }
        submit { context ->
            context.journalInputEvent = JournalInputEvent.DataWriteEvent(record, clientCallbackArg)
        }
    }

    private fun processInputEvent(messages: List<JournalInputEvent>) {
        if (messages.isEmpty()) {
            return
        }
        val size = messages.size
        val hi =  ringBuffer.next(size)
        val lo = hi - size + 1
        try {
            val iterator = messages.iterator()
            for (seq in lo..hi) {
                ringBuffer[seq].journalInputEvent = iterator.next()
            }
        } finally {
            ringBuffer.publish(lo, hi)
        }
    }

    override fun close() {
        if (!started) return
        started = false
        scheduledExecutor.shutdownNow()
        kafkaDataReader.close()
        journalDataWriter.shutdown()
        disruptor.shutdown()
    }

    private fun readerLock(acquire: Boolean): Boolean {
        val event = JournalInputEvent.LockReaderEvent(acquire)
        submit { context ->
            context.journalInputEvent = event
        }
        return event.result.join()
    }

    fun acquireReaderLock(): Boolean {
        return readerLock(true)
    }

    fun releaseReaderLock() {
        readerLock(false)
    }
}