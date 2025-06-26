package io.github.firsmic.postjournal.app.eventsink.status

import io.github.firsmic.postjournal.app.core.publisher.OutMessage
import io.github.firsmic.postjournal.app.core.publisher.Publisher
import io.github.firsmic.postjournal.app.core.publisher.PublisherFactory
import io.github.firsmic.postjournal.app.core.topology.SinkEventHandler
import io.github.firsmic.postjournal.app.eventsink.ChangeSinkId
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.kafka.PositionSeekMode
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ChangeSinkStatusSynchronizer(
    val sinkEventHandlers: Collection<SinkEventHandler<*>>,
    val publisherFactory: PublisherFactory<SinkStatusMessage>,
    val readerFactory: ChangeStatusReaderFactory,
    initialStableTransactions: Map<ChangeSinkId, TxnId>,
    private val sinkStatusPublishIntervalMs: Long = 100,
) {
    private val sequenceBarriers = sinkEventHandlers.associate { sinkEventHandler ->
        val sink = sinkEventHandler.sink
        val id = sink.id
        val barrier = sinkEventHandler.standbyPacingBarrier
        val initTxn = initialStableTransactions[id] ?: TxnId.NULL
        barrier.set(initTxn)
        sink.id to barrier
    }

    private val stablePublishedTransactions = sinkEventHandlers.map { it.sink }.associate { sink ->
        sink.id to AtomicLong()
    }

    private val publishScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private lateinit var statusPublisher: Publisher<SinkStatusMessage>
    @Volatile
    private var state = State.INIT
    private val lock = ReentrantLock()

    fun toActive() = lock.withLock {
        // Register stability listener for each sink
        check(state == State.STANDBY) {
            "Can't start publisher in state $state"
        }
        state = State.ACTIVE
        sequenceBarriers.forEach { (_, barrier) ->
            barrier.set(TxnId.MAX)
        }
        sinkEventHandlers.map { it.sink }.forEach { sink ->
            val stableTxnHolder = stablePublishedTransactions[sink.id] ?: throw IllegalStateException(
                "Can't find stable txn holder for ${sink.id}"
            )
            sink.addStabilityListener { txnId ->
                stableTxnHolder.lazySet(txnId.value)
            }
        }
        statusPublisher = publisherFactory.createPublisher()
        statusPublisher.start()

        // Start periodic publishing task
        publishScheduler.scheduleAtFixedRate(
            { publishStatus(statusPublisher) },
            sinkStatusPublishIntervalMs,
            sinkStatusPublishIntervalMs,
            TimeUnit.MILLISECONDS
        )
    }

    fun toStandby() = lock.withLock {
        check(state == State.INIT) {
            "Can't start listener in state $state"
        }
        state = State.STANDBY

        val reader = readerFactory.createChangeStatusReader { messages ->
            if (messages.isNotEmpty()) {
                val message = messages.last()
                message.latestStableTransactions.forEach { (sinkIdStr, txnIdValue) ->
                    val sinkId = ChangeSinkId(sinkIdStr)
                    val txnId = TxnId(txnIdValue)
                    sequenceBarriers[sinkId]?.advance(txnId)
                }
            }
        }
        reader.start {
            PositionSeekMode.Latest(backlogSize = 10.toULong())
        }
    }

    private fun publishStatus(publisher: Publisher<SinkStatusMessage>) {
        val status = stablePublishedTransactions.mapValues { it.value.get() }
        val message = SinkStatusMessage(
            latestStableTransactions = status
                .mapKeys { it.key.id }
                .mapValues { it.value }
        )
        val outMessage = OutMessage.Message(message)
        publisher.publish(outMessage)
    }

    fun shutdown() = lock.withLock {
        state = State.DESTROYED
        publishScheduler.shutdown()
        statusPublisher.destroy()
        try {
            if (!publishScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                publishScheduler.shutdownNow()
            }
        } catch (_: InterruptedException) {
            publishScheduler.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }

    private enum class State {
        INIT,
        STANDBY,
        ACTIVE,
        DESTROYED
    }
}
