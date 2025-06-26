package io.github.firsmic.postjournal.app.core

import io.github.firsmic.postjournal.app.core.event.InitCompleteEvent
import io.github.firsmic.postjournal.app.core.event.InitializeStateEvent
import io.github.firsmic.postjournal.app.core.event.IsInitCompleteQuery
import io.github.firsmic.postjournal.app.core.event.TxnRecoveryEvent
import io.github.firsmic.postjournal.app.core.publisher.PublisherFactory
import io.github.firsmic.postjournal.app.core.topology.SinkEventHandler
import io.github.firsmic.postjournal.app.eventsink.ChangeSink
import io.github.firsmic.postjournal.app.eventsink.ChangeSinkId
import io.github.firsmic.postjournal.app.eventsink.status.ChangeSinkStatusSynchronizer
import io.github.firsmic.postjournal.app.eventsink.status.ChangeStatusReaderFactory
import io.github.firsmic.postjournal.app.eventsink.status.SinkStatusMessage
import io.github.firsmic.postjournal.app.eventsource.EventSource
import io.github.firsmic.postjournal.app.eventsource.EventSourceManager
import io.github.firsmic.postjournal.app.state.ApplicationStateManagerFactory
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.*
import mu.KLogging
import kotlin.system.exitProcess

class LifecycleManager<CHANGES : TxnChanges>(
    val eventDispatcher: EventDispatcher,
    val journal: Journal,
    val sinkEventHandlers: Collection<SinkEventHandler<*>>,
    eventSources: List<EventSource>,
    val stateManagerFactory: ApplicationStateManagerFactory<CHANGES>,
    val sinkStatusReaderFactory: ChangeStatusReaderFactory,
    val sinkStatusPublisherFactory: PublisherFactory<SinkStatusMessage>
) : JournalReaderCallback,
    JournalStateListener {
    companion object : KLogging()

    private val changeSinks: List<ChangeSink<*>> get() = sinkEventHandlers.map { it.sink }
    private val stateUpdateUnmarshaller = stateManagerFactory.stateUpdateMarshallerFactory.createMarshaller()
    private lateinit var changeSinkStatusSynchronizer: ChangeSinkStatusSynchronizer
    private val eventSourceManager = EventSourceManager(
        eventDispatcher,
        eventSources
    )

    fun start() {
        val pendingTransactions = retrievePendingSinkTransactions()
        changeSinkStatusSynchronizer = ChangeSinkStatusSynchronizer(
            sinkEventHandlers = sinkEventHandlers,
            readerFactory = sinkStatusReaderFactory,
            publisherFactory = sinkStatusPublisherFactory,
            initialStableTransactions = pendingTransactions.mapValues { it.value.txnId },
            // 20 rps
            sinkStatusPublishIntervalMs = 50L,
        )

        val minPendingTxn = if (pendingTransactions.isEmpty()) {
            TxnId.MAX
        } else {
            pendingTransactions.values.minBy { it.txnId }.txnId
        }
        logger.info { "Pending sink transactions: $pendingTransactions" }
        logger.info { "minPendingTxn = $minPendingTxn" }

        val stateManager = stateManagerFactory.createStateManager()

        val startRecoveryTxn = if (minPendingTxn.isNull) {
            // start from scratch
            logger.info("Start recovery from scratch")
            JournalTxn.NULL
        } else {
            val snapshotInfo = stateManager.loadFromSnapshotBefore(minPendingTxn)
            logger.info("Start recovery from snapshot: $snapshotInfo")
            snapshotInfo?.txn ?: JournalTxn.NULL
        }
        eventDispatcher.processEvent(InitializeStateEvent(stateManager))
        journal.setStateListener(this)
        logger.info("Read journal from: $startRecoveryTxn")
        journal.start(JournalMode.ReadAndWrite, startRecoveryTxn, this)
    }

    fun shutdown() {
        // TODO - implement correct shutdown
        journal.destroy()
    }

    private fun retrievePendingSinkTransactions(): Map<ChangeSinkId, JournalTxn> {
        return changeSinks
            .associateBy { it.id }
            .mapValues {
                it.value.retrieveLastPendingTxn() ?: JournalTxn.NULL
            }
    }

    override fun onDataRead(record: ReaderRecord) {
        val event = TxnRecoveryEvent(
            journalEntry = record,
            unmarshaller = stateUpdateUnmarshaller
        )
        eventDispatcher.processEvent(event)
    }

    override fun onEndOfRecovery(error: Throwable?) {
        if (error != null) {
            logger.error(error) { "Recovery error"}
        } else {
            logger.info { "Recovery finished" }
        }
    }

    override fun onStandby() {
        logger.info("[${journal.replicaId}] is STANDBY")
        changeSinkStatusSynchronizer.toStandby()
    }

    override fun onActive() {
        logger.info("[${journal.replicaId}] ACTIVATING...")
        changeSinkStatusSynchronizer.toActive()
        startPublishers()
        if (!isInitComplete()) {
            setInitComplete()
        }
        startEventSources()
        logger.info("[${journal.replicaId}] is ACTIVE")
    }

    override fun onShutdown() {
        logger.info("[${journal.replicaId}] is DOWN")
        eventSourceManager.stop()
        changeSinkStatusSynchronizer.shutdown()
        exitProcess(1)
    }

    private fun startPublishers() {
        logger.info { "Start publishers" }
        changeSinks.forEach { sink ->
            logger.info { "Start publisher [${sink.id}]..." }
            sink.start()
            logger.info { "Start publisher [${sink.id}]. DONE" }
        }
    }

    private fun startEventSources() {
        eventSourceManager.start()
    }

    private fun isInitComplete(): Boolean {
        val query = IsInitCompleteQuery()
        eventDispatcher.processEvent(query)
        val initComplete = query.result.join()
        logger.info("Init complete: $initComplete")
        return initComplete
    }

    private fun setInitComplete() {
        val event = InitCompleteEvent()
        eventDispatcher.processEvent(event)
        event.result.join()
        logger.info("Init complete.")
    }
}
