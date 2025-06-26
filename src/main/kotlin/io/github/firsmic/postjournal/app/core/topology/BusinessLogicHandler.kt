package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.app.core.event.*
import io.github.firsmic.postjournal.app.state.ApplicationStateManager
import io.github.firsmic.postjournal.app.state.RecoveryContext
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.isNotNull
import mu.KLogging

class BusinessLogicHandler<CHANGES : TxnChanges> : BaseEventHandler<AppEventContext<CHANGES>> {
    companion object : KLogging()

    private var recoveryStarted = false
    private var stateManager: ApplicationStateManager<CHANGES>? = null

    override fun onEvent(
        context: AppEventContext<CHANGES>?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        try {
            val event = context?.event ?: throw IllegalStateException("Null event at seqNum=$sequence")
            context.changes = processEvent(event)
        } catch (ex: Exception) {
            logger.error(ex) { "Unhandled exception: event=${context?.event}, seqNum=$sequence" }
        }
    }

    private fun getStateManagerOrFail(): ApplicationStateManager<CHANGES> {
        return stateManager ?: throw ApplicationStateNotInitException()
    }

    private fun processNonTransactionalEvent(event: NonTransactionalEvent) {
        when (event) {
            is InitializeStateEvent<*> -> {
                if (stateManager != null) {
                    throw IllegalStateException("State already init!")
                }
                stateManager = event.state as ApplicationStateManager<CHANGES>
                null
            }
            is IsInitCompleteQuery -> processIsInitCompleteQuery(event)
            is ApplicationQuery<*> -> processApplicationQuery(event)
            is SaveSnapshotCommand -> processSaveSnapshot(event)
        }
    }

    private fun processEvent(event: Event): CHANGES? {
        return when (event) {
            is NonTransactionalEvent -> {
                processNonTransactionalEvent(event)
                null
            }
            is TxnRecoveryEvent<*> -> {
                val changes = event.unmarshalledChanges as CHANGES
                val context = RecoveryContext(changes, event.journalEntry.txnInfo)
                val state = getStateManagerOrFail()
                if (!recoveryStarted && state.txnId.isNotNull && state.txnId == context.changes.txnId) {
                    // 1st transaction after start of recovery from snapshot is duplicate by design
                    // ignore
                } else {
                    state.recovery(context)
                }
                recoveryStarted = true
                null
            }
            is InitCompleteEvent -> {
                getStateManagerOrFail().setInitComplete().also {
                    event.result.complete(Unit)
                }
            }
            is ApplicationEvent -> {
                getStateManagerOrFail().processEvent(event)
            }
        }
    }

    private fun processSaveSnapshot(event: SaveSnapshotCommand) {
        runCatching {
            getStateManagerOrFail().saveSnapshot()
        }.onFailure {
            event.result.completeExceptionally(it)
        }.onSuccess {
            event.result.complete(it)
        }
    }

    private fun processIsInitCompleteQuery(query: IsInitCompleteQuery) {
        val initComplete = getStateManagerOrFail().isInitComplete()
        query.result.complete(initComplete)
    }

    private fun processApplicationQuery(query: ApplicationQuery<*>) {
        runCatching {
            getStateManagerOrFail().processQuery(query)
        }.onFailure {
            query.result.completeExceptionally(it)
        }
    }
}

class ApplicationStateNotInitException : IllegalStateException("Application state is not initialized yet")