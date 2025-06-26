package io.github.firsmic.postjournal.app.state

import io.github.firsmic.postjournal.app.core.event.ApplicationEvent
import io.github.firsmic.postjournal.app.core.event.ApplicationQuery
import io.github.firsmic.postjournal.journal.api.TxnId

interface ApplicationStateManager<CHANGES : TxnChanges> {

    val txnId: TxnId

    fun loadFromSnapshotBefore(latestTxn: TxnId): SnapshotInfo?

    /**
     * A snapshot can only be saved in recovery mode.
     * This allows the snapshot to contain information about journal transactions.
     */
    fun saveSnapshot(): SnapshotInfo?

    fun isInitComplete(): Boolean

    fun setInitComplete(): CHANGES

    fun processQuery(query: ApplicationQuery<*>)

    fun processEvent(event: ApplicationEvent): CHANGES

    fun recovery(context: RecoveryContext<CHANGES>)
}
