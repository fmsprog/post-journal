package io.github.firsmic.postjournal.app.eventsink

import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.api.TxnId

interface ChangeSink<CHANGES : TxnChanges> {

    val id: ChangeSinkId

    fun start()

    fun destroy()

    fun addStabilityListener(listener: TxnStabilityListener)

    fun removeStabilityListener(listener: TxnStabilityListener)

    fun publish(changeHolder: TxnChangeHolder<CHANGES>)

    fun retrieveLastPendingTxn(): JournalTxn?
}

class TxnChangeHolder<CHANGES : TxnChanges>(
    val changes: CHANGES,
    val txn: JournalTxn
)

fun interface TxnStabilityListener {
    fun txnStable(txn: TxnId)
}
