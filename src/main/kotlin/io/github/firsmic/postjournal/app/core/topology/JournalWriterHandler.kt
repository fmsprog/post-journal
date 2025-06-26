package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.Journal
import io.github.firsmic.postjournal.journal.api.JournalStabilityListener
import io.github.firsmic.postjournal.journal.api.JournalTxn
import mu.KLogging

class JournalWriterHandler<CHANGES : TxnChanges>(
    private val journal: Journal,
    private val journalStabilityBarrier: TxnSequenceBarrier
) : RetryableEventHandler<AppEventContext<CHANGES>>(), JournalStabilityListener {
    companion object : KLogging()

    init {
        journal.setStabilityListener(this)
    }

    override fun executeWithRetry(context: AppEventContext<CHANGES>) {
        if (context.isRecovery) {
            return
        }
        val writerRecord = context.marshalledChanges
        if (writerRecord != null) {
            journal.write(writerRecord, context)
        }
    }

    override fun txnStable(txn: JournalTxn, clientCallbackArg: Any?) {
        val context = clientCallbackArg as? AppEventContext<*>
        if (context != null) {
            context.stableJournalTxn = txn
            journalStabilityBarrier.advance(txn.txnId)
            // TO DO: move to the parallel event handler?
            context.acknowledgment?.acknowledge()
        }
    }
}