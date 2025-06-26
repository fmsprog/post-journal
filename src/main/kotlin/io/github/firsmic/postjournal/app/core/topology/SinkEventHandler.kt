package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.app.eventsink.ChangeSink
import io.github.firsmic.postjournal.app.eventsink.TxnChangeHolder
import io.github.firsmic.postjournal.app.state.TxnChanges
import mu.KLogging

class SinkEventHandler<CHANGES : TxnChanges>(
    val sink: ChangeSink<CHANGES>,
    val standbyPacingBarrier: TxnSequenceBarrier
) : RetryableEventHandler<AppEventContext<CHANGES>>() {
    companion object : KLogging()

    private val txnBarrierWaiter = TxnBarrierWaiter(standbyPacingBarrier, destroyed)
    override val name: String get() = "Publisher (${sink.id})"

    override fun executeWithRetry(context: AppEventContext<CHANGES>) {
        val changes = context.changes
        if (changes != null) {
            txnBarrierWaiter.waitForTxn(changes.txnId)
            val changeHolder = TxnChangeHolder(
                changes = changes,
                txn = context.stableJournalTxn ?: throw IllegalStateException(
                    "Can't publish not stable txn ${changes.txnId}"
                )
            )
            sink.publish(changeHolder)
        }
    }
}