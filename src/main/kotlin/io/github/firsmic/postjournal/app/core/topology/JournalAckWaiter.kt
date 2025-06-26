package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.app.core.event.ApplicationEvent
import java.util.concurrent.atomic.AtomicBoolean

class JournalAckWaiter(
    journalStabilityBarrier: TxnSequenceBarrier
): BaseEventHandler<AppEventContext<*>> {
    private val destroyed = AtomicBoolean()
    private val txnBarrierWaiter = TxnBarrierWaiter(journalStabilityBarrier, destroyed)

    override fun onEvent(
        context: AppEventContext<*>?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        if (context == null) {
            return
        }
        val event = context.event
        if (event is ApplicationEvent) {
            val txnId = context.changes?.txnId ?: throw IllegalStateException(
                "Txn id is null"
            )
            txnBarrierWaiter.waitForTxn(txnId)
        }
    }
}