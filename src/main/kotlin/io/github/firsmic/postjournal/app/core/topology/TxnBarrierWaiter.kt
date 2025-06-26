package io.github.firsmic.postjournal.app.core.topology

import com.lmax.disruptor.AlertException
import io.github.firsmic.postjournal.journal.api.TxnId
import java.util.concurrent.atomic.AtomicBoolean

class TxnBarrierWaiter(
    private val txnStabilityBarrier: TxnSequenceBarrier,
    private val destroyed: AtomicBoolean
) {
    fun waitForTxn(txnId: TxnId): Boolean {
        if (destroyed.get()) {
            return false
        }
        try {
            txnStabilityBarrier.waitFor(txnId)
            return true
        } catch (ex: Exception) {
            return when (ex) {
                is AlertException, is InterruptedException -> {
                    destroyed.set(true)
                    false
                }
                else -> throw ex
            }
        }
    }
}