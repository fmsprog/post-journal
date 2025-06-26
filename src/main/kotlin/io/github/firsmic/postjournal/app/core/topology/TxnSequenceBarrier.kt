package io.github.firsmic.postjournal.app.core.topology

import com.lmax.disruptor.AlertException
import com.lmax.disruptor.Sequence
import com.lmax.disruptor.SequenceBarrier
import com.lmax.disruptor.WaitStrategy
import io.github.firsmic.postjournal.journal.api.TxnId
import kotlin.concurrent.Volatile

interface TxnStabilityBarrier {
    fun waitFor(txnId: TxnId)
}

class TxnSequenceBarrier(
    private val waitStrategy: WaitStrategy
) : TxnStabilityBarrier {
    private val sequence = Sequence(-1)
    private val barrier = SequenceBarrierImpl(
        waitStrategy = waitStrategy,
        cursorSequence = sequence,
    )

    fun advance(txnId: TxnId) {
        val next = txnId.value
        while (true) {
            val current = sequence.get()
            if (current >= next) {
                break
            }
            if (sequence.compareAndSet(current, next)) {
                break
            }
        }
    }

    fun set(txnId: TxnId) {
        sequence.set(txnId.value)
    }

    fun get(): TxnId {
        return TxnId(sequence.get())
    }

    override fun waitFor(txnId: TxnId) {
        barrier.waitFor(txnId.value)
    }

    fun alert() {
        barrier.alert()
    }
}

private class SequenceBarrierImpl(
    private val waitStrategy: WaitStrategy,
    private val cursorSequence: Sequence,
    private val dependentSequence: Sequence = cursorSequence
) : SequenceBarrier {
    @Volatile
    private var alerted = false

    override fun waitFor(sequence: Long): Long {
        checkAlert()
        return waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this)
    }

    override fun getCursor(): Long {
        return cursorSequence.get()
    }

    override fun isAlerted(): Boolean = alerted

    override fun alert() {
        alerted = true
        waitStrategy.signalAllWhenBlocking()
    }

    override fun clearAlert() {
        alerted = false
    }

    override fun checkAlert() {
        if (alerted) throw AlertException.INSTANCE
    }
}