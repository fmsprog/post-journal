package io.github.firsmic.postjournal.app.eventsink.publisher

import io.github.firsmic.postjournal.app.eventsink.TxnStabilityListener
import io.github.firsmic.postjournal.journal.api.TxnId
import mu.KLogging
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class PublisherAckHandler(
    private val name: String,
    private val stabilityListener: TxnStabilityListener,
) {
    companion object : KLogging()

    private val processingQueue = ArrayBlockingQueue<ProcessingEvent>(4096)
    private var processingThread: Thread? = null
    private val isRunning = AtomicBoolean(true)
    private val inFlightBuffer = TreeMap<TxnId, TxnAckState>()
    private var lastStableTxnId: TxnId? = null

    fun start() {
        processingThread = Thread(this::processEvents, "publisher-ack-processor [$name]").apply {
            isDaemon = true
            start()
        }
    }

    fun published(txn: TxnId, msgCount: Int, msgIndex: Int) {
        if (!processingQueue.offer(PublishedEvent(txn, msgCount, msgIndex))) {
            check(isRunning.get()) {
                "Is not started!"
            }
            throw IllegalStateException("In-flight size limit exceeded")
        }
    }

    fun acknowledged(txn: TxnId, msgIndex: Int) {
        processingQueue.put(AcknowledgedEvent(txn, msgIndex))
    }

    private fun processEvents() {
        while (isRunning.get()) {
            var event: ProcessingEvent? = null
            try {
                event = processingQueue.take()
                when (event) {
                    is PublishedEvent -> handlePublished(event)
                    is AcknowledgedEvent -> handleAcknowledged(event)
                }
            } catch (_: InterruptedException) {
                if (!isRunning.get()) break
            } catch (e: Exception) {
                logger.error(e) {"Error processing event: $event"}
            }
        }
    }

    private fun isAlreadyStable(txnId: TxnId): Boolean {
        val lastStable = lastStableTxnId
        return lastStable != null && txnId <= lastStable
    }

    private fun handlePublished(event: PublishedEvent) {
        val txnId = event.txn
        if (isAlreadyStable(txnId)) {
            return
        }
        val txnState = TxnAckState(event.msgCount)
        if (event.msgCount == 0) {
            inFlightBuffer[txnId] = txnState
            processStableTransactions()
        } else {
            val existing = inFlightBuffer[txnId]
            if (existing != null) {
                if (existing.totalMessages != txnState.totalMessages) {
                    throw IllegalStateException(
                        "Override message count for txn $txnId: " +
                                "${existing.totalMessages} != ${txnState.totalMessages}"
                    )
                }
            } else {
                inFlightBuffer[txnId] = txnState
            }
        }
    }

    private fun handleAcknowledged(event: AcknowledgedEvent) {
        val txnId = event.txn
        if (isAlreadyStable(txnId)) {
            return
        }
        val txnState = inFlightBuffer[event.txn]
        if (txnState == null) {
            logger.warn { "Acknowledgment for unknown transaction $txnId (msgIndex=${event.msgIndex})" }
        } else {
            val complete = txnState.acknowledgeMessage(event.msgIndex)
            if (complete) {
                processStableTransactions()
            }
        }
    }

    private fun processStableTransactions() {
        val iterator = inFlightBuffer.entries.iterator()
        while (iterator.hasNext()) {
            val (txnId, state) = iterator.next()
            if (state.complete) {
                lastStableTxnId = txnId
                try {
                    stabilityListener.txnStable(txnId)
                } catch (e: Exception) {
                    logger.error(e) {"Error notifying stability listener for txn $txnId: $stabilityListener" }
                }
                iterator.remove()
            } else {
                break
            }
        }
    }

    fun shutdown() {
        isRunning.set(false)
        processingThread?.interrupt()
    }

    fun shutdown(timeout: Long, unit: TimeUnit): Boolean {
        val startTime = System.nanoTime()
        val timeoutNanos = unit.toNanos(timeout)

        while (System.nanoTime() - startTime < timeoutNanos) {
            if (inFlightBuffer.isEmpty() && processingQueue.isEmpty()) {
                break
            }

            try {
                Thread.sleep(10) // Small pause between checks
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }

        val allProcessed = inFlightBuffer.isEmpty() && processingQueue.isEmpty()

        isRunning.set(false)
        processingThread?.interrupt()

        try {
            val remainingTime = timeoutNanos - (System.nanoTime() - startTime)
            if (remainingTime > 0) {
                processingThread?.join(TimeUnit.NANOSECONDS.toMillis(remainingTime))
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }

        return allProcessed
    }

    // Events for processing in a separate thread
    private sealed class ProcessingEvent

    private data class PublishedEvent(
        val txn: TxnId,
        val msgCount: Int,
        val msgIndex: Int
    ) : ProcessingEvent()

    private data class AcknowledgedEvent(
        val txn: TxnId,
        val msgIndex: Int
    ) : ProcessingEvent()
}

private class TxnAckState(
    val totalMessages: Int
) {
    private val acknowledgedMessages = BitSet()

    fun acknowledgeMessage(msgIndex: Int): Boolean {
        acknowledgedMessages.set(msgIndex)
        return complete
    }

    val complete: Boolean get() = acknowledgedMessages.cardinality() == totalMessages
}