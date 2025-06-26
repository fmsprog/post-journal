package io.github.firsmic.postjournal.app.core.topology

import mu.KLogging
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.min
import kotlin.math.pow

abstract class RetryableEventHandler<T>(
    private val baseDelayMs: Long = 100,
    private val maxDelayMs: Long = 5000
) : BaseEventHandler<T> {
    companion object : KLogging()
    protected val destroyed = AtomicBoolean()

    override fun onEvent(
        context: T?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        if (context != null) {
            var attempt = 0
            while (!destroyed.get()) {
                try {
                    executeWithRetry(context)
                    if (attempt > 0) {
                        logger.info("Completed after $attempt retries")
                    }
                    return
                } catch (ex: Throwable) {
                    logger.error(ex) { "Unhandled exception: event=$context, seqNum=$sequence" }
                    attempt++
                    val delayMs = calculateBackoffDelay(attempt)
                    sleep(delayMs)
                }
            }
        }
    }

    protected abstract fun executeWithRetry(context: T)

    private fun sleep(delayMs: Long) {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMs)
        do {
            try {
                Thread.sleep(10)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
        } while (System.nanoTime() < deadline && !destroyed.get())
    }

    private fun calculateBackoffDelay(attempt: Int): Long {
        val exponentialDelay = baseDelayMs * 2.0.pow(attempt - 1).toLong()
        return min(exponentialDelay, maxDelayMs)
    }

    fun destroy() {
        destroyed.set(true)
    }
}