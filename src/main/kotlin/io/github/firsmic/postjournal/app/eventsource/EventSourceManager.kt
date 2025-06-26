package io.github.firsmic.postjournal.app.eventsource

import io.github.firsmic.postjournal.app.core.EventDispatcher
import mu.KLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class EventSourceManager(
    private val eventDispatcher: EventDispatcher,
    private val eventSources: List<EventSource>,
) {
    companion object : KLogging()

    private val lock = ReentrantLock()
    private var startupFuture: CompletableFuture<*>? = null

    fun start() = lock.withLock {
        logger.info { "Start event sources" }
        startupFuture = CompletableFuture.runAsync(
            {
                eventSources.forEach { source ->
                    logger.info { "Start event source [${source.sourceId}]" }
                    source.registerEventDispatcher(eventDispatcher)
                    source.start()
                    logger.info { "Start event source [${source.sourceId}] DONE" }
                }
            },
            Executors.newVirtualThreadPerTaskExecutor()
        )
    }

    fun stop() = lock.withLock {
        logger.info { "Stop event sources" }
        startupFuture?.cancel(true)
        startupFuture = null

        CompletableFuture.runAsync(
            {
                eventSources.forEach { source ->
                    logger.info { "Stop event source [${source.sourceId}]" }
                    source.stop()
                    logger.info { "Stop event source [${source.sourceId}] DONE" }
                }
            },
            Executors.newVirtualThreadPerTaskExecutor()
        )
    }
}