package io.github.firsmic.postjournal.app.eventsource

import io.github.firsmic.postjournal.app.core.EventDispatcher

interface EventSource {

    val sourceId: EventSourceId

    val isStarted: Boolean

    fun registerEventDispatcher(eventDispatcher: EventDispatcher)

    fun start()

    fun stop()
}
