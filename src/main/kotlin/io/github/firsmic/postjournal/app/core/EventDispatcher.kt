package io.github.firsmic.postjournal.app.core

import io.github.firsmic.postjournal.app.core.event.Event

interface EventDispatcher {

    fun processEvent(event: Event)

    // fun processEvents(events: List<Event>)
}