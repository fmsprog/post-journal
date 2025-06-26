package io.github.firsmic.postjournal.app.core.event

open class EventContext<out E : Event>(
    val event: E
)