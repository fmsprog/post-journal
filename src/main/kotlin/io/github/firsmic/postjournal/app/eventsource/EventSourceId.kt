package io.github.firsmic.postjournal.app.eventsource

import kotlinx.serialization.Serializable

@Serializable
@JvmInline
value class EventSourceId(
    val id: String
) {
    override fun toString(): String {
        return id
    }
}
