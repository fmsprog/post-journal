package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.app.core.event.UnmarshallableEvent
import mu.KLogging

class UnmarshallerEventHandler : BaseEventHandler<AppEventContext<*>> {
    companion object : KLogging()

    override fun onEvent(
        context: AppEventContext<*>?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        try {
            val event = context?.event
            if (event is UnmarshallableEvent) {
                event.unmarshall()
            }
        } catch (ex: Exception) {
            logger.error(ex) { "Unhandled exception: event=${context?.event}, seqNum=$sequence" }
        }
    }
}