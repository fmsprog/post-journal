package io.github.firsmic.postjournal.app.core.topology

class CleanupHandler: BaseEventHandler<AppEventContext<*>> {
    override fun onEvent(
        context: AppEventContext<*>?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        context?.cleanup()
    }
}