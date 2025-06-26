package io.github.firsmic.postjournal.app.eventsink.publisher

import io.github.firsmic.postjournal.app.state.TxnChanges

fun interface PublishPayloadCreator<CHANGES : TxnChanges, T> {
    fun createPayload(changes: CHANGES): List<T>
}