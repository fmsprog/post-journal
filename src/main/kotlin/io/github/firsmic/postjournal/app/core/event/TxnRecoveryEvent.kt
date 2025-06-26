package io.github.firsmic.postjournal.app.core.event

import io.github.firsmic.postjournal.app.state.Marshaller
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.ReaderRecord

class TxnRecoveryEvent<CHANGES : TxnChanges>(
    val journalEntry: ReaderRecord,
    private val unmarshaller: Marshaller<CHANGES>,
) : Event(), UnmarshallableEvent {
    private var _unmarshalledChanges: CHANGES? = null

    val unmarshalledChanges: CHANGES get() = _unmarshalledChanges ?: throw IllegalStateException(
        "Payload is not unmarshalled"
    )

    override fun unmarshall() {
        if (_unmarshalledChanges == null) {
            _unmarshalledChanges = unmarshaller.unmarshall(journalEntry.payload)
        }
    }
}
