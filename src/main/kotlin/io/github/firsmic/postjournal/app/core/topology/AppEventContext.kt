package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.app.core.event.Acknowledgable
import io.github.firsmic.postjournal.app.core.event.Acknowledgment
import io.github.firsmic.postjournal.app.core.event.Event
import io.github.firsmic.postjournal.app.core.event.TxnRecoveryEvent
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.api.WriterRecord

class AppEventContext<CHANGES : TxnChanges>: Acknowledgable {
    var event: Event? = null
    var changes: CHANGES? = null
    var marshalledChanges: WriterRecord? = null
    var stableJournalTxn: JournalTxn? = null

    override val acknowledgment: Acknowledgment? get() = (event as? Acknowledgable)?.acknowledgment

    val isRecovery: Boolean get() = event is TxnRecoveryEvent<*>

    fun init(event: Event) {
        this.event = event
    }

    fun cleanup() {
        event = null
        changes = null
        marshalledChanges = null
        stableJournalTxn = null
    }
}