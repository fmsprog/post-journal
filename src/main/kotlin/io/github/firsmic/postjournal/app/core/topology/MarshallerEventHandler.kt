package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.app.state.Marshaller
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.WriterRecord

class MarshallerEventHandler<CHANGES : TxnChanges>(
    val marshaller: Marshaller<CHANGES>
) : BaseEventHandler<AppEventContext<CHANGES>> {
    override fun onEvent(
        context: AppEventContext<CHANGES>?,
        sequence: Long,
        endOfBatch: Boolean
    ) {
        if (context == null || context.isRecovery) {
            return
        }
        val changes = context.changes
        if (changes != null) {
            context.marshalledChanges = WriterRecord(
                payload = marshaller.marshal(changes),
                txnId = changes.txnId,
            )
        }
    }
}