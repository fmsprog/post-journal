package io.github.firsmic.postjournal.journal.api

import kotlinx.serialization.Serializable

@Serializable
data class JournalTxn(
    val epoch: Epoch,
    val offset: Offset,
    val txnId: TxnId
) {
    companion object {
        val NULL = JournalTxn(
            epoch = NullEpoch,
            offset = Offset(-1),
            txnId = TxnId.NULL
        )
    }
}
