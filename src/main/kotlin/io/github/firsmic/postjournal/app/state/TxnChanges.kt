package io.github.firsmic.postjournal.app.state

import io.github.firsmic.postjournal.journal.api.TxnId

interface TxnChanges {
    val txnId: TxnId
}
