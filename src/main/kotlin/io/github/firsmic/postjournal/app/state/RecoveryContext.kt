package io.github.firsmic.postjournal.app.state

import io.github.firsmic.postjournal.journal.api.JournalTxn

class RecoveryContext<out CHANGES : TxnChanges>(
    val changes: CHANGES,
    val journalTxn: JournalTxn,
)