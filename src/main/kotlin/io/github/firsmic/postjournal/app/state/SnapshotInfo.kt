package io.github.firsmic.postjournal.app.state

import io.github.firsmic.postjournal.journal.api.JournalTxn
import kotlinx.serialization.Serializable

@Serializable
data class SnapshotInfo(
    val txn: JournalTxn,
    val timestamp: Long,
    val path: String,
)