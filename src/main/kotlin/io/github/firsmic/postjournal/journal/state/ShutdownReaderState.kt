package io.github.firsmic.postjournal.journal.state

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.TxnId

internal class ShutdownReaderState(
    sharedState: SharedState,
    currentEpoch: Epoch,
    nextTxnId: TxnId,
) : TerminalState(
    sharedState,
    currentEpoch,
    nextTxnId
)