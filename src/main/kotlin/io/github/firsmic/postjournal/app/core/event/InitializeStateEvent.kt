package io.github.firsmic.postjournal.app.core.event

import io.github.firsmic.postjournal.app.state.ApplicationStateManager
import io.github.firsmic.postjournal.app.state.TxnChanges

class InitializeStateEvent<CHANGES : TxnChanges>(
    val state: ApplicationStateManager<CHANGES>
) : NonTransactionalEvent()
