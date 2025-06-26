package io.github.firsmic.postjournal.app.core.event

import io.github.firsmic.postjournal.app.state.SnapshotInfo
import java.util.concurrent.CompletableFuture

class SaveSnapshotCommand : NonTransactionalEvent() {
    val result = CompletableFuture<SnapshotInfo?>()
}
