package io.github.firsmic.postjournal.app.core.event

import java.util.concurrent.CompletableFuture

class InitCompleteEvent() : Event() {
    val result = CompletableFuture<Unit>()
}
