package io.github.firsmic.postjournal.app.core.event

import java.util.concurrent.CompletableFuture

sealed class Query<T> : NonTransactionalEvent() {
    val result = CompletableFuture<T>()
}