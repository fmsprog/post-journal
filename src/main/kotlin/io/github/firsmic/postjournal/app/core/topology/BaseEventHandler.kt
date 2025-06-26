package io.github.firsmic.postjournal.app.core.topology

import io.github.firsmic.postjournal.common.disruptor.NamedEventHandler

interface BaseEventHandler<T> : NamedEventHandler<T> {

    override val name: String get() = javaClass.simpleName
}