package io.github.firsmic.postjournal.common.disruptor

import com.lmax.disruptor.EventHandler

interface NamedEventHandler<T> : EventHandler<T> {
    val name: String
}