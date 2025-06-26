package io.github.firsmic.postjournal.common.disruptor

import com.lmax.disruptor.EventHandler

fun interface TopologyFactory<T> {

    fun createTopology(): List<List<EventHandler<T>>>
}