package io.github.firsmic.postjournal.common.disruptor

import com.lmax.disruptor.BatchEventProcessor
import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.WaitStrategy
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.dsl.ProducerType
import mu.KLogging
import java.util.concurrent.ThreadFactory

class DisruptorFactory<T>(
    private val name: String,
    private val ringBufferSize: Int,
    private val waitStrategy: WaitStrategy,
    private val eventFactory: EventFactory<T>,
    private val topologyFactory: TopologyFactory<T>,
) {
    companion object : KLogging()

    fun createDisruptor(): Disruptor<T> {
        val disruptor = Disruptor(
            eventFactory,
            ringBufferSize,
            DisruptorThreadFactory(name),
            ProducerType.MULTI,
            waitStrategy
        )
        initTopology(disruptor)
        return disruptor
    }

    private fun initTopology(disruptor: Disruptor<T>) {
        val handlers: List<List<EventHandler<T>>> = topologyFactory.createTopology()
        val topology = handlers.filter { it.isNotEmpty() }

        for (i in topology.indices) {
            val currentLevelHandlers = topology[i]
            check(currentLevelHandlers.isNotEmpty()) {
                "Empty handlers in topology $topology"
            }

            if (i == 0) {
                disruptor.handleEventsWith(*currentLevelHandlers.toTypedArray())
            } else {
                val prevHandlers = topology[i-1].toTypedArray()
                disruptor.after(*prevHandlers).handleEventsWith(*currentLevelHandlers.toTypedArray())
            }
        }

        val str = renderTopologyAsciiTable(topology)
        logger.info("$name topology:${System.lineSeparator()}$str")
    }

    private class DisruptorThreadFactory(private val name: String) : ThreadFactory {
        override fun newThread(r: Runnable): Thread {
            val t = Thread(r)
            t.name = getThreadName(r)
            t.isDaemon = true
            return t
        }
        private fun getThreadName(r: Runnable): String {
            if (r is BatchEventProcessor<*>) {
                val batchProcessor = r
                val eventHandlerField = batchProcessor::class.java.getDeclaredField("eventHandler")
                eventHandlerField.isAccessible = true
                val eventHandler = eventHandlerField.get(batchProcessor) as EventHandler<*>
                val handlerName = getEventHandlerName(eventHandler)
                return "[$name] EventHandler.$handlerName"
            } else {
                return "[$name] $r"
            }
        }
    }
}

private fun getEventHandlerName(handler: EventHandler<*>): String {
    return if (handler is NamedEventHandler<*>) {
        handler.name
    } else {
        handler.toString()
    }
}

private fun <T> renderTopologyAsciiTable(topology: List<List<EventHandler<T>>>): String {
    if (topology.isEmpty()) return "<empty>"

    val numStages = topology.size
    val maxHandlersPerStage = topology.maxOf { it.size }

    val handlerNames: List<List<String>> = topology.map { stage ->
        stage.map { handler -> getEventHandlerName(handler) }
    }

    val columnWidths = mutableListOf<Int>()
    for (stageIdx in 0 until numStages) {
        val maxWidth = handlerNames[stageIdx].maxOfOrNull { it.length } ?: 0
        columnWidths.add(maxWidth + 2) // +2 padding
    }

    val separator = buildString {
        append("+")
        for (w in columnWidths) {
            append("-".repeat(w)).append("+")
        }
    }

    val rows = mutableListOf<String>()
    for (rowIdx in 0 until maxHandlersPerStage) {
        val row = buildString {
            append("|")
            for (stageIdx in 0 until numStages) {
                val cell = handlerNames.getOrNull(stageIdx)?.getOrNull(rowIdx) ?: ""
                append(" ${cell.padEnd(columnWidths[stageIdx] - 1)}|")
            }
        }
        rows.add(row)
    }

    return buildString {
        appendLine(separator)
        rows.forEach { appendLine(it) }
        appendLine(separator)
    }
}
