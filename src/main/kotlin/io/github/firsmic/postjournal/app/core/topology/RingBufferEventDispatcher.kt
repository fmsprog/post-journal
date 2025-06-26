package io.github.firsmic.postjournal.app.core.topology

import com.lmax.disruptor.EventTranslatorOneArg
import com.lmax.disruptor.dsl.Disruptor
import io.github.firsmic.postjournal.app.core.EventDispatcher
import io.github.firsmic.postjournal.app.core.event.Event
import io.github.firsmic.postjournal.app.state.TxnChanges

class RingBufferEventDispatcher<CHANGES : TxnChanges>(
    private val disruptor: Disruptor<AppEventContext<CHANGES>>
) : EventDispatcher {
    private val translator = Translator<CHANGES>()

    override fun processEvent(event: Event) {
        disruptor.ringBuffer.publishEvent(translator, event)
    }

    private class Translator<CHANGES : TxnChanges> : EventTranslatorOneArg<AppEventContext<CHANGES>, Event> {
        override fun translateTo(
            context: AppEventContext<CHANGES>?,
            sequence: Long,
            event: Event?
        ) {
            context!!.init(event!!)
        }
    }
}