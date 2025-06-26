package io.github.firsmic.postjournal.app.eventsource.kafka

import io.github.firsmic.postjournal.app.core.event.ApplicationQuery
import io.github.firsmic.postjournal.app.eventsource.EventSourceId

class KafkaSourceStateQuery(
    val sourceId: EventSourceId,
) : ApplicationQuery<KafkaSourceState?>()