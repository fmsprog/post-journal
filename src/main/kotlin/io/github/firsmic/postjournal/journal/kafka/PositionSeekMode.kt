package io.github.firsmic.postjournal.journal.kafka

import io.github.firsmic.postjournal.journal.api.Offset

sealed interface PositionSeekMode {

    object Skip : PositionSeekMode

    @JvmInline
    value class AtOffset(val nextOffset: Offset) : PositionSeekMode {
        init {
            check(nextOffset.value >= 0)
        }
    }

    object Earliest : PositionSeekMode

    @JvmInline
    value class Latest(val backlogSize: ULong) : PositionSeekMode
}