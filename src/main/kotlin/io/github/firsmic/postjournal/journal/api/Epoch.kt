package io.github.firsmic.postjournal.journal.api

import kotlinx.serialization.Serializable

@Serializable
data class Epoch(
    val epochNumber: EpochNumber,
    val epochOffset: EpochOffset,
) : Comparable<Epoch> {
    companion object {
        fun of(number: Long, offset: Long): Epoch {
            return Epoch(EpochNumber(number), EpochOffset(offset))
        }
    }

    override fun compareTo(other: Epoch): Int {
        val epochCompare = epochNumber.compareTo(other.epochNumber)
        return if (epochCompare != 0) {
            epochCompare
        } else {
            // reverse offset comparison.
            // of two epochs with equal epochNumber
            // the epoch with the smaller offset "wins"
            // (the one that got into the journal earlier)
            - epochOffset.compareTo(other.epochOffset)
        }
    }

    override fun toString(): String {
        return if (isNullEpoch) {
            "Epoch.NULL"
        } else {
            "Epoch($epochNumber:$epochOffset)"
        }
    }
}

@Serializable
@JvmInline
value class EpochNumber(
    val value: Long
) : Comparable<EpochNumber> {

    override fun compareTo(other: EpochNumber) = value.compareTo(other.value)

    operator fun plus(add: Long) = EpochNumber(value + add)

    override fun toString() = value.toString()
}

@Serializable
@JvmInline
value class EpochOffset(
    val value: Long
) : Comparable<EpochOffset> {

    override fun compareTo(other: EpochOffset) = value.compareTo(other.value)

    operator fun compareTo(other: Int): Int = value.compareTo(other.toLong())

    operator fun compareTo(other: Long): Int = value.compareTo(other)

    operator fun plus(add: Long) = EpochOffset(value + add)

    override fun toString() = value.toString()
}

val NullEpoch = Epoch(EpochNumber(-1), EpochOffset(-1))

val Epoch.isNullEpoch: Boolean get() = this == NullEpoch
val Epoch.isNotNullEpoch: Boolean get() = this != NullEpoch
