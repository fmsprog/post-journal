package io.github.firsmic.postjournal.journal.api

import kotlinx.serialization.Serializable

@Serializable
@JvmInline
value class Offset(
    val value: Long
) : Comparable<Offset> {
    companion object {
        fun of(v: Long) = Offset(v)
    }

    override fun compareTo(other: Offset) = value.compareTo(other.value)

    operator fun compareTo(other: Int): Int = value.compareTo(other.toLong())

    operator fun compareTo(other: Long): Int = value.compareTo(other)

    operator fun plus(add: Long) = Offset(value + add)

    override fun toString() = value.toString()
}
