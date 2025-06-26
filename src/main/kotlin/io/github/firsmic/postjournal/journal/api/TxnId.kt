package io.github.firsmic.postjournal.journal.api

import kotlinx.serialization.Serializable

@Serializable
@JvmInline
value class TxnId(
    val value: Long
) : Comparable<TxnId> {
    companion object {
        val NULL = TxnId(-1)
        val MAX = TxnId(Long.MAX_VALUE)
    }

    override fun compareTo(other: TxnId) = value.compareTo(other.value)

    operator fun compareTo(other: Int): Int = value.compareTo(other.toLong())

    operator fun compareTo(other: Long): Int = value.compareTo(other)

    operator fun plus(add: Long) = TxnId(value + add)

    operator fun minus(v: Long) = TxnId(value - v)

    operator fun inc() = TxnId(value + 1)

    override fun toString() = value.toString()
}

val TxnId.isNull get() = this.value < 0
val TxnId.isNotNull get() = !isNull