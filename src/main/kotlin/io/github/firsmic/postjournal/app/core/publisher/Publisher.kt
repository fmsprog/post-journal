package io.github.firsmic.postjournal.app.core.publisher

interface Publisher<T> {

    fun start()

    fun destroy()

    fun addStabilityListener(listener: PublisherStabilityListener)

    fun removeStabilityListener(listener: PublisherStabilityListener)

    fun publish(outMessage: OutMessage<T>, clientCallbackArg: Any? = null)

    fun publishBatch(batch: List<OutMessageWithCallbackArg<T>>)

    fun retrieveLastHeader(): BinaryHeader?
}

data class OutMessageWithCallbackArg<T>(
    val outMessage: OutMessage<T>,
    val clientCallbackArg: Any? = null
)

sealed interface OutMessage<T> {

    class Message<T>(
        val message: T,
    ) : OutMessage<T>

    class MessageAndHeader<T>(
        val message: T,
        val header: BinaryHeader,
    ) : OutMessage<T>

    class Header<T>(
        val header: BinaryHeader
    ) : OutMessage<T>
}

fun interface PublisherStabilityListener {

    fun stable(clientCallbackArg: Any)
}

@JvmInline
value class BinaryHeader(
    val data: ByteArray
)