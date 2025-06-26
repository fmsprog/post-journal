package io.github.firsmic.postjournal.app.state

fun interface MarshallerFactory<T> {
    fun createMarshaller(): Marshaller<T>
}

interface Marshaller<T> {

    fun marshal(obj: T): ByteArray

    fun unmarshall(payload: ByteArray): T
}