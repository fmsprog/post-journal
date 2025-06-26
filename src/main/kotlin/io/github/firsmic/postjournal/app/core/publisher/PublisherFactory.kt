package io.github.firsmic.postjournal.app.core.publisher

interface PublisherFactory<T> {
    fun createPublisher(): Publisher<T>
}
