package io.github.firsmic.postjournal.app.eventsink.publisher

import io.github.firsmic.postjournal.app.core.publisher.BinaryHeader
import io.github.firsmic.postjournal.app.core.publisher.OutMessage
import io.github.firsmic.postjournal.app.core.publisher.Publisher
import io.github.firsmic.postjournal.app.core.publisher.PublisherFactory
import io.github.firsmic.postjournal.app.eventsink.ChangeSink
import io.github.firsmic.postjournal.app.eventsink.ChangeSinkId
import io.github.firsmic.postjournal.app.eventsink.TxnChangeHolder
import io.github.firsmic.postjournal.app.eventsink.TxnStabilityListener
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.api.TxnId
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KLogging
import java.lang.Exception
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class PublisherChangeSink<CHANGES : TxnChanges, T>(
    override val id: ChangeSinkId,
    val payloadCreator: PublishPayloadCreator<CHANGES, T>,
    val publisherFactory: PublisherFactory<T>,
) : ChangeSink<CHANGES> {
    companion object : KLogging()

    private val lock = ReentrantReadWriteLock()
    private val stabilityListeners = CopyOnWriteArrayList<TxnStabilityListener>()
    private var publisher: Publisher<T>? = null
    private val ackHandler = PublisherAckHandler(id.id) { txnId ->
        fireTxnStable(txnId)
    }
    private var lastSentMessageInfo: MessageInfo? = null

    @Volatile
    private var destroyed = false

    override fun start() {
        lock.writeLock().withLock {
            check(publisher == null)
            val publisher = publisherFactory.createPublisher()
            publisher.addStabilityListener(::onMessageStable)
            lastSentMessageInfo = retrieveLastPublishedHeader(publisher)?.toMessageInfo()
            publisher.start()
            this.publisher = publisher
            ackHandler.start()
            logger.info { "Started publisher '$id': latestSentMessage=$lastSentMessageInfo" }
        }
    }

    override fun destroy() {
        lock.writeLock().withLock {
            ackHandler.shutdown(1, TimeUnit.SECONDS)
            publisher?.destroy()
            destroyed = true
        }
    }

    private fun fireTxnStable(txnId: TxnId) {
        for (listener in stabilityListeners) {
            try {
                listener.txnStable(txnId)
            } catch (ex: Exception) {
                logger.error(ex) { "Exception in stability listener $listener" }
            }
        }
    }

    override fun addStabilityListener(listener: TxnStabilityListener) {
        stabilityListeners.add(listener)
    }

    override fun removeStabilityListener(listener: TxnStabilityListener) {
        stabilityListeners.remove(listener)
    }

    private fun createPayloadWithRetry(changeHolder: TxnChangeHolder<CHANGES>): List<OutMessage<T>> {
        var attempt = 0
        while (true) {
            if (destroyed) {
                throw IllegalStateException("Destroyed")
            }
            try {
                val messages = payloadCreator.createPayload(changeHolder.changes)
                return if (messages.isEmpty()) {
                    val header = MessageHeader(
                        messagesCount = 0,
                        messageIndex = 0,
                        journalTxn = changeHolder.txn
                    )
                    if (isMessageAlreadyDelivered(header)) {
                        emptyList()
                    } else {
                        val outMessage = OutMessage.Header<T>(
                            header = serializeMessageHeader(header)
                        )
                        listOf(outMessage)
                    }
                } else {
                    messages.mapIndexedNotNull { index, message ->
                        val header = MessageHeader(
                            messagesCount = messages.size,
                            messageIndex = index,
                            journalTxn = changeHolder.txn
                        )
                        if (isMessageAlreadyDelivered(header)) {
                            null
                        } else {
                            OutMessage.MessageAndHeader(
                                message = message,
                                header = serializeMessageHeader(header)
                            )
                        }
                    }
                }
            } catch (ex: Throwable) {
                logger.error(ex) { "Failed to create payload for txn changes ${changeHolder.txn} attempt=$attempt" }
                // TODO - backoff
                Thread.sleep(1000)
                attempt++
            }
        }
    }

    private fun isTxnAlreadyDelivered(txnId: TxnId): Boolean {
        val lastMsg = lastSentMessageInfo
        return lastMsg != null && lastMsg.txnId > txnId
    }

    private fun isMessageAlreadyDelivered(header: MessageHeader): Boolean {
        val lastMsg = lastSentMessageInfo
        if (lastMsg == null) {
            return false
        }
        val msgInfo = header.toMessageInfo()
        return lastMsg >= msgInfo
    }

    override fun publish(changeHolder: TxnChangeHolder<CHANGES>) {
        lock.readLock().withLock {
            val txnId = changeHolder.txn.txnId
            if (isTxnAlreadyDelivered(txnId)) {
                return
            }
            val messages = createPayloadWithRetry(changeHolder)
            var attempt = 0
            while (!destroyed) {
                try {
                    publishMessages(txnId, messages)
                    break
                } catch (ex: Throwable) {
                    logger.error(ex) { "Failed to publish txn changes ${changeHolder.txn} attempt=$attempt" }
                    // TODO - backoff
                    Thread.sleep(1000)
                    attempt++
                }
            }
        }
    }

    private fun publishMessages(txnId: TxnId, messages: List<OutMessage<T>>) {
        if (messages.isEmpty()) {
            ackHandler.published(txn = txnId, msgCount = 0, msgIndex = 0)
        } else {
            for (index in messages.indices) {
                val message = messages[index]
                ackHandler.published(txn = txnId, msgCount = messages.size, msgIndex = index)
                publisher!!.publish(message, AckInfo(txnId, index))
            }
        }
    }

    private fun onMessageStable(clientCallbackArg: Any?) {
        val ackInfo = clientCallbackArg as AckInfo
        ackHandler.acknowledged(ackInfo.txnId, ackInfo.msgIndex)
    }

    override fun retrieveLastPendingTxn(): JournalTxn? {
        val pub = publisherFactory.createPublisher()
        return try {
            val header = retrieveLastPublishedHeader(pub)
            header?.journalTxn
        } finally {
            pub.destroy()
        }
    }

    private fun retrieveLastPublishedHeader(publisher: Publisher<T>): MessageHeader? {
        val header = publisher.retrieveLastHeader()
        return if (header != null) {
            deserializeMessageHeader(header)
        } else {
            null
        }
    }
}

private class AckInfo(
    val txnId: TxnId,
    val msgIndex: Int
)

private val JSON = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}

@Serializable
private data class MessageHeader(
    val messagesCount: Int,
    val messageIndex: Int,
    val journalTxn: JournalTxn
) {
    fun toMessageInfo() = MessageInfo(
        txnId = journalTxn.txnId,
        messageIndex = messageIndex,
        isLast = messagesCount == 0 || messageIndex + 1 >= messagesCount
    )
}

private data class MessageInfo(
    val txnId: TxnId,
    val messageIndex: Int,
    val isLast: Boolean
) : Comparable<MessageInfo> {

    override fun compareTo(other: MessageInfo): Int {
        val txnComparison = txnId.compareTo(other.txnId)
        if (txnComparison != 0) {
            return txnComparison
        }
        val messageIndexComparison = messageIndex.compareTo(other.messageIndex)
        if (messageIndexComparison != 0) {
            return messageIndexComparison
        }
        return isLast.compareTo(other.isLast)
    }
}

private fun serializeMessageHeader(header: MessageHeader): BinaryHeader {
    val jsonString = JSON.encodeToString(header)
    return BinaryHeader(jsonString.toByteArray(Charsets.US_ASCII))
}

private fun deserializeMessageHeader(header: BinaryHeader): MessageHeader {
    val jsonString = header.data.toString(Charsets.US_ASCII)
    return JSON.decodeFromString<MessageHeader>(jsonString)
}
