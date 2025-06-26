package io.github.firsmic.postjournal.journal.kafka

import io.github.firsmic.postjournal.journal.api.Epoch
import io.github.firsmic.postjournal.journal.api.EpochNumber
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.api.Offset
import io.github.firsmic.postjournal.journal.api.ReaderRecord
import io.github.firsmic.postjournal.journal.api.TxnId
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

sealed interface JournalInputMessage {
    val epoch: Epoch
    val offset: Offset
    val replicaId: String
    val timestamp: Long

    data class NewEpoch(
        override val epoch: Epoch,
        override val offset: Offset,
        override val replicaId: String,
        override val timestamp: Long,
        val messageId: String,
    ) : JournalInputMessage

    data class Heartbeat(
        override val epoch: Epoch,
        override val offset: Offset,
        override val replicaId: String,
        override val timestamp: Long,
        val isLeader: Boolean
    ) : JournalInputMessage

    class Data(
        override val epoch: Epoch,
        override val offset: Offset,
        override val replicaId: String,
        override val timestamp: Long,
        val txnId: TxnId,
        val payload: ByteArray,
    ) : JournalInputMessage {
        fun toSensitiveSafeString(): String {
            return "{epoch='$epoch', offset='$offset', txnId='$txnId', replicaId='$replicaId'}"
        }
    }
}

internal fun JournalInputMessage.Data.toReaderRecord() = ReaderRecord(
    txnInfo = JournalTxn(
        epoch = epoch,
        offset = offset,
        txnId = txnId
    ),
    payload = payload,
)

sealed class OutputJournalMessage {
    abstract val replicaId: String
    abstract val timestamp: Long

    class NewEpochProposal(
        val epochNumber: EpochNumber,
        val messageId: String,
        override val replicaId: String,
        override val timestamp: Long
    ) : OutputJournalMessage()

    class Heartbeat(
        val epoch: Epoch,
        override val replicaId: String,
        override val timestamp: Long,
        val isLeader: Boolean
    ) : OutputJournalMessage()

    class Data(
        val epoch: Epoch,
        val txnId: TxnId,
        val payload: ByteArray,
        override var replicaId: String,
        override val timestamp: Long
    ) : OutputJournalMessage()
}

object JournalDataKafkaProtocol :
    KafkaMarshaller<String, ByteArray, OutputJournalMessage>,
    KafkaUnmarshaller<String, ByteArray, JournalInputMessage> {
    private const val HEADER_MESSAGE_TYPE = "0"
    private const val HEADER_REPLICA_ID = "1"
    private const val HEADER_EPOCH_NUMBER = "2"
    private const val HEADER_EPOCH_OFFSET = "3"
    private const val HEADER_TXN_ID = "4"
    private const val HEADER_IS_LEADER = "5"
    private const val HEADER_TIMESTAMP = "6"

    private const val MESSAGE_TYPE_HEARTBEAT = 1
    private const val MESSAGE_TYPE_NEW_EPOCH = 2
    private const val MESSAGE_TYPE_DATA = 3

    private fun longToBytes(value: Long): ByteArray {
        return ByteArray(8) { i ->
            ((value shr (56 - i * 8)) and 0xFF).toByte()
        }
    }

    private fun bytesToLong(bytes: ByteArray): Long {
        require(bytes.size == 8)
        return bytes.foldIndexed(0L) { i, acc, byte ->
            acc or ((byte.toLong() and 0xFF) shl (56 - i * 8))
        }
    }

    private fun newProducerRecord(
        topic: String,
        partition: Int?,
        key: String,
        value: ByteArray,
        headers: List<Header>,
    ) = ProducerRecord(
        topic,
        partition,
        null, // timestamp
        key,
        value,
        headers
    )

    override fun marshall(topic: String, partition: Int?, message: OutputJournalMessage): ProducerRecord<String, ByteArray> {
        return when (message) {
            is OutputJournalMessage.Heartbeat -> newProducerRecord(
                topic = topic,
                partition = partition,
                key = "",
                value = byteArrayOf(),
                headers = listOf(
                    RecordHeader(HEADER_MESSAGE_TYPE, byteArrayOf(MESSAGE_TYPE_HEARTBEAT.toByte())),
                    RecordHeader(HEADER_REPLICA_ID, message.replicaId.toByteArray(Charsets.UTF_8)),
                    RecordHeader(HEADER_EPOCH_NUMBER, longToBytes(message.epoch.epochNumber.value)),
                    RecordHeader(HEADER_EPOCH_OFFSET, longToBytes(message.epoch.epochOffset.value)),
                    RecordHeader(HEADER_IS_LEADER, byteArrayOf(if (message.isLeader) 1 else 0)),
                    RecordHeader(HEADER_TIMESTAMP, longToBytes(message.timestamp)),
                )
            )
            is OutputJournalMessage.NewEpochProposal -> newProducerRecord(
                topic = topic,
                partition = partition,
                key = message.messageId,
                value = byteArrayOf(),
                headers = listOf(
                    RecordHeader(HEADER_MESSAGE_TYPE, byteArrayOf(MESSAGE_TYPE_NEW_EPOCH.toByte())),
                    RecordHeader(HEADER_REPLICA_ID, message.replicaId.toByteArray(Charsets.UTF_8)),
                    RecordHeader(HEADER_EPOCH_NUMBER, longToBytes(message.epochNumber.value)),
                    RecordHeader(HEADER_TIMESTAMP, longToBytes(message.timestamp)),
                )
            )
            is OutputJournalMessage.Data -> newProducerRecord(
                topic = topic,
                partition = partition,
                key = "",
                value = message.payload,
                headers = listOf(
                    RecordHeader(HEADER_MESSAGE_TYPE, byteArrayOf(MESSAGE_TYPE_DATA.toByte())),
                    RecordHeader(HEADER_REPLICA_ID, message.replicaId.toByteArray(Charsets.UTF_8)),
                    RecordHeader(HEADER_EPOCH_NUMBER, longToBytes(message.epoch.epochNumber.value)),
                    RecordHeader(HEADER_EPOCH_OFFSET, longToBytes(message.epoch.epochOffset.value)),
                    RecordHeader(HEADER_TXN_ID, longToBytes(message.txnId.value)),
                    RecordHeader(HEADER_TIMESTAMP, longToBytes(message.timestamp)),
                )
            )
        }
    }


    private fun Map<String?, Header?>.requiredHeader(key: String): Header {
        return this[key] ?: throw IllegalStateException(
            "Required header '$key' not found in $keys"
        )
    }

    private fun Map<String?, Header?>.getLongHeader(key: String): Long = bytesToLong(requiredHeader(key).value())

    private fun Map<String?, Header?>.getStringHeader(key: String): String = requiredHeader(key).value().toString(Charsets.UTF_8)

    override fun unmarshall(record: ConsumerRecord<String, ByteArray>): JournalInputMessage {
        val headers = record.headers().associateBy { it.key() }
        val msgType = headers.requiredHeader(HEADER_MESSAGE_TYPE).value()[0].toInt()
        return when (msgType) {
            MESSAGE_TYPE_HEARTBEAT -> JournalInputMessage.Heartbeat(
                epoch = Epoch.Companion.of(
                    number = headers.getLongHeader(HEADER_EPOCH_NUMBER),
                    offset = headers.getLongHeader(HEADER_EPOCH_OFFSET)
                ),
                offset = Offset.Companion.of(record.offset()),
                replicaId = headers.getStringHeader(HEADER_REPLICA_ID),
                timestamp = headers.getLongHeader(HEADER_TIMESTAMP),
                isLeader = headers.requiredHeader(HEADER_IS_LEADER).value()[0] == 1.toByte()
            )

            MESSAGE_TYPE_NEW_EPOCH -> JournalInputMessage.NewEpoch(
                epoch = Epoch.Companion.of(
                    number = headers.getLongHeader(HEADER_EPOCH_NUMBER),
                    offset = record.offset()
                ),
                offset = Offset.Companion.of(record.offset()),
                messageId = record.key(),
                replicaId = headers.getStringHeader(HEADER_REPLICA_ID),
                timestamp = headers.getLongHeader(HEADER_TIMESTAMP),
            )

            MESSAGE_TYPE_DATA -> JournalInputMessage.Data(
                epoch= Epoch.Companion.of(
                    number = headers.getLongHeader(HEADER_EPOCH_NUMBER),
                    offset = headers.getLongHeader(HEADER_EPOCH_OFFSET)
                ),
                offset = Offset.Companion.of(record.offset()),
                txnId = TxnId(headers.getLongHeader(HEADER_TXN_ID)),
                payload = record.value(),
                replicaId = headers.getStringHeader(HEADER_REPLICA_ID),
                timestamp = headers.getLongHeader(HEADER_TIMESTAMP),
            )

            else -> error(
                "Unknown message type '$msgType' received at " +
                        "topic='${record.topic()}', " +
                        "partition=${record.partition()}, " +
                        "offset='${record.offset()}'"
            )
        }
    }
}