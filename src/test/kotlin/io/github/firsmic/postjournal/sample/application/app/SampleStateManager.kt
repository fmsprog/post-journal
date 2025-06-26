package io.github.firsmic.postjournal.sample.application.app

import io.github.firsmic.postjournal.app.core.event.ApplicationEvent
import io.github.firsmic.postjournal.app.core.event.ApplicationQuery
import io.github.firsmic.postjournal.app.eventsource.EventSourceId
import io.github.firsmic.postjournal.app.eventsource.kafka.KafkaMessageEvent
import io.github.firsmic.postjournal.app.eventsource.kafka.KafkaMessageInfo
import io.github.firsmic.postjournal.app.eventsource.kafka.KafkaSourceState
import io.github.firsmic.postjournal.app.eventsource.kafka.KafkaSourceStateQuery
import io.github.firsmic.postjournal.app.eventsource.kafka.KafkaTopicPartition
import io.github.firsmic.postjournal.app.state.ApplicationStateManager
import io.github.firsmic.postjournal.app.state.RecoveryContext
import io.github.firsmic.postjournal.app.state.SnapshotInfo
import io.github.firsmic.postjournal.app.state.TxnChanges
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.api.Offset
import io.github.firsmic.postjournal.journal.api.TxnId
import kotlinx.serialization.Polymorphic
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText
import kotlin.io.path.writeText

val SNAPSHOTS_DIRECTORY = Paths.get("snapshots")

class SampleStateManager(
    private val snapshotDirectory: Path = SNAPSHOTS_DIRECTORY
) : ApplicationStateManager<SampleChanges> {
    companion object : KLogging() {
        private const val SNAPSHOT_EXTENSION = ".snapshot"
        private const val TEMP_EXTENSION = ".tmp"
    }

    private val state = SampleState()
    private val json = Json {
        prettyPrint = true
        ignoreUnknownKeys = true
        encodeDefaults = true
        allowStructuredMapKeys = true
    }
    override val txnId: TxnId get() = state.txnId

    init {
        if (!snapshotDirectory.exists()) {
            Files.createDirectories(snapshotDirectory)
        }
    }

    override fun loadFromSnapshotBefore(latestTxn: TxnId): SnapshotInfo? {
        val snapshotFile = findLatestSnapshotBefore(latestTxn) ?: return null
        val snapshotContent = snapshotFile.readText()
        val loadedState = json.decodeFromString<SampleState>(snapshotContent)
        state.restoreFrom(loadedState)
        return SnapshotInfo(
            txn = loadedState.recoveredTxn ?: throw IllegalStateException(),
            timestamp = Files.getLastModifiedTime(snapshotFile).toMillis(),
            path = snapshotFile.toString()
        )
    }

    override fun saveSnapshot(): SnapshotInfo? {
        val journalTxn = state.recoveredTxn ?: throw IllegalStateException(
            "State is not recovered"
        )
        if (journalTxn.txnId != state.txnId) {
            throw IllegalStateException(
                "Can't take snapshot in active state! (recoveredTxn=$journalTxn, latestTxn=${state.txnId})"
            )
        }
        val currentTxnId = state.txnId
        val snapshotFile = getSnapshotFilePath(currentTxnId)
        val tempFile = getSnapshotFilePath(currentTxnId, isTemp = true)
        val snapshotContent = json.encodeToString(state)
        tempFile.writeText(snapshotContent)
        Files.move(tempFile, snapshotFile, StandardCopyOption.REPLACE_EXISTING)
        cleanupOldSnapshots(keepLast = 10)
        logger.info { "Snapshot saved: txn='$journalTxn', path='${snapshotFile.toAbsolutePath()}'" }
        return SnapshotInfo(
            txn = journalTxn,
            timestamp = Files.getLastModifiedTime(snapshotFile).toMillis(),
            path = snapshotFile.toString()
        )
    }

    private fun findLatestSnapshotBefore(latestTxn: TxnId): Path? {
        return snapshotDirectory.listDirectoryEntries("*$SNAPSHOT_EXTENSION")
            .mapNotNull { file ->
                extractTxnIdFromFileName(file.fileName.toString())?.let { txnId ->
                    txnId to file
                }
            }
            .filter { (txnId, _) -> txnId < latestTxn }
            .maxByOrNull { (txnId, _) -> txnId }
            ?.second
    }

    private fun getSnapshotFilePath(txnId: TxnId, isTemp: Boolean = false): Path {
        val extension = if (isTemp) TEMP_EXTENSION else SNAPSHOT_EXTENSION
        val fileName = "${txnId.value}$extension"
        return snapshotDirectory.resolve(fileName)
    }

    private fun extractTxnIdFromFileName(fileName: String): TxnId? {
        return try {
            if (fileName.endsWith(SNAPSHOT_EXTENSION)) {
                val txnValue = fileName.removeSuffix(SNAPSHOT_EXTENSION).toLong()
                TxnId(txnValue)
            } else null
        } catch (e: NumberFormatException) {
            logger.error(e) { "Unable to parse $fileName" }
            null
        }
    }

    private fun cleanupOldSnapshots(keepLast: Int) {
        try {
            val snapshotFiles = snapshotDirectory.listDirectoryEntries("*$SNAPSHOT_EXTENSION")
                .mapNotNull { file ->
                    extractTxnIdFromFileName(file.fileName.toString())?.let { txnId ->
                        txnId to file
                    }
                }
                .sortedByDescending { (txnId, _) -> txnId }

            snapshotFiles.drop(keepLast).forEach { (_, file) ->
                try {
                    Files.deleteIfExists(file)
                } catch (e: Exception) {
                    logger.error(e) { "Failed to delete old snapshot ${file.fileName}: ${e.message}" }
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to cleanup old snapshots: ${e.message}" }
        }
    }

    override fun isInitComplete(): Boolean = state.initComplete

    override fun recovery(context: RecoveryContext<SampleChanges>) {
        state.recovery(context.journalTxn,context.changes)
    }

    override fun setInitComplete() = state.initComplete()

    override fun processQuery(query: ApplicationQuery<*>) {
        when (query) {
            is KafkaSourceStateQuery -> {
                val sourceState =  state.latestKafkaOffsets[query.sourceId]
                val result = if (sourceState == null) {
                    null
                } else {
                    KafkaSourceState(
                        sourceId = query.sourceId,
                        latestOffsets = sourceState.toMap()
                    )
                }
                query.result.complete(result)
            }
        }
    }

    override fun processEvent(event: ApplicationEvent): SampleChanges {
        return when (event) {
            is KafkaMessageEvent<*, *, *> -> {
                logger.info("Process kafka message: ${event.record.value()}, offset=${event.record.offset()}")
                val message = event.unmarshalledPayload as SampleAppMessage
                state.addKey(event.messageInfo, message.key, message.value)
            }
            else -> throw IllegalStateException(
                "Unknown event type $event"
            )
        }
    }
}

@Serializable
@Polymorphic
sealed interface SampleChanges : TxnChanges {

    @Serializable
    class InitComplete(override val txnId: TxnId) : SampleChanges

    @Serializable
    class AddKey(
        override val txnId: TxnId,
        val messageInfo: KafkaMessageInfo,
        val key: String,
        val value: String
    ) : SampleChanges
}

@Serializable
class SampleState {
    var initComplete = false
        private set

    val data = mutableMapOf<String, String>()
    val latestKafkaOffsets = mutableMapOf<EventSourceId, MutableMap<KafkaTopicPartition, Offset>>()
    var txnId: TxnId = TxnId.NULL
    var recoveredTxn: JournalTxn? = null

    fun recovery(journalTxn: JournalTxn, changes: SampleChanges) {
        check(txnId + 1 == changes.txnId) {
            "Invalid transaction sequence: expected ${txnId + 1}, got ${changes.txnId}"
        }
        txnId = changes.txnId
        recoveredTxn = journalTxn
        when (changes) {
            is SampleChanges.InitComplete -> {
                check(!initComplete) { "Init already completed" }
                initComplete = true
            }
            is SampleChanges.AddKey -> {
                check(initComplete) { "Init is not completed" }
                data[changes.key] = changes.value
                addKafkaMessageInfo(changes.messageInfo)
            }
        }
    }

    private fun addKafkaMessageInfo(kafkaMessageInfo: KafkaMessageInfo) {
        val sourceState = latestKafkaOffsets.computeIfAbsent(kafkaMessageInfo.sourceId) {
            mutableMapOf()
        }
        sourceState[kafkaMessageInfo.topicPartition] = kafkaMessageInfo.offset
    }

    fun initComplete(): SampleChanges {
        txnId++
        return SampleChanges.InitComplete(txnId)
    }

    fun addKey(
        messageInfo: KafkaMessageInfo,
        key: String,
        value: String
    ): SampleChanges {
        txnId++
        data[key] = value
        addKafkaMessageInfo(messageInfo)
        return SampleChanges.AddKey(txnId, messageInfo, key, value)
    }

    fun restoreFrom(snapshot: SampleState) {
        this.initComplete = snapshot.initComplete
        this.data.clear()
        this.data.putAll(snapshot.data)
        this.txnId = snapshot.txnId
    }
}
