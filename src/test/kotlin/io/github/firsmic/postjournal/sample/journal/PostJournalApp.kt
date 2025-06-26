package io.github.firsmic.postjournal.sample.journal

import io.github.firsmic.postjournal.journal.api.JournalMode
import io.github.firsmic.postjournal.journal.api.JournalReaderCallback
import io.github.firsmic.postjournal.journal.api.JournalStateListener
import io.github.firsmic.postjournal.journal.api.JournalTxn
import io.github.firsmic.postjournal.journal.api.ReaderRecord
import io.github.firsmic.postjournal.journal.api.TxnId
import io.github.firsmic.postjournal.journal.api.WriterRecord
import io.github.firsmic.postjournal.journal.kafka.KafkaJournalConfig
import io.github.firsmic.postjournal.journal.kafka.KafkaJournalFactory
import mu.KLogging
import java.lang.Exception
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess


class PostJournalApp(
    val replicaId: String,
    val mode: JournalMode = JournalMode.ReadAndWrite,
    val startReaderFrom: JournalTxn = JournalTxn.NULL
) {
    companion object : KLogging()
    
    var latestTxn = TxnId.NULL

    fun run() {
        val journalFactory = KafkaJournalFactory(
            config = KafkaJournalConfig(
                replicaId = replicaId,
                topicPrefix = "journal",
                partition = 0
            )
        )
        val journal = journalFactory.createJournal()

        journal.setStabilityListener { txn, arg ->
            logger.info("Txn $txn was written by $arg")
        }

        journal.setStateListener(object : JournalStateListener {
            var periodicWriteHandle: ScheduledFuture<*>? = null
            val scheduledExecutor = Executors.newSingleThreadScheduledExecutor()

            private fun writeTxn() {
                val nextTxn = latestTxn + 1
                try {
                    journal.write(
                        record = WriterRecord(
                            payload = "Written by $replicaId at ${LocalDateTime.now()}".toByteArray(charset = Charsets.US_ASCII),
                            txnId = nextTxn
                        ),
                        clientCallbackArg = replicaId
                    )
                    latestTxn = nextTxn
                } catch (ex: Exception) {
                    logger.error(ex) { "Unable to write $nextTxn by $replicaId" }
                }
            }

            override fun onStandby() {
                logger.info("STANDBY $replicaId")
            }

            override fun onActive() {
                logger.info("ACTIVE $replicaId")
                val task = Runnable {
                    writeTxn()
                }
                periodicWriteHandle = scheduledExecutor.scheduleWithFixedDelay(
                    task,
                    1L,
                    5000L,
                    TimeUnit.MILLISECONDS
                )
            }

            override fun onShutdown() {
                logger.info("SHUTDOWN $replicaId...")
                // wait for session finalization
                Thread.sleep(5000)
                periodicWriteHandle?.cancel(false)
                scheduledExecutor.shutdownNow()
                logger.info("SHUTDOWN $replicaId DONE")
                exitProcess(1)
            }
        })

        journal.start(
            mode = mode,
            startReaderFrom = startReaderFrom,
            callback = object : JournalReaderCallback {

                override fun onDataRead(record: ReaderRecord) {
                    val txn = record.txnInfo
                    logger.info("Data read: txn=$txn: ${record.payload.toString(charset = Charsets.US_ASCII)}")
                    latestTxn = txn.txnId
                }

                override fun onEndOfRecovery(error: Throwable?) {
                    if (error != null) {
                        error.printStackTrace()
                    } else {
                        logger.info("Recovery finished")
                    }
                }
            }
        )

        logger.info("STARTED $replicaId")

        Thread.currentThread().join()
    }
}
