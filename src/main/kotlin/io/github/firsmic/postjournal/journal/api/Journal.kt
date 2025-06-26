package io.github.firsmic.postjournal.journal.api

interface Journal {

    val replicaId: String

    fun setStateListener(stateListener: JournalStateListener)

    fun setStabilityListener(stabilityListener: JournalStabilityListener)

    val isWriter: Boolean

    val isReader: Boolean

    fun write(record: WriterRecord, clientCallbackArg: Any?)

    fun start(
        mode: JournalMode,
        startReaderFrom: JournalTxn,
        callback: JournalReaderCallback
    )

    /**
     * Block transition to active state (if possible)
     */
    fun acquireReaderLock(): Boolean

    fun releaseReaderLock()

    fun destroy()
}

interface JournalReaderCallback {

    fun onDataRead(record: ReaderRecord)

    fun onEndOfRecovery(error: Throwable?)
}

interface JournalStateListener {

    fun onStandby()

    fun onActive()

    fun onShutdown()
}

fun interface JournalStabilityListener {
    fun txnStable(txn: JournalTxn, clientCallbackArg: Any?)
}
