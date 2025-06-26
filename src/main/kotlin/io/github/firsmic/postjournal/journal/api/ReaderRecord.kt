package io.github.firsmic.postjournal.journal.api

class ReaderRecord(
    val txnInfo: JournalTxn,
    val payload: ByteArray,
)