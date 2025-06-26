package io.github.firsmic.postjournal.journal.api

class WriterRecord(
    val payload: ByteArray,
    val txnId: TxnId
)
