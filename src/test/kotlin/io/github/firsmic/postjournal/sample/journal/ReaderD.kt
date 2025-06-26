package io.github.firsmic.postjournal.sample.journal

import io.github.firsmic.postjournal.journal.api.JournalMode
import io.github.firsmic.postjournal.journal.api.JournalTxn

fun main() {
//    val initialTxn = JournalTxn(
//        epoch = Epoch(
//            EpochNumber(3),
//            EpochOffset(4138)
//        ),
//        offset = Offset.of(29612),
//        txnId = TxnId(1345)
//    )
    PostJournalApp(
        replicaId = "D",
        mode = JournalMode.Read(),
        startReaderFrom = JournalTxn.NULL
    ).run()
}
