package io.github.firsmic.postjournal.sample.application

import io.github.firsmic.postjournal.sample.application.app.SampleApp

fun main() {
    SampleApp(replicaId = "C").start()
    Thread.currentThread().join()
}
