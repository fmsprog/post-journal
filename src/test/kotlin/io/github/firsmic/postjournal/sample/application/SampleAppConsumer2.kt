package io.github.firsmic.postjournal.sample.application

import io.github.firsmic.postjournal.sample.application.app.SampleAppConsumer

fun main() {
    println("Started sample app consumer 2")
    SampleAppConsumer("sample-app-output-publisher-2").run()
}
