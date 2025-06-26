package io.github.firsmic.postjournal.sample.application.app

import kotlinx.serialization.Serializable

@Serializable
data class SampleAppMessage(
    val key: String,
    val value: String,
)