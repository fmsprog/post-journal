package io.github.firsmic.postjournal.sample.application.app

import io.github.firsmic.postjournal.app.state.ApplicationStateManagerFactory
import io.github.firsmic.postjournal.app.state.Marshaller
import io.github.firsmic.postjournal.app.state.MarshallerFactory
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

class SampleStateManagerFactory(
    override val stateUpdateMarshallerFactory: MarshallerFactory<SampleChanges>
) : ApplicationStateManagerFactory<SampleChanges> {

    override fun createStateManager(): SampleStateManager {
        return SampleStateManager()
    }
}

object SampleMarshaller : Marshaller<SampleChanges> {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    override fun marshal(changes: SampleChanges): ByteArray {
        return json.encodeToString(changes).toByteArray()
    }

    override fun unmarshall(payload: ByteArray): SampleChanges {
        val string = String(payload)
        return json.decodeFromString(string)
    }
}