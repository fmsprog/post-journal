package io.github.firsmic.postjournal.app.state

interface ApplicationStateManagerFactory<CHANGES : TxnChanges> {

    val stateUpdateMarshallerFactory: MarshallerFactory<CHANGES>

    fun createStateManager(): ApplicationStateManager<CHANGES>
}