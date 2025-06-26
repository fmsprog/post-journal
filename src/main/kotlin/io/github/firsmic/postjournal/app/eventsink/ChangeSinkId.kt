package io.github.firsmic.postjournal.app.eventsink

@JvmInline
value class ChangeSinkId(
    val id: String
) {
    override fun toString(): String {
        return id
    }
}