package io.github.firsmic.postjournal.app.core.event

interface Acknowledgable {
    val acknowledgment: Acknowledgment?
}