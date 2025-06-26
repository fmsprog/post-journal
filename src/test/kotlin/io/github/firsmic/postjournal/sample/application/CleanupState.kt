package io.github.firsmic.postjournal.sample.application

import io.github.firsmic.postjournal.sample.application.app.SNAPSHOTS_DIRECTORY
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private val KAFKA_DIRECTORY = Paths.get("docker/kafka/data")

fun main() {
    deleteDirectoryRecursively(SNAPSHOTS_DIRECTORY)
    deleteDirectoryRecursively(KAFKA_DIRECTORY)
}

private fun deleteDirectoryRecursively(directory: Path) {
    Files.walk(directory)
        .sorted(Comparator.reverseOrder())
        .forEach { path ->
            Files.delete(path)
            println("Deleted: $path")
        }
}
