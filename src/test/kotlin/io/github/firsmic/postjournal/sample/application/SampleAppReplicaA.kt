package io.github.firsmic.postjournal.sample.application

import io.github.firsmic.postjournal.sample.application.app.SampleApp
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.bind.annotation.*

@SpringBootApplication
class SampleAppApplicationA

fun main(args: Array<String>) {
    runApplication<SampleAppApplicationA>(*args)
}

@Configuration
class SampleAppConfig {
    @Bean
    fun sampleApp() = SampleApp("A").apply { start() }
}

@RestController
@RequestMapping("/api/v1")
class SampleAppController(
    private val sampleApp: SampleApp
) {
    @GetMapping("/snapshot")
    fun takeSnapshot(): String {
        return sampleApp.application.takeSnapshot()?.toString() ?: "No snapshot created"
    }
}
