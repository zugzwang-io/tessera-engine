package io.zugzwang.tessera

import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing

fun main() {
    val config = Config.fromEnv()
    embeddedServer(Netty, port = config.port) {
        module(KafkaChangeLog.fromConfig(config))
    }.start(wait = true)
}

fun Application.module(changeLog: ChangeLog = KafkaChangeLog.fromConfig(Config.fromEnv())) {
    install(ContentNegotiation) {
        json()
    }
    routing {
        get("/") {
            call.respondText("Hello, world!")
        }
        writeApi(changeLog)
    }
}
