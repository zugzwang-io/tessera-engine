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
import org.slf4j.LoggerFactory
import kotlin.concurrent.thread

fun main() {
    val config = Config.fromEnv()
    val projector = Projector(
        Postgres.dataSource(config),
        Projector.consumer(config.kafkaBootstrapServers),
        config.logTopic,
    )
    // Fail fast: the schema and resume checkpoint need Postgres at boot.
    // (Kafka has no equivalent check only because the producer connects
    // lazily — its failures surface per-request as 503s.)
    projector.start()
    // A dedicated platform thread, not a coroutine: KafkaConsumer.poll is a
    // blocking API and the consumer is single-threaded by contract, so a
    // coroutine would just park an IO thread with worse clarity. Coroutines
    // earn their keep in the mailbox/fan-out layer, not here.
    thread(name = "projector", isDaemon = true) {
        try {
            projector.run()
        } catch (cause: Exception) {
            // Projection stalls (view staleness grows); the write path keeps
            // serving. Poison records are never skipped — see Schema's kdoc.
            LoggerFactory.getLogger("io.zugzwang.tessera.Projector").error("projector stopped", cause)
        }
    }
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
