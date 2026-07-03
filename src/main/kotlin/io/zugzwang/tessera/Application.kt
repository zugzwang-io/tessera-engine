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
    val dataSource = lazy { Postgres.dataSourceFromEnv() }
    if (System.getenv("POSTGRES_URL") != null) {
        val projector = Projector.fromEnv(dataSource.value)
        thread(name = "projector", isDaemon = true) {
            try {
                projector.run()
            } catch (cause: Exception) {
                // Projection stops (view staleness grows); the write path keeps serving.
                LoggerFactory.getLogger("io.zugzwang.tessera.Projector").error("projector stopped", cause)
            }
        }
    }
    val port = System.getenv("PORT")?.toInt() ?: 8080
    embeddedServer(Netty, port = port) {
        module(KafkaChangeLog.fromEnv(), PgStateView { dataSource.value })
    }.start(wait = true)
}

fun Application.module(
    changeLog: ChangeLog = KafkaChangeLog.fromEnv(),
    stateView: StateView = PgStateView { Postgres.dataSourceFromEnv() },
) {
    install(ContentNegotiation) {
        json()
    }
    routing {
        get("/") {
            call.respondText("Hello, world!")
        }
        writeApi(changeLog)
        readApi(stateView)
    }
}
