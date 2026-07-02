package io.zugzwang.tessera

import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** Boots the real Netty engine (port 0) and hits it over HTTP. */
class ServerBootTest {

    @Test
    fun `server boots and serves requests`() = runBlocking {
        val server = embeddedServer(Netty, port = 0, module = Application::module).start(wait = false)
        try {
            val port = server.engine.resolvedConnectors().first().port
            HttpClient().use { client ->
                val response = client.get("http://localhost:$port/")
                assertEquals(HttpStatusCode.OK, response.status)
                assertEquals("Hello, world!", response.bodyAsText())
            }
        } finally {
            server.stop(gracePeriodMillis = 0, timeoutMillis = 1000)
        }
    }
}
