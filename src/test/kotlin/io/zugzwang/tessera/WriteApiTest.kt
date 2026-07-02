package io.zugzwang.tessera

import io.ktor.client.HttpClient
import io.ktor.client.request.put
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals

class WriteApiTest {

    private fun writeApiTest(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) = testApplication {
        application { module() }
        block(client)
    }

    private fun changeJson(vararg entries: Pair<String, ByteArray>): String {
        val encoder = Base64.getEncoder()
        return entries.joinToString(",", prefix = """{"entries":[""", postfix = "]}") { (key, value) ->
            """{"key":"$key","value":"${encoder.encodeToString(value)}"}"""
        }
    }

    @Test
    fun `accepts a multi-entry change`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody(changeJson("a" to byteArrayOf(1, 2), "b" to byteArrayOf(3)))
        }
        assertEquals(HttpStatusCode.Accepted, response.status)
    }

    @Test
    fun `rejects a duplicate key within a change`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody(changeJson("a" to byteArrayOf(1), "a" to byteArrayOf(2)))
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
    }

    @Test
    fun `accepts a single-key put of raw bytes`() = writeApiTest { client ->
        val response = client.put("/v1/collections/orders/keys/a") {
            contentType(ContentType.Application.OctetStream)
            setBody(byteArrayOf(1, 2, 3))
        }
        assertEquals(HttpStatusCode.Accepted, response.status)
    }

    @Test
    fun `rejects an empty change`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"entries":[]}""")
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
    }

    @Test
    fun `rejects a blank key`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"entries":[{"key":" ","value":"AQ=="}]}""")
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
    }

    @Test
    fun `rejects a value that is not base64`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"entries":[{"key":"a","value":"not base64!"}]}""")
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
    }

    @Test
    fun `rejects a malformed body`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"nope":true}""")
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
    }

    @Test
    fun `rejects a change over the size limit`() = writeApiTest { client ->
        val response = client.put("/v1/collections/orders/keys/a") {
            contentType(ContentType.Application.OctetStream)
            setBody(ByteArray(MAX_CHANGE_BYTES + 1))
        }
        assertEquals(HttpStatusCode.PayloadTooLarge, response.status)
    }

    @Test
    fun `accepts an empty value (opaque bytes include zero bytes)`() = writeApiTest { client ->
        val response = client.put("/v1/collections/orders/keys/a") {
            contentType(ContentType.Application.OctetStream)
            setBody(ByteArray(0))
        }
        assertEquals(HttpStatusCode.Accepted, response.status)
    }
}
