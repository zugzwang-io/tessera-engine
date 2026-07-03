package io.zugzwang.tessera

import io.ktor.client.HttpClient
import io.ktor.client.request.delete
import io.ktor.client.request.put
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RecordingChangeLog : ChangeLog {
    val changes = mutableListOf<Change>()
    var failure: Exception? = null

    override suspend fun append(change: Change): Long {
        failure?.let { throw it }
        changes += change
        return (changes.size - 1).toLong()
    }
}

class WriteApiTest {

    private val changeLog = RecordingChangeLog()

    private fun writeApiTest(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) = testApplication {
        application { module(changeLog) }
        block(client)
    }

    private fun changeJson(vararg entries: Pair<String, ByteArray>): String {
        val encoder = Base64.getEncoder()
        return entries.joinToString(",", prefix = """{"entries":[""", postfix = "]}") { (key, value) ->
            """{"key":"$key","value":"${encoder.encodeToString(value)}"}"""
        }
    }

    @Test
    fun `commits a multi-entry change and returns its sequence`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody(changeJson("a" to byteArrayOf(1, 2), "b" to byteArrayOf(3)))
        }
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("""{"sequence":0}""", response.bodyAsText())

        val change = changeLog.changes.single()
        assertEquals("orders", change.collection)
        assertEquals(listOf("a", "b"), change.entries.map { it.key })
        assertContentEquals(byteArrayOf(1, 2), assertIs<Change.Put>(change.entries[0]).value)
    }

    @Test
    fun `sequences increase per committed change`() = writeApiTest { client ->
        client.put("/v1/collections/orders/keys/a") { setBody(byteArrayOf(1)) }
        val response = client.put("/v1/collections/orders/keys/b") { setBody(byteArrayOf(2)) }
        assertEquals("""{"sequence":1}""", response.bodyAsText())
    }

    @Test
    fun `responds 503 when the log is unavailable`() = writeApiTest { client ->
        changeLog.failure = RuntimeException("broker down")
        val response = client.put("/v1/collections/orders/keys/a") { setBody(byteArrayOf(1)) }
        assertEquals(HttpStatusCode.ServiceUnavailable, response.status)
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
    fun `commits a single-key put of raw bytes`() = writeApiTest { client ->
        val response = client.put("/v1/collections/orders/keys/a") {
            contentType(ContentType.Application.OctetStream)
            setBody(byteArrayOf(1, 2, 3))
        }
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("""{"sequence":0}""", response.bodyAsText())
    }

    @Test
    fun `delete commits a tombstone change`() = writeApiTest { client ->
        val response = client.delete("/v1/collections/orders/keys/a")
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("""{"sequence":0}""", response.bodyAsText())

        val entry = changeLog.changes.single().entries.single()
        assertIs<Change.Tombstone>(entry)
        assertEquals("a", entry.key)
    }

    @Test
    fun `a change can mix puts and tombstones atomically`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"entries":[{"key":"a","value":"AQ=="},{"key":"b","tombstone":true}]}""")
        }
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals(listOf(false, true), changeLog.changes.single().entries.map { it is Change.Tombstone })
    }

    @Test
    fun `rejects a tombstone entry carrying a value`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"entries":[{"key":"a","value":"AQ==","tombstone":true}]}""")
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
    }

    @Test
    fun `rejects a non-tombstone entry without a value`() = writeApiTest { client ->
        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"entries":[{"key":"a"}]}""")
        }
        assertEquals(HttpStatusCode.BadRequest, response.status)
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
    fun `commits an empty value (opaque bytes include zero bytes)`() = writeApiTest { client ->
        val response = client.put("/v1/collections/orders/keys/a") {
            contentType(ContentType.Application.OctetStream)
            setBody(ByteArray(0))
        }
        assertEquals(HttpStatusCode.OK, response.status)
    }
}
