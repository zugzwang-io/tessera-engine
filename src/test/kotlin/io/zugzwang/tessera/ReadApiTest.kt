package io.zugzwang.tessera

import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.readRawBytes
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class FakeStateView : StateView {
    var ready = true
    var sequence = 0L
    val collections = mutableMapOf<String, List<StateEntry>>()

    override fun collection(collection: String): CollectionState? =
        if (ready) CollectionState(sequence, collections[collection].orEmpty()) else null

    override fun key(collection: String, key: String): KeyState? =
        if (ready) KeyState(sequence, collections[collection].orEmpty().find { it.key == key }?.value) else null
}

class ReadApiTest {

    private val stateView = FakeStateView()

    private fun readApiTest(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) = testApplication {
        application { module(RecordingChangeLog(), stateView) }
        block(client)
    }

    @Test
    fun `serves a collection with its watermark`() = readApiTest { client ->
        stateView.sequence = 7
        stateView.collections["orders"] = listOf(StateEntry("a", byteArrayOf(1, 2)))

        val response = client.get("/v1/collections/orders")
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("""{"as_of_sequence":7,"entries":[{"key":"a","value":"AQI="}]}""", response.bodyAsText())
    }

    @Test
    fun `an unknown collection is an empty collection`() = readApiTest { client ->
        val response = client.get("/v1/collections/unknown")
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("""{"as_of_sequence":0,"entries":[]}""", response.bodyAsText())
    }

    @Test
    fun `serves a key as raw bytes with the watermark header`() = readApiTest { client ->
        stateView.sequence = 3
        stateView.collections["orders"] = listOf(StateEntry("a", byteArrayOf(1, 2, 3)))

        val response = client.get("/v1/collections/orders/keys/a")
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("3", response.headers[SEQUENCE_HEADER])
        assertContentEquals(byteArrayOf(1, 2, 3), response.readRawBytes())
    }

    @Test
    fun `an absent key is 404 with the watermark header`() = readApiTest { client ->
        stateView.sequence = 5
        val response = client.get("/v1/collections/orders/keys/missing")
        assertEquals(HttpStatusCode.NotFound, response.status)
        assertEquals("5", response.headers[SEQUENCE_HEADER])
    }

    @Test
    fun `responds 503 while the view has never checkpointed`() = readApiTest { client ->
        stateView.ready = false
        assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/v1/collections/orders").status)
        assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/v1/collections/orders/keys/a").status)
    }
}
