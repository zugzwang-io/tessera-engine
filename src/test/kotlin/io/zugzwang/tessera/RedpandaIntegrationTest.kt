package io.zugzwang.tessera

import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.redpanda.RedpandaContainer
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

@Testcontainers
class RedpandaIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val redpanda = RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v26.1.12")
    }

    private fun changeLog(topic: String) = KafkaChangeLog(KafkaChangeLog.producer(redpanda.bootstrapServers), topic)

    @Test
    fun `a change commits as exactly one record and round-trips`() = runBlocking {
        val log = changeLog("commit-test")
        val change = Change(
            "orders",
            listOf(Change.Entry("a", byteArrayOf(1, 2)), Change.Entry("b", ByteArray(0))),
        )

        assertEquals(0, log.append(change))
        assertEquals(1, log.append(Change("orders", listOf(Change.Entry("a", byteArrayOf(3))))))

        val records = consumeAll("commit-test")
        assertEquals(2, records.size)
        val (collection, entries) = records.first()
        assertEquals("orders", collection)
        assertEquals(listOf("a", "b"), entries.map { it.key })
        assertContentEquals(byteArrayOf(1, 2), entries[0].value)
        assertContentEquals(ByteArray(0), entries[1].value)
    }

    @Test
    fun `the endpoint commits to the log end to end`() = testApplication {
        application { module(changeLog("endpoint-test")) }

        val response = client.post("/v1/collections/orders/changes") {
            contentType(ContentType.Application.Json)
            setBody("""{"entries":[{"key":"a","value":"AQI="}]}""")
        }

        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("""{"sequence":0}""", response.bodyAsText())
        val (_, entries) = consumeAll("endpoint-test").single()
        assertContentEquals(byteArrayOf(1, 2), entries.single().value)
    }

    private fun consumeAll(topic: String): List<Pair<String, List<Change.Entry>>> {
        val config = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to redpanda.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        )
        KafkaConsumer(config, StringDeserializer(), ByteArrayDeserializer()).use { consumer ->
            consumer.assign(listOf(TopicPartition(topic, 0)))
            val records = consumer.poll(Duration.ofSeconds(10))
            return records.map { it.key() to EnvelopeV1.decode(it.value()) }
        }
    }
}
