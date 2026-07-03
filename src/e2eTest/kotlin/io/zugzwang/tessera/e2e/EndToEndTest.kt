package io.zugzwang.tessera.e2e

import io.zugzwang.tessera.Change
import io.zugzwang.tessera.EnvelopeV1
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.redpanda.RedpandaContainer
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.fail

/** The app as it ships: built from the Dockerfile, wired to Redpanda over the container network. */
@Testcontainers
class EndToEndTest {

    companion object {
        private val network: Network = Network.newNetwork()

        @Container
        @JvmStatic
        val redpanda: RedpandaContainer = RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v26.1.12")
            .withNetwork(network)
            .withNetworkAliases("redpanda")
            .withListener { "redpanda:19093" }

        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:18.4")
            .withNetwork(network)
            .withNetworkAliases("postgres")

        @Container
        @JvmStatic
        val app: GenericContainer<*> = GenericContainer(
            // The context is the Dockerfile's parent directory (honors .dockerignore);
            // the path must be absolute or that parent resolves to null.
            ImageFromDockerfile().withDockerfile(Path.of("Dockerfile").toAbsolutePath()),
        )
            .withNetwork(network)
            .withEnv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:19093")
            .withEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/test")
            .withEnv("POSTGRES_USER", "test")
            .withEnv("POSTGRES_PASSWORD", "test")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/").forStatusCode(200))
            .dependsOn(redpanda, postgres)
    }

    private val http = HttpClient.newHttpClient()

    private fun url(path: String) = URI("http://${app.host}:${app.getMappedPort(8080)}$path")

    private fun postChange(body: String, collection: String = "orders"): HttpResponse<String> {
        val request = HttpRequest.newBuilder()
            .uri(url("/v1/collections/$collection/changes"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        return http.send(request, HttpResponse.BodyHandlers.ofString())
    }

    /** Reads are lagging by the projector batch window; poll until the view catches up. */
    private fun awaitKey(collection: String, key: String, expectedStatus: Int): HttpResponse<ByteArray> {
        val request = HttpRequest.newBuilder().uri(url("/v1/collections/$collection/keys/$key")).GET().build()
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (true) {
            val response = http.send(request, HttpResponse.BodyHandlers.ofByteArray())
            if (response.statusCode() == expectedStatus) return response
            if (System.nanoTime() > deadline) {
                fail("key $key never reached status $expectedStatus (last: ${response.statusCode()})")
            }
            Thread.sleep(250)
        }
    }

    private fun sequenceIn(body: String): Long =
        Regex("\"sequence\":(\\d+)").find(body)?.groupValues?.get(1)?.toLong()
            ?: fail("no sequence in $body")

    @Test
    fun `changes commit through the shipped image and land durably in the log`() {
        // The shipped image serves one shared log, so sequences are relative
        // to whatever other tests have written, never absolute.
        val first = postChange("""{"entries":[{"key":"a","value":"AQI="},{"key":"b","value":""}]}""")
        assertEquals(200, first.statusCode())
        val sequence = sequenceIn(first.body())

        val second = postChange("""{"entries":[{"key":"a","value":"Aw=="}]}""")
        assertEquals(sequence + 1, sequenceIn(second.body()))

        val (collection, entries) = consumeAll("tessera-log").single { it.first == sequence }.second
        assertEquals("orders", collection)
        assertEquals(listOf("a", "b"), entries.map { it.key })
        assertContentEquals(byteArrayOf(1, 2), assertIs<Change.Put>(entries[0]).value)
        assertContentEquals(ByteArray(0), assertIs<Change.Put>(entries[1]).value)
    }

    @Test
    fun `validation runs in the shipped image`() {
        assertEquals(400, postChange("""{"entries":[]}""").statusCode())
    }

    @Test
    fun `written state becomes readable and deletable end to end`() {
        val write = postChange("""{"entries":[{"key":"k","value":"AQID"}]}""", collection = "e2e-read")
        assertEquals(200, write.statusCode())

        val read = awaitKey("e2e-read", "k", expectedStatus = 200)
        assertContentEquals(byteArrayOf(1, 2, 3), read.body())

        val delete = http.send(
            HttpRequest.newBuilder().uri(url("/v1/collections/e2e-read/keys/k")).DELETE().build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        assertEquals(200, delete.statusCode())
        awaitKey("e2e-read", "k", expectedStatus = 404)
    }

    private fun consumeAll(topic: String) = KafkaConsumer(
        mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to redpanda.bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ),
        StringDeserializer(),
        ByteArrayDeserializer(),
    ).use { consumer ->
        consumer.assign(listOf(TopicPartition(topic, 0)))
        consumer.poll(Duration.ofSeconds(10)).map { it.offset() to (it.key() to EnvelopeV1.decode(it.value())) }
    }
}
