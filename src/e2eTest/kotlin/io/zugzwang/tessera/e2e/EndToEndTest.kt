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
import java.sql.DriverManager
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

    private fun postChange(body: String): HttpResponse<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${app.host}:${app.getMappedPort(8080)}/v1/collections/orders/changes"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        return http.send(request, HttpResponse.BodyHandlers.ofString())
    }

    @Test
    fun `changes commit through the shipped image and land durably in the log`() {
        val first = postChange("""{"entries":[{"key":"a","value":"AQI="},{"key":"b","value":""}]}""")
        assertEquals(200, first.statusCode())
        assertEquals("""{"sequence":0}""", first.body())

        val second = postChange("""{"entries":[{"key":"a","value":"Aw=="}]}""")
        assertEquals("""{"sequence":1}""", second.body())

        val records = consumeAll("tessera-log")
        assertEquals(2, records.size)
        val (collection, entries) = records.first()
        assertEquals("orders", collection)
        assertEquals(listOf("a", "b"), entries.map { it.key })
        assertContentEquals(byteArrayOf(1, 2), assertIs<Change.Put>(entries[0]).value)
        assertContentEquals(ByteArray(0), assertIs<Change.Put>(entries[1]).value)
    }

    @Test
    fun `validation runs in the shipped image`() {
        assertEquals(400, postChange("""{"entries":[]}""").statusCode())
    }

    /** Reads are lagging by the projector batch window; poll pg until it catches up. */
    @Test
    fun `the projector inside the shipped image folds writes into postgres`() {
        assertEquals(200, postChange("""{"entries":[{"key":"projected","value":"BQY="}]}""").statusCode())

        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (true) {
            val value = DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
                .use { connection ->
                    connection
                        .prepareStatement("SELECT value FROM latest_state WHERE collection = 'orders' AND key = 'projected'")
                        .use { it.executeQuery().use { rs -> if (rs.next()) rs.getBytes(1) else null } }
                }
            if (value != null) {
                assertContentEquals(byteArrayOf(5, 6), value)
                return
            }
            if (System.nanoTime() > deadline) fail("write was never projected into postgres")
            Thread.sleep(250)
        }
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
        consumer.poll(Duration.ofSeconds(10)).map { it.key() to EnvelopeV1.decode(it.value()) }
    }
}
