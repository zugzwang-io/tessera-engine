package io.zugzwang.tessera

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.redpanda.RedpandaContainer
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.fail

@Testcontainers
class ProjectorIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val redpanda = RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v26.1.12")

        // A database of our own: tests here run sequentially (JUnit default),
        // but a dedicated database means enabling parallel execution across
        // classes later can never silently share this state.
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:18.4").withDatabaseName("projector_it")
    }

    private val dataSource: DataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = postgres.jdbcUrl
            username = postgres.username
            password = postgres.password
            maximumPoolSize = 2
        },
    )

    @BeforeEach
    fun resetSchema() {
        dataSource.connection.use { connection ->
            connection.createStatement().use {
                it.execute("DROP TABLE IF EXISTS latest_state, change_history, projector_checkpoint")
            }
        }
    }

    private fun projector(topic: String) = Projector(dataSource, Projector.consumer(redpanda.bootstrapServers), topic)

    private fun append(topic: String, vararg changes: Change) = runBlocking {
        val log = KafkaChangeLog(KafkaChangeLog.producer(redpanda.bootstrapServers), topic)
        changes.forEach { log.append(it) }
    }

    private fun Projector.drain(expected: Int) {
        var applied = 0
        repeat(30) {
            applied += runOnce()
            if (applied >= expected) return
        }
        fail("applied only $applied of $expected records")
    }

    private fun latestState(): List<Triple<String, String, Pair<ByteArray, Long>>> =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT collection, key, value, log_offset FROM latest_state ORDER BY collection, key")
                    .use { rs ->
                        generateSequence { if (rs.next()) Triple(rs.getString(1), rs.getString(2), rs.getBytes(3) to rs.getLong(4)) else null }
                            .toList()
                    }
            }
        }

    private fun count(sql: String): Int = dataSource.connection.use { connection ->
        connection.createStatement().use { it.executeQuery(sql).use { rs -> rs.next(); rs.getInt(1) } }
    }

    @Test
    fun `folds the log into latest state, history, and checkpoint transactionally`() {
        val topic = "proj-fold"
        append(
            topic,
            Change("orders", listOf(Change.Put("a", byteArrayOf(1)), Change.Put("b", byteArrayOf(2)))),
            Change("orders", listOf(Change.Put("a", byteArrayOf(3)))),
            Change("inventory", listOf(Change.Put("x", byteArrayOf(9)))),
            Change("orders", listOf(Change.Tombstone("b"))),
        )

        projector(topic).use {
            it.start()
            it.drain(4)
        }

        val state = latestState()
        assertEquals(listOf("inventory" to "x", "orders" to "a"), state.map { it.first to it.second })
        assertContentEquals(byteArrayOf(3), state[1].third.first)
        assertEquals(1L, state[1].third.second)
        assertEquals(5, count("SELECT count(*) FROM change_history"))
        assertEquals(1, count("SELECT count(*) FROM change_history WHERE tombstone AND value IS NULL"))
        assertEquals(3, count("SELECT log_offset FROM projector_checkpoint WHERE log_partition = 0"))
    }

    @Test
    fun `replaying already-applied records is a no-op`() {
        val topic = "proj-replay"
        append(
            topic,
            Change("orders", listOf(Change.Put("a", byteArrayOf(1)))),
            Change("orders", listOf(Change.Put("a", byteArrayOf(2)), Change.Put("b", byteArrayOf(4)))),
        )
        projector(topic).use {
            it.start()
            it.drain(2)
        }
        val before = latestState()

        // Simulate redelivery of everything (a lost ack, a rebuild, operator
        // replay): forget the checkpoint so a fresh projector re-reads from
        // the beginning against the already-populated tables.
        dataSource.connection.use { c ->
            c.createStatement().use { it.execute("DELETE FROM projector_checkpoint") }
        }
        projector(topic).use {
            it.start()
            it.drain(2)
        }

        val after = latestState()
        assertEquals(before.map { it.first to it.second }, after.map { it.first to it.second })
        assertContentEquals(before.map { it.third.first }.flatMap { it.asIterable() }, after.map { it.third.first }.flatMap { it.asIterable() })
        assertEquals(3, count("SELECT count(*) FROM change_history"))
        assertEquals(1, count("SELECT log_offset FROM projector_checkpoint WHERE log_partition = 0"))
    }

    private fun record(offset: Long, change: Change) =
        org.apache.kafka.clients.consumer.ConsumerRecord("manual", 0, offset, change.collection, EnvelopeV1.encode(change))

    @Test
    fun `conflates within a batch - the last write per key wins, including set-delete-set`() {
        val projector = projector("manual")
        projector.use {
            it.start()
            it.applyBatch(
                listOf(
                    record(0, Change("orders", listOf(Change.Put("a", byteArrayOf(1)), Change.Put("b", byteArrayOf(9))))),
                    record(1, Change("orders", listOf(Change.Tombstone("a")))),
                    record(2, Change("orders", listOf(Change.Put("a", byteArrayOf(3))))),
                    record(3, Change("orders", listOf(Change.Tombstone("b")))),
                ),
            )
        }

        val state = latestState()
        assertEquals(listOf("orders" to "a"), state.map { it.first to it.second })
        assertContentEquals(byteArrayOf(3), state.single().third.first)
        assertEquals(2L, state.single().third.second)
        // History is never conflated: every entry of every change is appended.
        assertEquals(5, count("SELECT count(*) FROM change_history"))
        assertEquals(3, count("SELECT log_offset FROM projector_checkpoint WHERE log_partition = 0"))
    }

    @Test
    fun `set delete set across batches converges to the last set`() {
        val projector = projector("manual")
        projector.use {
            it.start()
            it.applyBatch(listOf(record(0, Change("orders", listOf(Change.Put("a", byteArrayOf(1)))))))
            it.applyBatch(listOf(record(1, Change("orders", listOf(Change.Tombstone("a"))))))
            assertEquals(emptyList(), latestState())
            it.applyBatch(listOf(record(2, Change("orders", listOf(Change.Put("a", byteArrayOf(3)))))))
        }

        val state = latestState().single()
        assertContentEquals(byteArrayOf(3), state.third.first)
        assertEquals(2L, state.third.second)
    }

    @Test
    fun `a second projector instance hits the checkpoint fence instead of interleaving`() {
        val topic = "proj-fence"
        append(topic, Change("orders", listOf(Change.Put("a", byteArrayOf(1)))))

        val zombie = projector(topic)
        zombie.start()
        projector(topic).use {
            it.start()
            it.drain(1)
        }

        zombie.use {
            assertFailsWith<IllegalStateException> { it.drain(1) }
        }
    }
}
