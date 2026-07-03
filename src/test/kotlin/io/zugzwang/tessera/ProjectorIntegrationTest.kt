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

        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:18.4")
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
        assertEquals(1, count("SELECT count(*) FROM change_history WHERE tombstone"))
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

    @Test
    fun `the state view serves the fold at its exact frontier`() {
        val topic = "proj-view"
        append(
            topic,
            Change("orders", listOf(Change.Put("a", byteArrayOf(1)), Change.Put("b", byteArrayOf(2)))),
            Change("orders", listOf(Change.Tombstone("b"))),
        )
        projector(topic).use {
            it.start()
            it.drain(2)
        }

        val view = PgStateView { dataSource }
        val collection = view.collection("orders")!!
        assertEquals(1L, collection.asOfSequence)
        assertEquals(listOf("a"), collection.entries.map { it.key })

        assertContentEquals(byteArrayOf(1), view.key("orders", "a")!!.value)
        assertEquals(null, view.key("orders", "b")!!.value)
        assertEquals(emptyList(), view.collection("unknown")!!.entries)
    }

    @Test
    fun `the state view is null before the first checkpoint`() {
        dataSource.connection.use { c ->
            c.createStatement().use {
                it.execute("CREATE TABLE IF NOT EXISTS projector_checkpoint (log_partition int PRIMARY KEY, log_offset bigint NOT NULL)")
                it.execute("CREATE TABLE IF NOT EXISTS latest_state (collection text NOT NULL, key text NOT NULL, value bytea NOT NULL, log_offset bigint NOT NULL, PRIMARY KEY (collection, key))")
            }
        }
        assertEquals(null, PgStateView { dataSource }.collection("orders"))
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
