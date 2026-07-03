package io.zugzwang.tessera

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.sql.Connection
import java.time.Duration
import javax.sql.DataSource

internal fun readProjectorCheckpoint(connection: Connection): Long? = connection.createStatement().use { statement ->
    statement.executeQuery("SELECT log_offset FROM projector_checkpoint WHERE log_partition = 0").use {
        if (it.next()) it.getLong(1) else null
    }
}

object Postgres {
    fun dataSourceFromEnv(): DataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = System.getenv("POSTGRES_URL") ?: "jdbc:postgresql://localhost:5432/tessera"
            username = System.getenv("POSTGRES_USER") ?: "tessera"
            password = System.getenv("POSTGRES_PASSWORD") ?: "tessera"
            maximumPoolSize = 4
        },
    )
}

/**
 * Folds the log into the Postgres read model: an unconflated append-only
 * `change_history` (the OLAP projection, prunable — the log holds truth) plus
 * a `latest_state` view conflated per key within each batch. Both advance
 * with the checkpoint in a single transaction, so at every commit the view
 * equals exactly the fold of offsets [0..checkpoint].
 *
 * The resume position is the pg checkpoint, never Kafka's consumer offsets —
 * a crash between the pg commit and a Kafka ack therefore cannot replay.
 * All writes are idempotent anyway (offset-guarded upserts and deletes,
 * keyed history inserts) so rebuilds and redeliveries land as no-ops, and
 * the checkpoint advance is fenced on the expected previous offset so a
 * second projector instance aborts instead of interleaving.
 *
 * Everything here is a derived projection: drop the tables, replay the log,
 * and the result is identical.
 */
class Projector(
    private val dataSource: DataSource,
    private val consumer: Consumer<String, ByteArray>,
    topic: String,
) : AutoCloseable {

    private val partition = TopicPartition(topic, 0)
    private var expectedCheckpoint: Long? = null

    fun start() {
        dataSource.connection.use { it.ensureSchema() }
        expectedCheckpoint = dataSource.connection.use { readProjectorCheckpoint(it) }
        consumer.assign(listOf(partition))
        when (val next = expectedCheckpoint?.plus(1)) {
            null -> consumer.seekToBeginning(listOf(partition))
            else -> consumer.seek(partition, next)
        }
    }

    /**
     * Polls one batch and applies it in one transaction; returns records
     * applied. The poll is the batch window: staleness of the view is
     * bounded by the poll timeout plus transaction time.
     */
    fun runOnce(timeout: Duration = Duration.ofSeconds(1)): Int {
        val records = consumer.poll(timeout).records(partition)
        if (records.isEmpty()) return 0
        apply(records)
        return records.size
    }

    fun run() {
        start()
        // Supervision is deliberately absent for now: a projector failure
        // stops projection (staleness grows) but never the write path.
        while (true) runOnce()
    }

    override fun close() {
        consumer.close()
    }

    private fun apply(records: List<ConsumerRecord<String, ByteArray>>) {
        val decoded = records.map { it to EnvelopeV1.decode(it.value()) }
        dataSource.connection.use { connection ->
            connection.autoCommit = false
            connection.appendHistory(decoded)
            connection.applyLatest(decoded)
            connection.advanceCheckpoint(records.last().offset())
            connection.commit()
        }
        expectedCheckpoint = records.last().offset()
    }

    private fun Connection.appendHistory(decoded: List<Pair<ConsumerRecord<String, ByteArray>, List<Change.Entry>>>) {
        val sql = """
            INSERT INTO change_history (log_offset, entry_index, collection, key, value, tombstone)
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
        """.trimIndent()
        prepareStatement(sql).use { statement ->
            for ((record, entries) in decoded) {
                entries.forEachIndexed { index, entry ->
                    statement.setLong(1, record.offset())
                    statement.setInt(2, index)
                    statement.setString(3, record.key())
                    statement.setString(4, entry.key)
                    statement.setBytes(5, entry.value)
                    statement.setBoolean(6, entry.tombstone)
                    statement.addBatch()
                }
            }
            statement.executeBatch()
        }
    }

    private fun Connection.applyLatest(decoded: List<Pair<ConsumerRecord<String, ByteArray>, List<Change.Entry>>>) {
        // Conflate within the batch: only the last write per key is applied
        // to the view. History above is never conflated.
        val latest = LinkedHashMap<Pair<String, String>, Pair<Long, Change.Entry>>()
        for ((record, entries) in decoded) {
            for (entry in entries) {
                latest[record.key() to entry.key] = record.offset() to entry
            }
        }
        val upsert = """
            INSERT INTO latest_state (collection, key, value, log_offset) VALUES (?, ?, ?, ?)
            ON CONFLICT (collection, key) DO UPDATE
            SET value = EXCLUDED.value, log_offset = EXCLUDED.log_offset
            WHERE latest_state.log_offset < EXCLUDED.log_offset
        """.trimIndent()
        prepareStatement(upsert).use { statement ->
            for ((collectionKey, offsetEntry) in latest) {
                val (offset, entry) = offsetEntry
                if (entry.tombstone) continue
                statement.setString(1, collectionKey.first)
                statement.setString(2, collectionKey.second)
                statement.setBytes(3, entry.value)
                statement.setLong(4, offset)
                statement.addBatch()
            }
            statement.executeBatch()
        }
        prepareStatement("DELETE FROM latest_state WHERE collection = ? AND key = ? AND log_offset < ?").use { statement ->
            for ((collectionKey, offsetEntry) in latest) {
                val (offset, entry) = offsetEntry
                if (!entry.tombstone) continue
                statement.setString(1, collectionKey.first)
                statement.setString(2, collectionKey.second)
                statement.setLong(3, offset)
                statement.addBatch()
            }
            statement.executeBatch()
        }
    }

    private fun Connection.advanceCheckpoint(offset: Long) {
        val updated = when (val expected = expectedCheckpoint) {
            null -> prepareStatement(
                "INSERT INTO projector_checkpoint (log_partition, log_offset) VALUES (0, ?) ON CONFLICT DO NOTHING",
            ).use { it.setLong(1, offset); it.executeUpdate() }
            else -> prepareStatement(
                "UPDATE projector_checkpoint SET log_offset = ? WHERE log_partition = 0 AND log_offset = ?",
            ).use { it.setLong(1, offset); it.setLong(2, expected); it.executeUpdate() }
        }
        check(updated == 1) { "checkpoint fence violated: another projector owns this partition" }
    }

    private fun Connection.ensureSchema() = createStatement().use { statement ->
        statement.execute(
            """
            CREATE TABLE IF NOT EXISTS latest_state (
                collection text NOT NULL,
                key text NOT NULL,
                value bytea NOT NULL,
                log_offset bigint NOT NULL,
                PRIMARY KEY (collection, key)
            )
            """.trimIndent(),
        )
        statement.execute(
            """
            CREATE TABLE IF NOT EXISTS change_history (
                log_offset bigint NOT NULL,
                entry_index int NOT NULL,
                collection text NOT NULL,
                key text NOT NULL,
                value bytea NOT NULL,
                tombstone boolean NOT NULL,
                PRIMARY KEY (log_offset, entry_index)
            )
            """.trimIndent(),
        )
        statement.execute(
            """
            CREATE TABLE IF NOT EXISTS projector_checkpoint (
                log_partition int PRIMARY KEY,
                log_offset bigint NOT NULL
            )
            """.trimIndent(),
        )
    }

    companion object {
        fun fromEnv(dataSource: DataSource): Projector {
            val bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
            val topic = System.getenv("TESSERA_LOG_TOPIC") ?: "tessera-log"
            return Projector(dataSource, consumer(bootstrap), topic)
        }

        fun consumer(bootstrapServers: String): KafkaConsumer<String, ByteArray> = KafkaConsumer(
            mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ),
            StringDeserializer(),
            ByteArrayDeserializer(),
        )
    }
}
