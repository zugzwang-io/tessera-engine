package io.zugzwang.tessera

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.sql.Connection
import java.sql.Types
import java.time.Duration
import javax.sql.DataSource

/** One log record with its envelope decoded: the unit the SQL below works in. */
private data class DecodedRecord(
    val offset: Long,
    val collection: String,
    val entries: List<Change.Entry>,
)

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

    /**
     * Prepares the projector: creates the schema if absent, reads the resume
     * checkpoint F from Postgres, and positions the consumer at F+1 (or the
     * beginning of the partition when no checkpoint exists yet). Must be
     * called once before [run] or [runOnce]; fails fast when Postgres is
     * unreachable.
     */
    fun start() {
        Schema.ensure(dataSource)
        expectedCheckpoint = dataSource.connection.use { it.readCheckpoint() }
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
        applyBatch(records)
        return records.size
    }

    /** Projects forever. [start] must have been called first. */
    fun run() {
        // Supervision is deliberately absent for now: a projector failure
        // stops projection (staleness grows) but never the write path.
        while (true) runOnce()
    }

    override fun close() {
        consumer.close()
    }

    internal fun applyBatch(records: List<ConsumerRecord<String, ByteArray>>) {
        val decoded = records.map { DecodedRecord(it.offset(), it.key(), EnvelopeV1.decode(it.value())) }
        dataSource.connection.use { connection ->
            connection.autoCommit = false
            // Fence FIRST: advancing the checkpoint takes its row lock for
            // the rest of the transaction, so a competing projector fails
            // here before writing anything, and all latest_state writes
            // happen strictly serialized under the lock (no deadlocks from
            // differing batch boundaries).
            connection.advanceCheckpoint(decoded.last().offset)
            connection.appendHistory(decoded)
            connection.applyLatest(decoded)
            connection.commit()
        }
        expectedCheckpoint = decoded.last().offset
    }

    // (log_offset, entry_index) is the entry's identity — its coordinates in
    // the log, not a semantic ordering (keys are unique within a change, so
    // entry order carries no meaning). The index is stable across replays
    // because entry order is serialized into the record bytes by EnvelopeV1
    // and preserved by decode; that stability is part of the envelope ABI.
    // History must stay lossless over whatever the log contains — identity
    // by position can describe any record, including ones violating current
    // API contracts.
    private fun Connection.appendHistory(decoded: List<DecodedRecord>) {
        val sql = """
            INSERT INTO change_history (log_offset, entry_index, collection, key, value, tombstone)
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
        """.trimIndent()
        prepareStatement(sql).use { statement ->
            for (record in decoded) {
                record.entries.forEachIndexed { index, entry ->
                    statement.setLong(1, record.offset)
                    statement.setInt(2, index)
                    statement.setString(3, record.collection)
                    statement.setString(4, entry.key)
                    when (entry) {
                        is Change.Put -> statement.setBytes(5, entry.value)
                        // NULL, not empty bytes: a tombstone has no value,
                        // mirroring the sealed model (empty bytes = a legal put).
                        is Change.Tombstone -> statement.setNull(5, Types.BINARY)
                    }
                    statement.setBoolean(6, entry is Change.Tombstone)
                    statement.addBatch()
                }
            }
            statement.executeBatch()
        }
    }

    private fun Connection.applyLatest(decoded: List<DecodedRecord>) {
        // Conflate within the batch: the map keeps only the LAST entry per
        // (collection, key), so e.g. set → delete → set collapses to the
        // final set — which is exactly the fold's result. History above is
        // never conflated.
        val latest = LinkedHashMap<Pair<String, String>, Pair<Long, Change.Entry>>()
        for (record in decoded) {
            for (entry in record.entries) {
                latest[record.collection to entry.key] = record.offset to entry
            }
        }
        // Insert-or-update, guarded on offset: the DO UPDATE only fires when
        // the incoming record is NEWER than the stored row. A replayed old
        // record (redelivery, rebuild, manual replay) therefore never
        // overwrites newer state — this guard is what makes writes idempotent.
        val upsert = """
            INSERT INTO latest_state (collection, key, value, log_offset) VALUES (?, ?, ?, ?)
            ON CONFLICT (collection, key) DO UPDATE
            SET value = EXCLUDED.value, log_offset = EXCLUDED.log_offset
            WHERE latest_state.log_offset < EXCLUDED.log_offset
        """.trimIndent()
        prepareStatement(upsert).use { statement ->
            for ((collectionKey, offsetEntry) in latest) {
                val (offset, entry) = offsetEntry
                val put = entry as? Change.Put ?: continue
                statement.setString(1, collectionKey.first)
                statement.setString(2, collectionKey.second)
                statement.setBytes(3, put.value)
                statement.setLong(4, offset)
                statement.addBatch()
            }
            statement.executeBatch()
        }
        // Same idempotence guard for deletes: a tombstone only removes rows
        // OLDER than itself, so replaying an old tombstone past newer state
        // is a no-op.
        prepareStatement("DELETE FROM latest_state WHERE collection = ? AND key = ? AND log_offset < ?").use { statement ->
            for ((collectionKey, offsetEntry) in latest) {
                val (offset, entry) = offsetEntry
                if (entry !is Change.Tombstone) continue
                statement.setString(1, collectionKey.first)
                statement.setString(2, collectionKey.second)
                statement.setLong(3, offset)
                statement.addBatch()
            }
            statement.executeBatch()
        }
    }

    // The checkpoint table is keyed by partition so the schema survives the
    // multi-partition world without migration; this projector owns exactly
    // one partition and always filters on it.
    private fun Connection.advanceCheckpoint(offset: Long) {
        val updated = when (val expected = expectedCheckpoint) {
            null -> prepareStatement(
                "INSERT INTO projector_checkpoint (log_partition, log_offset) VALUES (?, ?) ON CONFLICT DO NOTHING",
            ).use {
                it.setInt(1, partition.partition())
                it.setLong(2, offset)
                it.executeUpdate()
            }
            else -> prepareStatement(
                "UPDATE projector_checkpoint SET log_offset = ? WHERE log_partition = ? AND log_offset = ?",
            ).use {
                it.setLong(1, offset)
                it.setInt(2, partition.partition())
                it.setLong(3, expected)
                it.executeUpdate()
            }
        }
        check(updated == 1) { "checkpoint fence violated: another projector owns this partition" }
    }

    private fun Connection.readCheckpoint(): Long? =
        prepareStatement("SELECT log_offset FROM projector_checkpoint WHERE log_partition = ?").use { statement ->
            statement.setInt(1, partition.partition())
            statement.executeQuery().use { if (it.next()) it.getLong(1) else null }
        }

    companion object {
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
