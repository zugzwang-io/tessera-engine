package io.zugzwang.tessera

import java.sql.Connection
import javax.sql.DataSource

/**
 * Read access to the latest-state view. Reads are lagging by the projector's
 * batch window: every result carries the frontier it was read at
 * ([CollectionState.asOfSequence] / [KeyState.asOfSequence]), and
 * read-your-write is not guaranteed — clients compare the watermark against
 * their own committed sequences.
 */
interface StateView {
    /** null until the projector has checkpointed at least once. */
    fun collection(collection: String): CollectionState?

    /** null until the projector has checkpointed at least once. */
    fun key(collection: String, key: String): KeyState?
}

data class CollectionState(val asOfSequence: Long, val entries: List<StateEntry>)

data class StateEntry(val key: String, val value: ByteArray)

/** [value] is null when the key is absent (never written, or tombstoned). */
data class KeyState(val asOfSequence: Long, val value: ByteArray?)

class PgStateView(provider: () -> DataSource) : StateView {

    private val dataSource by lazy(provider)

    override fun collection(collection: String): CollectionState? = withSnapshot { connection, checkpoint ->
        val sql = "SELECT key, value FROM latest_state WHERE collection = ? ORDER BY key"
        val entries = connection.prepareStatement(sql).use { statement ->
            statement.setString(1, collection)
            statement.executeQuery().use { rs ->
                generateSequence { if (rs.next()) StateEntry(rs.getString(1), rs.getBytes(2)) else null }.toList()
            }
        }
        CollectionState(checkpoint, entries)
    }

    override fun key(collection: String, key: String): KeyState? = withSnapshot { connection, checkpoint ->
        val sql = "SELECT value FROM latest_state WHERE collection = ? AND key = ?"
        val value = connection.prepareStatement(sql).use { statement ->
            statement.setString(1, collection)
            statement.setString(2, key)
            statement.executeQuery().use { rs -> if (rs.next()) rs.getBytes(1) else null }
        }
        KeyState(checkpoint, value)
    }

    /**
     * Rows and checkpoint must come from one repeatable-read snapshot so the
     * watermark is exact for the rows returned, not approximately concurrent.
     */
    private fun <T> withSnapshot(block: (Connection, Long) -> T): T? = dataSource.connection.use { connection ->
        connection.autoCommit = false
        connection.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ
        try {
            readProjectorCheckpoint(connection)?.let { checkpoint -> block(connection, checkpoint) }
        } finally {
            connection.rollback()
            connection.autoCommit = true
        }
    }
}
