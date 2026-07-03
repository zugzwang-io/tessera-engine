package io.zugzwang.tessera

import javax.sql.DataSource

/**
 * The Postgres read model: derived, rebuildable projections of the log
 * (CLAUDE.md invariant 1). Dropping every table here and replaying the log
 * from offset 0 must reproduce them exactly — nothing in this schema is a
 * source of truth, which is also why plain `CREATE TABLE IF NOT EXISTS` is
 * (for now) the whole migration story: schema evolution is a rebuild.
 * Migration tooling (Flyway) arrives with the first change that can't be.
 *
 * All three tables are written by the projector in ONE transaction per
 * batch, so at every commit `latest_state` equals exactly the fold of log
 * offsets `[0..projector_checkpoint.log_offset]`.
 */
object Schema {

    fun ensure(dataSource: DataSource) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                // The latest-state view: one row per LIVE (collection, key).
                //
                // - A tombstoned key is a deleted row — absence means "never
                //   written or deleted", exactly like the future in-memory maps.
                //   `value` is therefore NOT NULL here (empty bytes = legal put).
                // - `log_offset` is the sequence of the change that set the row.
                //   It is the idempotence guard: writers only apply a change to
                //   a row when `log_offset < incoming offset`, so replays and
                //   redeliveries are no-ops and stale data never wins.
                // - PK (collection, key) serves both point reads and ordered
                //   per-collection scans (GET collection / GET key).
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
                // The audit/OLAP projection: one row per ENTRY of every change,
                // append-only, never conflated, prunable at will (the log keeps
                // full history).
                //
                // - A multi-key change is N rows sharing one log_offset; that
                //   shared offset is what makes "state as of sequence S" a whole
                //   number of changes.
                // - PK (log_offset, entry_index) is the entry's coordinates in
                //   the log — identity, not semantic order (keys are unique
                //   within a change). The index is stable across replays because
                //   entry order is serialized into the record bytes (envelope
                //   ABI). Identity by position stays lossless for ANY log
                //   content, including records violating current API contracts.
                // - `value` NULL means tombstone (no value existed); empty bytes
                //   remain a legal put. The CHECK ties the two columns together
                //   so the sealed Put/Tombstone model can't be misrepresented.
                // - When multi-partition placement lands, offsets are only
                //   unique per partition and log_partition must join the PK;
                //   deferred because that phase rebuilds the projection anyway.
                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS change_history (
                        log_offset bigint NOT NULL,
                        entry_index int NOT NULL,
                        collection text NOT NULL,
                        key text NOT NULL,
                        value bytea,
                        tombstone boolean NOT NULL,
                        PRIMARY KEY (log_offset, entry_index),
                        CHECK (tombstone = (value IS NULL))
                    )
                    """.trimIndent(),
                )
                // The frontier: how far into the log the tables above reflect.
                //
                // - Advanced in the same transaction as the rows, so the view is
                //   never approximately consistent — it is exactly fold[0..F].
                //   Readers return F as the watermark; rehydrating consumers
                //   snapshot the view then tail the log from F+1, which is why
                //   new keys can never be missed.
                // - The projector's resume position comes from here, never from
                //   Kafka consumer offsets — a crash between the pg commit and a
                //   Kafka ack cannot cause replay (and replays are no-ops anyway).
                // - Keyed by partition so the schema survives the multi-partition
                //   world without migration; today exactly one row (partition 0).
                // - Single-writer fence: the advance is an UPDATE guarded on the
                //   expected previous offset; a competing projector matches zero
                //   rows and aborts its whole transaction.
                statement.execute(
                    """
                    CREATE TABLE IF NOT EXISTS projector_checkpoint (
                        log_partition int PRIMARY KEY,
                        log_offset bigint NOT NULL
                    )
                    """.trimIndent(),
                )
            }
        }
    }
}
